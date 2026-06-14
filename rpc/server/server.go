// The MembussNode gRPC server. All RPCs route through the
// Backend interface so the daemon can swap in real
// implementations (BadgerDB store, libp2p host, DHT, PEX,
// memex engine, anchor engine) while the server stays unit
// testable against an in-memory Backend.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	membusspb "github.com/nnlgsakib/membuss/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Backend is the contract the server depends on. The daemon
// supplies a real Backend wired to its subsystems; tests
// supply an in-memory Backend.
type Backend interface {
	// Add ingests a local file. The chunker selection
	// (chunk.NewFixed or chunk.NewRabin) and chunk size are
	// honored when non-zero. If sealRoot is true, the root is
	// sealed before returning.
	Add(ctx context.Context, path, chunker string, chunkSize uint32, sealRoot bool) (AddResult, error)
	// Get resolves a MID locally if present; otherwise it falls
	// back to fetching via Memex. The returned ReadCloser
	// streams the bytes.
	Get(ctx context.Context, midStr string, offset, limit uint64) (io.ReadCloser, error)
	// Seal pins the given MID, optionally recursive.
	Seal(ctx context.Context, midStr string, recursive bool) (SealResult, error)
	// Unseal removes the pin on a MID.
	Unseal(ctx context.Context, midStr string) (uint64, error)
	// Stat returns a snapshot describing a MID.
	Stat(ctx context.Context, midStr string) (StatInfo, error)
	// Peers returns the local PEX peer table.
	Peers(limit uint32) ([]NodePeerInfo, uint32, error)
	// DHTPeek returns the providers the DHT knows for a MID.
	DHTPeek(ctx context.Context, midStr string, limit uint32) ([]NodePeerInfo, error)
	// GC runs garbage collection on the local store.
	GC(ctx context.Context, all bool) (GCInfo, error)
	// AnchorStatus returns the anchor engine's stats.
	AnchorStatus() AnchorInfo
}

// AddResult is the return value of Backend.Add.
type AddResult struct {
	MID    string
	Size   uint64
	Blocks uint64
	Sealed bool
}

// SealResult is the return value of Backend.Seal.
type SealResult struct {
	Pinned  uint64
	Already bool
}

// StatInfo is the return value of Backend.Stat.
type StatInfo struct {
	Present bool
	Size    uint64
	Blocks  uint64
	Sealed  bool
	Codec   uint64
	Erasure *ErasureInfo
}

// ErasureInfo mirrors the ErasureInfo proto, kept separate so
// the Backend contract does not leak protobuf types to
// non-rpc callers.
type ErasureInfo struct {
	DataShards   uint32
	ParityShards uint32
	ShardMIDs    []string
}

// NodePeerInfo mirrors the NodePeerInfo proto.
type NodePeerInfo struct {
	PeerID string
	Addrs  []string
}

// GCInfo mirrors the GCResponse proto.
type GCInfo struct {
	BytesFreed uint64
	BlocksKept uint64
}

// AnchorInfo mirrors the AnchorStatusResponse proto.
type AnchorInfo struct {
	PeerID     string
	UptimeSecs int64
	BlocksHeld int64
	Anchors    int32
	Backlog    int32
	Synced     int64
}

// Build is the daemon build identifier reported in PingResponse.
var Build = "dev"

// Server implements the gRPC MembussNode and Node services.
type Server struct {
	membusspb.UnimplementedMembussNodeServer
	membusspb.UnimplementedNodeServer

	Backend Backend
}

// New returns a Server that delegates to b.
func NewServer(b Backend) *Server {
	return &Server{Backend: b}
}

// Register attaches both Node and MembussNode services to the
// gRPC server.
func (s *Server) Register(g *grpc.Server) {
	membusspb.RegisterNodeServer(g, s)
	membusspb.RegisterMembussNodeServer(g, s)
}

// --- Node service ---

// Ping is a connectivity probe.
func (s *Server) Ping(ctx context.Context, req *membusspb.PingRequest) (*membusspb.PingResponse, error) {
	return &membusspb.PingResponse{Message: req.GetMessage(), Build: Build}, nil
}

// --- MembussNode service ---

// Add ingests a file. The path is resolved on the daemon side.
func (s *Server) Add(ctx context.Context, req *membusspb.AddRequest) (*membusspb.AddResponse, error) {
	if req.GetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "add: path required")
	}
	res, err := s.Backend.Add(ctx, req.GetPath(), req.GetChunker(), req.GetChunkSize(), !req.GetNoSeal())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "add: %v", err)
	}
	return &membusspb.AddResponse{
		Mid:    res.MID,
		Size:   res.Size,
		Blocks: res.Blocks,
		Sealed: res.Sealed,
	}, nil
}

// Get streams a MID's content back to the caller in chunks.
func (s *Server) Get(req *membusspb.GetRequest, stream membusspb.MembussNode_GetServer) error {
	if req.GetMid() == "" {
		return status.Error(codes.InvalidArgument, "get: mid required")
	}
	rc, err := s.Backend.Get(stream.Context(), req.GetMid(), req.GetOffset(), req.GetLimit())
	if err != nil {
		return status.Errorf(codes.Internal, "get: %v", err)
	}
	defer rc.Close()

	const frameSize = 64 * 1024
	buf := make([]byte, frameSize)
	var (
		index uint64
		total uint64
	)
	// We don't know the total up front; leave it 0.
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if err := stream.Send(&membusspb.GetChunk{Data: append([]byte(nil), buf[:n]...), Index: index, Total: total}); err != nil {
				return err
			}
			index++
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "get: read: %v", err)
		}
	}
}

// Seal pins a MID.
func (s *Server) Seal(ctx context.Context, req *membusspb.SealRequest) (*membusspb.SealResponse, error) {
	if req.GetMid() == "" {
		return nil, status.Error(codes.InvalidArgument, "seal: mid required")
	}
	res, err := s.Backend.Seal(ctx, req.GetMid(), req.GetRecursive())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "seal: %v", err)
	}
	return &membusspb.SealResponse{Pinned: res.Pinned, Already: res.Already}, nil
}

// Unseal removes a pin.
func (s *Server) Unseal(ctx context.Context, req *membusspb.UnsealRequest) (*membusspb.UnsealResponse, error) {
	if req.GetMid() == "" {
		return nil, status.Error(codes.InvalidArgument, "unseal: mid required")
	}
	n, err := s.Backend.Unseal(ctx, req.GetMid())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unseal: %v", err)
	}
	return &membusspb.UnsealResponse{Removed: n}, nil
}

// Stat describes a MID.
func (s *Server) Stat(ctx context.Context, req *membusspb.StatRequest) (*membusspb.StatResponse, error) {
	if req.GetMid() == "" {
		return nil, status.Error(codes.InvalidArgument, "stat: mid required")
	}
	info, err := s.Backend.Stat(ctx, req.GetMid())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stat: %v", err)
	}
	resp := &membusspb.StatResponse{
		Present: info.Present,
		Size:    info.Size,
		Blocks:  info.Blocks,
		Sealed:  info.Sealed,
		Codec:   info.Codec,
	}
	if info.Erasure != nil {
		resp.Erasure = &membusspb.ErasureInfo{
			DataShards:   info.Erasure.DataShards,
			ParityShards: info.Erasure.ParityShards,
			ShardMids:    info.Erasure.ShardMIDs,
		}
	}
	return resp, nil
}

// Peers returns the local PEX peer table.
func (s *Server) Peers(ctx context.Context, req *membusspb.PeersRequest) (*membusspb.PeersResponse, error) {
	peers, total, err := s.Backend.Peers(req.GetLimit())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "peers: %v", err)
	}
	out := make([]*membusspb.NodePeerInfo, 0, len(peers))
	for _, p := range peers {
		out = append(out, peerInfoToProto(p))
	}
	return &membusspb.PeersResponse{Peers: out, Total: total}, nil
}

// DHTPeek asks the DHT who provides a MID.
func (s *Server) DHTPeek(ctx context.Context, req *membusspb.DHTPeekRequest) (*membusspb.DHTPeekResponse, error) {
	if req.GetMid() == "" {
		return nil, status.Error(codes.InvalidArgument, "dht peek: mid required")
	}
	provs, err := s.Backend.DHTPeek(ctx, req.GetMid(), req.GetLimit())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "dht peek: %v", err)
	}
	out := make([]*membusspb.NodePeerInfo, 0, len(provs))
	for _, p := range provs {
		out = append(out, peerInfoToProto(p))
	}
	return &membusspb.DHTPeekResponse{Providers: out}, nil
}

// GC runs garbage collection on the local store.
func (s *Server) GC(ctx context.Context, req *membusspb.GCRequest) (*membusspb.GCResponse, error) {
	info, err := s.Backend.GC(ctx, req.GetAll())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "gc: %v", err)
	}
	return &membusspb.GCResponse{BytesFreed: info.BytesFreed, BlocksKept: info.BlocksKept}, nil
}

// AnchorStatus returns the anchor engine's stats.
func (s *Server) AnchorStatus(ctx context.Context, req *membusspb.AnchorStatusRequest) (*membusspb.AnchorStatusResponse, error) {
	info := s.Backend.AnchorStatus()
	return &membusspb.AnchorStatusResponse{
		PeerId:        info.PeerID,
		UptimeSeconds: info.UptimeSecs,
		BlocksHeld:    info.BlocksHeld,
		Anchors:       info.Anchors,
		Backlog:       info.Backlog,
		Synced:        info.Synced,
	}, nil
}

func peerInfoToProto(p NodePeerInfo) *membusspb.NodePeerInfo {
	return &membusspb.NodePeerInfo{PeerId: p.PeerID, Addrs: append([]string(nil), p.Addrs...)}
}

// Helper used by the daemon Backend: turn a generic error into
// a gRPC status. Kept in this file so Backend implementations
// can import a single package.
func ToStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

// ErrNotImplemented is returned by the noop backend for methods
// the daemon has not wired up.
var ErrNotImplemented = errors.New("rpc: not implemented")

// formatBytes is a small helper that callers can use to render
// sizes consistently in CLI output.
func formatBytes(n uint64) string {
	const (
		KiB = 1 << 10
		MiB = 1 << 20
		GiB = 1 << 30
	)
	switch {
	case n >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(n)/float64(GiB))
	case n >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(n)/float64(MiB))
	case n >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(n)/float64(KiB))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// FormatBytes is exported for reuse by the CLI command printers.
func FormatBytes(n uint64) string { return formatBytes(n) }
