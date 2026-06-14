# Generate Go bindings for all .proto files in rpc/proto/.
#
# Requirements: protoc, protoc-gen-go, protoc-gen-go-grpc.
# Install with:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

$ErrorActionPreference = 'Stop'

$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProtoDir = Join-Path $Root 'rpc\proto'
$OutDir = Join-Path $Root 'proto'

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$protos = Get-ChildItem -Path $ProtoDir -Filter *.proto -Recurse | ForEach-Object { $_.FullName }
if ($protos.Count -eq 0) {
    Write-Host "No .proto files found in $ProtoDir"
    exit 0
}

protoc `
  --proto_path=$ProtoDir `
  --go_out=$OutDir --go_opt=paths=source_relative `
  --go-grpc_out=$OutDir --go-grpc_opt=paths=source_relative `
  @protos
