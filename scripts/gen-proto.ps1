# Generate Go bindings for all .proto files in rpc/proto/.
#
# Requirements: protoc, protoc-gen-go, protoc-gen-go-grpc.
# Install with:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# Download protoc from:
#   https://github.com/protocolbuffers/protobuf/releases

$ErrorActionPreference = 'Stop'

# Prepend common install locations so the script works without
# manually editing PATH in the current shell.
$candidates = @(
    (Join-Path $env:USERPROFILE 'go\bin'),
    'C:\protoc\bin',
    (Join-Path $env:GOPATH 'bin')
) | Where-Object { $_ -and (Test-Path $_) }

foreach ($c in $candidates) {
    if ($env:Path -notlike ("{0};*" -f $c) -and $env:Path -notlike ("*;{0};*" -f $c) -and $env:Path -notlike ("*;{0}" -f $c)) {
        $env:Path = "$c;$env:Path"
    }
}

foreach ($bin in @('protoc','protoc-gen-go','protoc-gen-go-grpc')) {
    if (-not (Get-Command $bin -ErrorAction SilentlyContinue)) {
        throw "Required binary not found on PATH: $bin"
    }
}

$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProtoDir = Join-Path $Root 'rpc\proto'
$OutDir = Join-Path $Root 'proto'

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$protos = @(Get-ChildItem -Path $ProtoDir -Filter *.proto -Recurse | ForEach-Object { $_.FullName })
if ($protos.Count -eq 0) {
    Write-Host "No .proto files found in $ProtoDir"
    exit 0
}

& protoc `
    --proto_path=$ProtoDir `
    --go_out=$OutDir --go_opt=paths=source_relative `
    --go-grpc_out=$OutDir --go-grpc_opt=paths=source_relative `
    @protos

Write-Host ("Generated {0} proto file(s) -> {1}" -f $protos.Count, $OutDir)
