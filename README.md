# Noxy DynamoDB

A DynamoDB client for the [Noxy](https://github.com/estevaofon/noxy) language,
shipped as a **process extension**: `noxy --get` downloads a prebuilt binary
for your platform, verifies it, and records its hash in `noxy.sum`. No Go
toolchain, no build step.

Requires Noxy **0.23.0** or newer. Binaries are published for linux/amd64,
linux/arm64, darwin/amd64, darwin/arm64, windows/amd64 and windows/arm64.

## Installation

```bash
noxy --get github.com/estevaofon/noxy_dynamodb@v0.2.0
```

Without `@version`, `--get` resolves the newest release tag. The package lands
in `noxy_libs/github_com/estevaofon/noxy_dynamodb/` with the binary for your
OS/arch in `bin/`; `noxy.sum` gets one line for the manifest plus one per
published binary, so commit it — the same lockfile verifies a teammate's
macOS download and a Lambda's Linux binary.

## Usage

```noxy
use github_com.estevaofon.noxy_dynamodb as dynamodb

func main() -> void
    // Credentials and region come from the environment (see below).
    let client: dynamodb.Client = dynamodb.connect({"region": "us-east-1"})

    let item: map[string, any] = {
        "id": "user_123",
        "name": "Estevao",
        "email": "estevao@example.com",
        "age": 30
    }
    if dynamodb.put_item(client, "Users", item) then
        print("Item saved")
    end

    let found: map[string, any]? = dynamodb.get_item(client, "Users", {"id": "user_123"})
    if found != null then
        print(f"Found user: {found['name']}")
    end

    let users: map[string, any][] = dynamodb.scan(client, "Users")
    print(f"{length(users)} user(s)")

    dynamodb.close(client)
end

main()
```

## API

| Function | Returns | On failure |
|---|---|---|
| `connect(options: map[string, any])` | `Client` | raises |
| `close(client: Client)` | `void` | raises (unknown or already closed client) |
| `put_item(client, table: string, item: map[string, any])` | `bool` | `false` |
| `get_item(client, table: string, key: map[string, any])` | `map[string, any]?` — `null` when the key does not exist | raises |
| `update_item(client, table: string, key: map[string, any], update_expression: string, expression_values: map[string, any])` | `bool` | `false` |
| `delete_item(client, table: string, key: map[string, any])` | `bool` | `false` |
| `scan(client, table: string)` | `map[string, any][]` — every item, all pages | raises |
| `query(client, table: string, key_condition: string, expression_values: map[string, any])` | `map[string, any][]` — all pages | raises |

`Client` is a struct with a single field, `handle: int`, minted by `connect`.
`update_item` and `query` take DynamoDB expressions verbatim, e.g.
`dynamodb.update_item(client, "Users", {"id": "u1"}, "SET age = :a", {":a": 31})`
and `dynamodb.query(client, "Users", "id = :id", {":id": "u1"})`.

### Errors

A failure inside the extension is a runtime error,
`extension 'dynamodb' failed: <message from AWS>`, with the Noxy stack of the
call site. The raising functions can be captured with `call_result`:

```noxy
use errors select *

let r = call_result(dynamodb.scan, client, "Users")
if r.ok then
    print(length(r.value))
else
    print(f"scan failed: {r.failure.message}")
end
```

If the plugin process dies, the next call fails with
`extension 'dynamodb' trapped: ...` and the extension stays unusable for the
rest of the program (restart it). Each call has a 60 s deadline.

### Connect options

| Key | Meaning |
|---|---|
| `"region"` | AWS region; else `AWS_REGION` / the shared config, else `us-east-1` |
| `"endpoint"` | Custom endpoint, e.g. `"http://localhost:8000"` for DynamoDB Local |
| `"profile"` | A named profile from `~/.aws/config` / `~/.aws/credentials` |

Credentials come from the default AWS chain: `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` (/ `AWS_SESSION_TOKEN`), the shared config files
(`AWS_PROFILE`), or the instance / Lambda execution role. An unknown key is
an error.

### Types

| Noxy | DynamoDB | Comes back as |
|---|---|---|
| `int`, `float` | N | `float` (numbers are always floats on the way back) |
| `string` | S | `string` |
| `bool` | BOOL | `bool` |
| `bytes` | B | `bytes` |
| array | L | array |
| `map[string, T]` | M | `map[string, any]` |
| struct | M (its fields) | `map[string, any]` |
| `null` | NULL | `null` |

String, number and binary *sets* come back as arrays.

## AWS Lambda

Deploy the package directory as it is installed —
`noxy_libs/github_com/estevaofon/noxy_dynamodb/` with `noxy_ext.toml`,
`noxy_dynamodb.nx` and `bin/noxy-plugin-dynamodb-linux-amd64` (or
`-linux-arm64` on Graviton) with the execute bit — next to your `noxy.mod`
and `noxy.sum`. `noxy --get` on a Windows or macOS workstation only
downloads that workstation's binary, but it records the Linux hashes in
`noxy.sum`; fetch the Lambda's binary from the release page into `bin/`:

```bash
curl -L -o noxy_libs/github_com/estevaofon/noxy_dynamodb/bin/noxy-plugin-dynamodb-linux-amd64 \
  https://github.com/estevaofon/noxy_dynamodb/releases/download/v0.2.0/noxy-plugin-dynamodb-linux-amd64
chmod +x noxy_libs/github_com/estevaofon/noxy_dynamodb/bin/noxy-plugin-dynamodb-linux-amd64
```

The VM verifies the binary against `noxy.sum` before starting it. Nothing
needs to be on `PATH`. See `docs/AWS_LAMBDA_LAYER.md` in the Noxy repository
for the layer layout.

## Migrating from v0.1.x

- Import the package as `use github_com.estevaofon.noxy_dynamodb as dynamodb`
  (the wrapper is now `noxy_dynamodb.nx`; the old `...noxy_dynamodb.dynamodb`
  path is gone).
- No build step: `build_plugin.sh` / `build_plugin.ps1` and
  `sys_load_plugin` are gone; `noxy --get` installs the binary.
- `Client` has `handle: int` instead of `id: string`; `connect` raises on
  failure instead of returning `Client("")`.
- `get_item` returns `map[string, any]?`; `scan` and `query` return
  `map[string, any][]` and follow pagination (all items, not just the first
  page). `put_item`, `update_item` and `delete_item` still return `bool`.
- New: `close(client)`; connect options `"endpoint"` and `"profile"`.

## Development

The extension is a Go program on the Noxy plugin SDK
(`github.com/estevaofon/noxy/sdk/noxyplugin`). To run a checkout without a
release, build your platform's asset (the name is in `[binaries]` of
`noxy_ext.toml`) and point a project at the checkout:

```bash
go test ./...
CGO_ENABLED=0 go build -o bin/noxy-plugin-dynamodb-windows-amd64.exe .   # or -linux-amd64, -darwin-arm64, ...
```

Copy (or symlink) the checkout to
`<project>/noxy_libs/github_com/estevaofon/noxy_dynamodb`; without a
`noxy.sum` entry the VM prints a trust-on-first-use warning and runs it.

Releasing: push a tag `vX.Y.Z`. The GitHub Actions workflow
(`.github/workflows/release.yml`) runs `release/build.sh dynamodb`, which
builds the six binaries with `CGO_ENABLED=0`, writes `checksums.txt`, and
publishes everything as release assets — the exact layout `noxy --get`
expects.
