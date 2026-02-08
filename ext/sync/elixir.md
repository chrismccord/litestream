# Synchronous Replication with Elixir + ecto_sqlite3

Ensure critical transactions are replicated to S3 before `Repo.transaction` returns. If replication fails, the transaction rolls back — your app sees an error, and nothing is committed locally or remotely.

## Setup

### 1. Add the C source to your project

Copy two files into `c_src/` in your Elixir project root:

```bash
mkdir -p c_src
cp path/to/litestream/ext/sync/litestream_sync.c c_src/
cp path/to/litestream/src/sqlite3ext.h c_src/
```

`sqlite3ext.h` is a public-domain SQLite header — stable across versions, no other dependencies.

### 2. Create the Makefile

`elixir_make` expects a `Makefile` at your project root. Create one that builds the extension into `priv/native/`:

```makefile
# Makefile
PRIV_DIR = $(MIX_APP_PATH)/priv
NATIVE_DIR = $(PRIV_DIR)/native
TARGET = $(NATIVE_DIR)/litestream_sync.so

CFLAGS = -shared -fPIC -Ic_src -Wall -Wextra -O2 -pthread

.PHONY: all clean

all: $(TARGET)

$(NATIVE_DIR):
	mkdir -p $(NATIVE_DIR)

$(TARGET): c_src/litestream_sync.c c_src/sqlite3ext.h | $(NATIVE_DIR)
	$(CC) $(CFLAGS) -o $@ c_src/litestream_sync.c

clean:
	rm -rf $(PRIV_DIR)/native
```

`MIX_APP_PATH` is set automatically by `elixir_make` and points to `_build/dev/lib/my_app` (or prod, etc.).

### 3. Add `elixir_make` to mix.exs

```elixir
# mix.exs
defmodule MyApp.MixProject do
  use Mix.Project

  def project do
    [
      app: :my_app,
      version: "0.1.0",
      elixir: "~> 1.17",
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_clean: ["clean"],
      # ...
    ]
  end

  defp deps do
    [
      {:ecto_sqlite3, "~> 0.17"},
      {:elixir_make, "~> 0.9", runtime: false},
      # ...
    ]
  end
end
```

Now `mix compile` builds the `.so` automatically. `mix clean` removes it.

### 4. Bootstrap the VFS at application start

The extension registers a VFS shim as the default SQLite VFS. It must be loaded **once** before any Ecto connections open:

```elixir
# lib/my_app/application.ex
defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    load_litestream_sync!()

    children = [
      MyApp.Repo,
      # ...
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp load_litestream_sync! do
    ext_path = Path.join(:code.priv_dir(:my_app), "native/litestream_sync")
    {:ok, conn} = Exqlite.Sqlite3.open(":memory:")
    :ok = Exqlite.Sqlite3.enable_load_extension(conn, true)
    :ok = Exqlite.Sqlite3.execute(conn, "SELECT load_extension('#{ext_path}')")
    :ok = Exqlite.Sqlite3.close(conn)
  end
end
```

After this call, the "litestream" VFS is the process-wide default. All subsequent connections automatically use it. The shim is a pure passthrough (zero overhead) unless you explicitly set `PRAGMA litestream_sync = 1`.

### 5. Configure the socket path

Set the `LITESTREAM_SOCKET` environment variable to match your litestream config:

```bash
export LITESTREAM_SOCKET="/var/run/litestream.sock"
```

This is read once when the extension loads. If unset, it defaults to `/var/run/litestream.sock`.

You can also override per-database at runtime (rarely needed):

```elixir
Repo.query!("PRAGMA litestream_socket = '/tmp/my-other.sock'")
```

## Usage

### Synchronous transaction helper

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.SQLite3

  @doc """
  Like `transaction/2`, but ensures the transaction is replicated to S3
  before returning. Rolls back if replication fails.
  """
  def synced_transaction(fun, opts \\ []) when is_function(fun, 0) do
    transaction(fn ->
      query!("PRAGMA synchronous=FULL")
      query!("PRAGMA litestream_sync = 1")
      fun.()
    end, opts)
  end
end
```

Then:

```elixir
MyApp.Repo.synced_transaction(fn ->
  Repo.insert!(%Payment{amount: 100, user_id: user.id})
end)
# If we reach here: committed locally AND uploaded to S3.
# If error/rollback: nothing committed anywhere.
```

Normal transactions that don't call `synced_transaction` are completely unaffected — the VFS is a no-op passthrough unless the PRAGMA is set.

## How it works

```
Repo.transaction →
  PRAGMA litestream_sync = 1  (increments sync counter in VFS shim)
  INSERT ...                  (writes to WAL as normal)
  COMMIT →
    SQLite calls xSync on WAL file →
      VFS shim sees sync counter > 0 →
        POST /sync-replicate to litestream Unix socket →
          litestream: DB.Sync() → Replica.Sync() → S3 upload
        success → real fsync → COMMIT completes, counter decremented
        failure → SQLITE_IOERR → SQLite rolls back, counter decremented
```

## Requirements

- litestream running with `socket.enabled: true` in its config
- `LITESTREAM_SOCKET` env var pointing to the litestream control socket
- `PRAGMA synchronous=FULL` — in WAL mode, the default (`NORMAL`) only fsyncs during checkpoints, not on each commit, so the xSync interception would never fire. Set this on each connection, e.g. via ecto_sqlite3 config or in your `synced_transaction` helper.
- A C compiler on the build machine (gcc or clang)
