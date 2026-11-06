# Validation record

Validated on **2026-11-06**, on Windows amd64, with the repository at
`C:\Users\qingy\OneDrive\Desktop\DistributedSystems`.

## Toolchain

- Go `go1.27.3 windows/amd64`, downloaded from the official Go distribution.
- Workspace-local WinLibs MinGW-w64 UCRT GCC 16.2.0 for cgo/race support.
- Both downloaded archives were checked against the publisher's SHA-256 digest.
- No Go dependencies beyond the standard library.

The preinstalled GCC 7.3.0 could compile cgo but could not run Go's race-test
binaries. A current compiler was installed only under the ignored `.tools`
directory. Sandbox restrictions also prevented loopback tests and native DLL
loading, so execution checks used approved access outside that sandbox.

To reproduce with these workspace-local tools in PowerShell:

```powershell
$env:PATH = "$PWD\.tools\go\bin;$PWD\.tools\mingw64\bin;$env:PATH"
$env:CC = "$PWD\.tools\mingw64\bin\gcc.exe"
$env:CGO_ENABLED = '1'
$env:GOCACHE = "$PWD\.cache\go-build"

go test -race ./... -count=3 -timeout=180s
go vet ./...
go build ./cmd/...
gofmt -l cmd internal tests
```

The toolchains are local validation aids, not committed project dependencies.
On Linux, an installed supported Go toolchain and GCC suffice.

## Results

| Check | Observed result |
|---|---|
| Full native suite | Passed, including actual subprocess termination/restart |
| Full race-enabled suite, three repetitions | Passed, no detected data races; integration package completed in 37.009 seconds |
| `go vet ./...` | Passed without diagnostics |
| `gofmt -l cmd internal tests` | Empty output |
| Native Windows command builds | Passed |
| Linux amd64 command cross-build, cgo disabled | Passed |
| Windows local startup script | Started three separate processes and elected a leader |
| Compiled CLI PUT / GET / DELETE | Passed; GET returned `hello` |
| Local cluster status | All three nodes reported commit/applied index 4 at the sample observation |
| Script stop/start persistence drill | `restart-demo=retained` survived complete cluster stop/start |
| Demo process cleanup | All script-managed nodes stopped after validation; data retained |
| Docker/Compose execution | Not run: Docker was not installed on this host |

The repeated race run also enabled coverage instrumentation. Default Go coverage
attributes integration work to the test package rather than imported packages;
no aggregate coverage percentage is claimed. Use `-coverpkg=./internal/...` if
collecting cross-package coverage for future changes.

## Coverage of failure scenarios

The automated suite exercises three healthy HTTP members, reads seeded through
a different node, concurrent clients, leader shutdown, replacement election,
writes with one node down, lagging follower snapshot catch-up, suffix conflict
repair, minority write/read rejection, complete link outage, healed links,
whole-cluster restart, and abrupt OS process kills. Protocol unit tests check
durable votes, freshness restrictions, current-term commitment, snapshot restore,
and refusal to grant a vote on storage failure. WAL tests cover torn tail
recovery, corruption rejection, compaction restoration, and directory locking.

No power-cut, remote multi-machine, sustained load, Docker runtime, formal
verification, or exhaustive linearizability checking is represented by these
results. Linux cross-compilation establishes build compatibility, not a Linux
runtime test. CI includes a Linux race-enabled suite for subsequent execution.
