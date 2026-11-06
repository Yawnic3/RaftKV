$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$goCommand = Get-Command go -ErrorAction SilentlyContinue
if ($goCommand) {
    $goBinary = $goCommand.Source
} else {
    $goBinary = Join-Path $PWD '.tools/go/bin/go.exe'
}
if (!(Test-Path -LiteralPath $goBinary)) { throw 'Install Go 1.24 or newer and add it to PATH.' }
New-Item -ItemType Directory -Force bin, data | Out-Null
& $goBinary build -o bin/raftkv.exe ./cmd/raftkv
if ($LASTEXITCODE -ne 0) { throw 'Server build failed' }
& $goBinary build -o bin/raftkvctl.exe ./cmd/raftkvctl
if ($LASTEXITCODE -ne 0) { throw 'Client build failed' }
if (Test-Path data/local-processes.json) { throw 'A cluster process record exists. Run scripts/stop-local.ps1 first.' }
$processRecords = @()
try {
    foreach ($nodeNumber in 1..3) {
        $process = Start-Process -FilePath (Join-Path $PWD 'bin/raftkv.exe') `
            -ArgumentList '-config', "config/node$nodeNumber.json" `
            -WorkingDirectory $PWD -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput "data/node$nodeNumber.stdout.log" `
            -RedirectStandardError "data/node$nodeNumber.stderr.log"
        $processRecords += @{
            id = "node$nodeNumber"
            pid = $process.Id
            started = $process.StartTime.ToUniversalTime().ToString('o')
        }
    }
    $deadline = [DateTime]::UtcNow.AddSeconds(15)
    $lastProbeError = 'No leader reported yet.'
    do {
        $healthy = 0
        $leader = ''
        foreach ($nodeNumber in 1..3) {
            try {
                $status = Invoke-RestMethod "http://127.0.0.1:800$nodeNumber/v1/status" -TimeoutSec 2
                if ($status.id -ne "node$nodeNumber" -or $status.error) {
                    throw "Unexpected or failed node at port 800$nodeNumber"
                }
                if ($status.role -eq 'leader') {
                    $leader = $status.id
                }
                $healthy++
            } catch {
                $lastProbeError = $_.Exception.Message
            }
        }
        if ($healthy -eq 3 -and $leader) {
            foreach ($record in $processRecords) {
                $process = Get-Process -Id $record.pid -ErrorAction Stop
                if ($process.StartTime.ToUniversalTime().ToString('o') -ne $record.started) {
                    throw 'A newly started node exited unexpectedly. Inspect data/*.log.'
                }
            }
            $processRecords | ConvertTo-Json | Set-Content data/local-processes.json
            Write-Host "Cluster started. Leader: $leader. Logs: data/*.log"
            return
        }
        Start-Sleep -Milliseconds 100
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Cluster startup timed out. Last probe: $lastProbeError. Inspect data/*.log."
} catch {
    foreach ($record in $processRecords) {
        $process = Get-Process -Id $record.pid -ErrorAction SilentlyContinue
        if ($process -and $process.StartTime.ToUniversalTime().ToString('o') -eq $record.started) {
            Stop-Process -Id $record.pid
        }
    }
    throw
}
