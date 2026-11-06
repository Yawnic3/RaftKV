$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (!(Test-Path data/local-processes.json)) {
    Write-Host 'No managed local cluster record.'
    return
}
$records = Get-Content data/local-processes.json -Raw | ConvertFrom-Json
foreach ($record in $records) {
    $process = Get-Process -Id $record.pid -ErrorAction SilentlyContinue
    if (!$process) { continue }
    $sameStart = $process.StartTime.ToUniversalTime().ToString('o') -eq $record.started
    $expectedPath = Join-Path $PWD 'bin/raftkv.exe'
    if ($sameStart -and $process.Path -eq $expectedPath) {
        Stop-Process -Id $record.pid
    } else {
        Write-Warning "PID $($record.pid) does not match the recorded node; leaving it alone."
    }
}
Remove-Item -LiteralPath data/local-processes.json
Write-Host 'Local nodes stopped. Durable data retained.'
