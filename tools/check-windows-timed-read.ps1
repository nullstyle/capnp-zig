# Run this before the full Windows suite so a timed-read regression identifies
# its test name and fails within a bounded native execution window.
param(
    [ValidateSet('Debug', 'ReleaseSafe')]
    [string]$Optimize = 'Debug'
)

$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'This focused gate requires Windows and PowerShell 7.' }

Push-Location (Join-Path $PSScriptRoot '..')
$receiptDirectory = Join-Path (Get-Location) ".zig-cache/ci/windows-timed-read/$Optimize"
New-Item -ItemType Directory -Force $receiptDirectory | Out-Null
$executable = Join-Path $receiptDirectory 'timed-read.exe'
$stdoutPath = Join-Path $receiptDirectory 'stdout.log'
$stderrPath = Join-Path $receiptDirectory 'stderr.log'
# Never accept a previous executable or evidence file after a failed rebuild.
Get-ChildItem $receiptDirectory -File | Remove-Item -Force
$process = $null
$elapsed = [Diagnostics.Stopwatch]::StartNew()
$exitCode = 1
$timedOut = $false
$phase = 'build'
$revision = (& git rev-parse HEAD)
$zigVersion = ''
$binaryHash = ''

try {
    $zigVersion = (& mise exec -- zig version)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve the repository-pinned Zig compiler.' }
    $buildArguments = @(
        'exec', '--', 'zig', 'test', "-O$Optimize", '--test-no-exec',
        '--dep', 'capnpc-zig', '--dep', 'io-write-compat',
        '-Mroot=tests/rpc/transport/tcp/rpc_tick_idle_test.zig',
        "-O$Optimize", '--dep', 'capnpc-zig', '-Mcapnpc-zig=src/lib.zig',
        "-O$Optimize", '--dep', 'capnpc-zig',
        '-Mio-write-compat=tests/rpc/support/io_write_compat.zig',
        "-femit-bin=$executable"
    )
    & mise @buildArguments 2>&1 | Tee-Object (Join-Path $receiptDirectory 'build.log')
    $exitCode = $LASTEXITCODE
    if ($exitCode -eq 0) {
        $binaryHash = (Get-FileHash $executable -Algorithm SHA256).Hash.ToLowerInvariant()
        $phase = 'run'
        $exitCode = 1
        $process = Start-Process $executable -PassThru -NoNewWindow `
            -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
        # Retain the native handle before polling so ExitCode stays available.
        $null = $process.Handle
        $elapsed.Restart()
        $stdoutLines = 0
        $stderrLines = 0
        while (-not $process.HasExited -and $elapsed.Elapsed.TotalSeconds -lt 90) {
            Start-Sleep -Seconds 1
            $stdout = @(Get-Content $stdoutPath -ErrorAction SilentlyContinue)
            $stderr = @(Get-Content $stderrPath -ErrorAction SilentlyContinue)
            $stdout | Select-Object -Skip $stdoutLines | Write-Host
            $stderr | Select-Object -Skip $stderrLines | Write-Host
            $stdoutLines = $stdout.Count
            $stderrLines = $stderr.Count
            $process.Refresh()
        }
        if (-not $process.HasExited) {
            $timedOut = $true
            $exitCode = 124
            try {
                Get-CimInstance Win32_Process -OperationTimeoutSec 10 |
                    Where-Object { $_.ProcessId -eq $process.Id } |
                    Format-List ProcessId, ParentProcessId, CreationDate, ExecutablePath, CommandLine |
                    Out-String | Tee-Object (Join-Path $receiptDirectory 'timeout.log') | Write-Host
                $process.Threads | Select-Object Id, ThreadState, WaitReason |
                    Format-Table | Out-String |
                    Tee-Object (Join-Path $receiptDirectory 'timeout.log') -Append | Write-Host
            } catch { Write-Warning "Timeout snapshot unavailable: $_" }
            Write-Warning 'Focused timed-read tests exceeded 90 seconds; the final test name is in stderr.log.'
        } else {
            $process.WaitForExit()
            $exitCode = $process.ExitCode
            if ($null -eq $exitCode) { throw 'The focused test process did not expose an exit status.' }
        }
    }
} catch {
    $exitCode = 1
    $_ | Out-String | Tee-Object (Join-Path $receiptDirectory 'gate-error.log') | Write-Host
} finally {
    if ($null -ne $process) {
        if (-not $process.HasExited) {
            & taskkill.exe /PID $process.Id /T /F 2>&1 |
                Tee-Object (Join-Path $receiptDirectory 'termination.log') | Write-Host
            if (-not $process.WaitForExit(5000)) {
                Write-Warning 'The focused test process did not exit after taskkill.'
                $exitCode = 1
            }
        }
        $process.Dispose()
    }
    foreach ($log in @($stdoutPath, $stderrPath)) {
        if (Test-Path $log) { Get-Content $log | Write-Host }
    }
    [ordered]@{
        revision = $revision
        zig = $zigVersion
        optimize = $Optimize
        phase = $phase
        executableSha256 = $binaryHash
        runLimitSeconds = 90
        elapsedSeconds = $elapsed.Elapsed.TotalSeconds
        timedOut = $timedOut
        exitCode = $exitCode
    } | ConvertTo-Json | Set-Content (Join-Path $receiptDirectory 'receipt.json')
    Pop-Location
}
exit $exitCode
