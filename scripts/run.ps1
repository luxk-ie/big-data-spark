param([Parameter(Mandatory=$true)][string]$Script, [Parameter(ValueFromRemainingArguments=$true)][string[]]$ScriptArgs)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$jdk = Get-ChildItem "$projectRoot/.runtime/java" -Directory | Select-Object -First 1
if (!$jdk) { throw 'Java missing. Run scripts/setup.ps1 first.' }
$env:JAVA_HOME = $jdk.FullName
$env:PATH = "$env:JAVA_HOME/bin;$env:PATH"
$env:PYSPARK_PYTHON = "$projectRoot/.venv/Scripts/python.exe"
$env:SPARK_LOCAL_IP = '127.0.0.1'
Push-Location $projectRoot
try {
    # Windows PowerShell 5 reports native stderr as errors; Spark writes warnings there.
    $ErrorActionPreference = 'Continue'
    & $env:PYSPARK_PYTHON $Script @ScriptArgs
    $result = $LASTEXITCODE
} finally {
    $ErrorActionPreference = 'Stop'
    Pop-Location
}
exit $result
