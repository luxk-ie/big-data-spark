param([string]$Python = '')
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
Push-Location $projectRoot
try {
    New-Item -ItemType Directory -Force .runtime,logs,metadata,data/raw,data/sample | Out-Null
    if (!$Python) {
        $bundled = "$env:USERPROFILE/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe"
        if (Test-Path $bundled) { $Python = $bundled }
        elseif (Test-Path '.runtime/python/python.exe') { $Python = (Resolve-Path '.runtime/python/python.exe').Path }
        else {
            $installer = Join-Path $projectRoot '.runtime/python-3.12.10-amd64.exe'
            Invoke-WebRequest 'https://www.python.org/ftp/python/3.12.10/python-3.12.10-amd64.exe' -OutFile $installer
            $target = Join-Path $projectRoot '.runtime/python'
            $process = Start-Process -FilePath $installer -ArgumentList @('/quiet','InstallAllUsers=0',"TargetDir=`"$target`"",'Include_pip=1','Include_launcher=0','Include_test=0','AssociateFiles=0','Shortcuts=0','PrependPath=0') -Wait -PassThru -WindowStyle Hidden
            if ($process.ExitCode -ne 0) { throw "Python installation failed: $($process.ExitCode)" }
            $Python = Join-Path $target 'python.exe'
        }
    }
    & $Python -m venv --without-pip .venv
    if ($LASTEXITCODE -ne 0) { throw 'venv creation failed' }
    & $Python -m pip --python .venv install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { throw 'Dependency install failed' }
    if (!(Test-Path '.runtime/java/*/bin/java.exe')) {
        $assets = Invoke-RestMethod 'https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=x64&image_type=jdk&os=windows&vendor=eclipse'
        $package = $assets[0].binary.package
        $package | ConvertTo-Json | Set-Content metadata/java_download.json -Encoding utf8
        Invoke-WebRequest $package.link -OutFile .runtime/java.zip
        if ((Get-FileHash .runtime/java.zip -Algorithm SHA256).Hash.ToLower() -ne $package.checksum.ToLower()) { throw 'Java checksum mismatch' }
        Expand-Archive .runtime/java.zip -DestinationPath .runtime/java -Force
    }
    Write-Output 'Setup complete. Run: ./scripts/run.ps1 src/env_check.py'
} finally { Pop-Location }
