param(
    [ValidateSet('baseline', 'after', 'after_cache', 'after_hash')][string]$Variant = 'baseline',
    [ValidateRange(3, 20)][int]$Runs = 3,
    [string]$Manifest = 'data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/manifest.json',
    [string]$Zones = 'data/reference/taxi_zone_lookup.csv'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$batch = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
$csvPath = Join-Path $root 'reports/benchmark.csv'
$memory = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
$commit = (git -C $root rev-parse HEAD).Trim()
$pipelineDirty = [bool](git -C $root status --porcelain -- src/pipeline_tlc.py)
$invariant = [System.Globalization.CultureInfo]::InvariantCulture

foreach ($number in 0..$Runs) {
    $label = if ($number -eq 0) { 'warmup' } else { "run$number" }
    $runRoot = Join-Path $root "data/processed/checkpoint4/$Variant/$batch/$label"
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    Write-Host "Checkpoint 4 $Variant $label ($batch)"
    & (Join-Path $PSScriptRoot 'run.ps1') -Script 'src/pipeline_tlc.py' -ScriptArgs @(
        '--manifest', $Manifest, '--zones', $Zones, '--output', $runRoot, '--stage-evidence'
    ) *> (Join-Path $runRoot 'console.log')
    if ($LASTEXITCODE -ne 0) { throw "Pipeline failed for $label. See $runRoot/console.log" }
    $versions = @(Get-ChildItem -LiteralPath $runRoot -Directory)
    if ($versions.Count -ne 1) { throw "Expected one output version for $label" }
    $output = $versions[0].FullName
    $evidence = Get-Content (Join-Path $output 'evidence.json') -Raw | ConvertFrom-Json
    $stageFile = Join-Path $output 'stages.json'
    if (!(Test-Path -LiteralPath $stageFile) -or !(Test-Path -LiteralPath (Join-Path $output 'jobs.json'))) {
        throw "Spark execution evidence missing for $label"
    }
    $goldHash = (Get-FileHash (Join-Path $output 'gold.parquet') -Algorithm SHA256).Hash.ToLowerInvariant()
    $row = [pscustomobject][ordered]@{
        variant = $Variant; batch = $batch; run = $label; warmup = ($number -eq 0)
        commit = $commit; pipeline_dirty = $pipelineDirty; code_sha256 = $evidence.code_sha256
        raw_sha256 = $evidence.raw_sha256; raw_bytes = $evidence.raw_bytes
        lookup_sha256 = $evidence.lookup_sha256; lookup_rows = $evidence.lookup_rows
        spark_version = $evidence.spark_version; master = $evidence.master
        logical_cores = $evidence.benchmark_context.logical_cores
        physical_memory_bytes = $memory; driver_memory = $evidence.benchmark_context.driver_memory
        aqe_enabled = $evidence.benchmark_context.aqe_enabled
        shuffle_partitions = $evidence.benchmark_context.shuffle_partitions
        elapsed_seconds = $evidence.elapsed_seconds.ToString('0.000', $invariant)
        quality_and_gold_seconds = $evidence.action_seconds.quality_and_gold_checks.ToString('0.000', $invariant)
        duplicate_full_seconds = $evidence.action_seconds.duplicate_full_rows.ToString('0.000', $invariant)
        gold_collect_seconds = $evidence.action_seconds.gold_collect.ToString('0.000', $invariant)
        raw_input_rows = $evidence.metrics.raw_input_rows
        in_scope_rows = $evidence.metrics.in_scope_rows
        joined_rows = $evidence.metrics.joined_rows
        output_rows = $evidence.metrics.output_rows
        trip_count = $evidence.metrics.trip_count
        zone_status_json = ($evidence.metrics.zone_status | ConvertTo-Json -Compress)
        gold_sha256 = $goldHash; completed_stages = $evidence.benchmark_context.completed_stages
        output = $output
    }
    if (Test-Path -LiteralPath $csvPath) { $row | Export-Csv $csvPath -NoTypeInformation -Encoding UTF8 -Append }
    else { $row | Export-Csv $csvPath -NoTypeInformation -Encoding UTF8 }
    Write-Host "$label`: $($evidence.elapsed_seconds) s; Gold SHA-256 $goldHash"
}
$measured = @(Import-Csv $csvPath | Where-Object { $_.batch -eq $batch -and $_.variant -eq $Variant -and $_.warmup -eq 'False' } |
    ForEach-Object { [double]::Parse($_.elapsed_seconds, $invariant) } | Sort-Object)
$median = $measured[[int][Math]::Floor($measured.Count / 2)]
if ($measured.Count % 2 -eq 0) { $median = ($measured[$measured.Count / 2 - 1] + $measured[$measured.Count / 2]) / 2 }
Write-Host "Median $Variant ($($measured.Count) measured runs): $($median.ToString('0.000', $invariant)) s"
