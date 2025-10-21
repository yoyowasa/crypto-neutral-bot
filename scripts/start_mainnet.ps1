# PowerShell startup for mainnet (Windows)
param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$ArgsPassthrough
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Load .env if present (simple parser: KEY=VALUE lines)
if (Test-Path .env) {
  Get-Content .env | ForEach-Object {
    if ($_ -match '^(?<k>[^#=]+)=(?<v>.*)$') {
      $k = $Matches['k'].Trim()
      $v = $Matches['v']
      [Environment]::SetEnvironmentVariable($k, $v)
    }
  }
}

# Require explicit opt-in for live trading
if (-not $env:EXCHANGE__ALLOW_LIVE) { $env:EXCHANGE__ALLOW_LIVE = 'true' }

# Default config file if not provided
if (-not $env:APP_CONFIG_FILE) { $env:APP_CONFIG_FILE = 'config/app.mainnet.yaml' }

Write-Host "[start_mainnet] APP_CONFIG_FILE=$env:APP_CONFIG_FILE"
Write-Host "[start_mainnet] EXCHANGE__ALLOW_LIVE=$env:EXCHANGE__ALLOW_LIVE"

New-Item -ItemType Directory -Force -Path logs | Out-Null

python -m bot.app.live_runner --env mainnet $ArgsPassthrough
