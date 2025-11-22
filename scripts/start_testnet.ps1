# PowerShell startup for testnet (Windows)
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

# Disable live trading by default on testnet
if (-not $env:EXCHANGE__ALLOW_LIVE) { $env:EXCHANGE__ALLOW_LIVE = 'false' }

# Default config file for testnet
if (-not $env:APP_CONFIG_FILE) {
  if (Test-Path 'config/app.testnet.yaml') { $env:APP_CONFIG_FILE = 'config/app.testnet.yaml' }
  elseif (Test-Path 'config/app.yaml') { $env:APP_CONFIG_FILE = 'config/app.yaml' }
  else { $env:APP_CONFIG_FILE = 'config/app.mainnet.example.yaml' }
}

Write-Host "[start_testnet] APP_CONFIG_FILE=$env:APP_CONFIG_FILE"
Write-Host "[start_testnet] EXCHANGE__ALLOW_LIVE=$env:EXCHANGE__ALLOW_LIVE"

New-Item -ItemType Directory -Force -Path logs | Out-Null

python -m bot.app.live_runner --env testnet $ArgsPassthrough

