# 何をするスクリプト？
# - 自己昇格（管理者）して Windows の時刻を NTP 同期（完全自動）
# - そのまま ops-check を起動し、数量刻み/最小制約が取得できるまで待機（最大タイムアウト）
# - 取得できたら即終了し、主要項目をダンプして確認を支援

param(
  [int]$Seconds = 180  # 何をする？→ 最大待機秒数（既定180秒。値が埋まれば早期終了）
)

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Err($msg)  { Write-Host "[ERR ] $msg" -ForegroundColor Red }

# 1) 自己昇格（管理者）
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
  try {
    Write-Info "管理者権限で再起動します…"
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName  = (Get-Process -Id $PID).Path
    $psi.Arguments = "-ExecutionPolicy Bypass -File `"$PSCommandPath`" -Seconds $Seconds"
    $psi.Verb      = 'runas'
    [System.Diagnostics.Process]::Start($psi) | Out-Null
  } catch {
    Write-Err "管理者再起動に失敗しました: $_"
  }
  exit
}

# 2) 時刻同期（NTP）
try {
  Write-Info "NTP 同期: time.google.com / time.cloudflare.com / time.windows.com に設定…"
  w32tm /config /manualpeerlist:"time.google.com,0x8 time.cloudflare.com,0x8 time.windows.com,0x8" /syncfromflags:manual /update | Out-Null
  Restart-Service w32time -Force -ErrorAction SilentlyContinue
  Start-Sleep -Seconds 2
  w32tm /resync /force | Out-Null
  Write-Info "NTP 同期 OK。状態:"
  w32tm /query /status
  w32tm /query /peers
} catch {
  Write-Warn "NTP 同期に失敗（権限/ネットワークを確認）。$_"
}

# 3) ops-check を起動（dry-runで安全に収集）
Write-Info "ops-check を起動し、数量刻み/最小制約が埋まるまで待機します（最大 $Seconds 秒）…"
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  Write-Err "python が見つかりません。仮想環境の有効化や Path を確認してください。"
  exit 1
}

$argsList = @('-m','bot.app.live_runner','--dry-run','--ops-check','--ops-out-json','ops-check.json','--log-level','INFO')
$proc = Start-Process -FilePath python -ArgumentList $argsList -PassThru

# 4) ポーリング：キーが None 以外になるまで待機（早期終了あり）
$start = Get-Date
$ok = $false
while (-not $ok) {
  Start-Sleep -Seconds 5
  try {
    $py = @"
import json, os
p='ops-check.json'
result={'ok': False, 'vals': None}
if os.path.exists(p):
    try:
        rows=json.load(open(p,'r',encoding='utf-8'))
        if rows:
            r=rows[0]
            keys=['qty_step_spot','qty_step_perp','qty_common_step','min_qty_spot','min_qty_perp','min_notional_spot','min_notional_perp']
            vals={k:r.get(k) for k in keys}
            result['vals']=vals
            result['ok']=any(v is not None for v in vals.values())
    except Exception:
        pass
print(json.dumps(result))
"@
    $tmp = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), "opscheck_probe_$(Get-Random).py")
    Set-Content -Path $tmp -Value $py -Encoding UTF8
    $out = & python $tmp
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    $json = $null
    try { $json = $out | ConvertFrom-Json } catch {}
    if ($null -ne $json) {
      if ($json.ok) {
        Write-Info ("取得成功: " + ($json.vals | ConvertTo-Json -Compress))
        $ok = $true
        break
      } else {
        Write-Info ("未取得（継続待機）: " + (($json.vals | ConvertTo-Json -Compress)))
      }
    }
  } catch {}
  if (((Get-Date) - $start).TotalSeconds -ge $Seconds) { break }
}

# 5) 後片付け＆最終表示
try { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } catch {}

if (-not $ok) {
  Write-Warn "タイムアウト：数量刻み/最小制約が取得できませんでした。ネットワーク／認証／Bybit時刻要件を再確認してください。"
  exit 2
}

Write-Info "完了。ops-check.json に数量刻み/最小制約の値が出力されています。"
