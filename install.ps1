# ============================================================================
#  install.ps1 — one-shot setup for the Social Media Intelligence System
#
#  Usage:   powershell -ExecutionPolicy Bypass -File install.ps1
#  Flags:   -Reset    wipe .venv and reinstall from scratch
#           -CpuOnly  skip CUDA torch even if an NVIDIA GPU is present
#
#  Re-runnable: every phase checks existing state and skips what's done.
#  Honest hand-offs (cannot be automated): API keys (web portals),
#  Ollama installer (its own setup.exe), Telegram login (interactive).
# ============================================================================
param([switch]$Reset, [switch]$CpuOnly)
 $ErrorActionPreference = "Continue"

if (-not (Test-Path "main.py")) {
    Write-Host "Run this from the repository root (main.py not found here)." -ForegroundColor Red
    exit 1
}
 $VenvPy = Join-Path $Pwd ".venv\Scripts\python.exe"

function Step($n, $m) { Write-Host "`n=== [$n] $m ===" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "  [OK]   $m" -ForegroundColor Green }
function Warn($m) { Write-Host "  [WARN] $m" -ForegroundColor Yellow }
function Fail($m) { Write-Host "  [FAIL] $m" -ForegroundColor Red }
function AskDefault($prompt, $default) {
    $a = Read-Host "  ? $prompt [$default]"
    if ([string]::IsNullOrWhiteSpace($a)) { $default } else { $a }
}

 $Summary = [ordered]@{}

# ---------------------------------------------------------------- 1. PYTHON
Step 1 "Python interpreter"
 $pyCmd = $null; $pyVer = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
    foreach ($v in @("3.12","3.11","3.10","3.13","3.14")) {
        & py -$v -c "print(1)" *> $null
        if ($LASTEXITCODE -eq 0) { $pyCmd = "py -$v"; $pyVer = $v; break }
    }
}
if (-not $pyCmd -and (Get-Command python -ErrorAction SilentlyContinue)) {
    $v = & python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>$null
    if ($LASTEXITCODE -eq 0 -and $v -match '^3\.\d+$') { $pyCmd = "python"; $pyVer = $v }
}
if (-not $pyCmd) {
    Fail "No usable Python found. Install Python 3.12: https://www.python.org/downloads/"
    exit 1
}
Ok "Selected Python $pyVer (via: $pyCmd)"
if ($pyVer -in @("3.13","3.14")) {
    Warn "Python $pyVer has limited/no CUDA torch wheels — Layer 1 (GPU ensemble) may be unavailable."
    if ($pyVer -eq "3.14") { Warn "Strongly consider installing 3.12 alongside and re-running." }
    $CpuOnly = $true   # don't attempt CUDA install on 3.14
}
 $Summary["Python"] = "$pyVer (.venv)"

# ---------------------------------------------------------------- 2. HARDWARE
Step 2 "Hardware audit"
 $gpuName = $null; $vramMB = 0
if (Get-Command nvidia-smi -ErrorAction SilentlyContinue) {
    $q = nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>$null
    if ($q -and $q -match '^(.+?),\s*(\d+)\s*MiB') {
        $gpuName = $Matches[1].Trim(); $vramMB = [int]$Matches[2]
    }
}
if ($gpuName) {
    Ok "GPU: $gpuName ($([math]::Round($vramMB/1024,1)) GB VRAM)"
    if ($vramMB -lt 4000) { Warn "Under 4 GB VRAM — Layer 1 will be tight; consider -CpuOnly." }
    elseif ($vramMB -lt 8000) { Warn "6-8 GB VRAM: ensemble fits; running Ollama 3B simultaneously is tight (the pipeline's OOM cooldown handles collisions)." }
} else {
    Warn "No NVIDIA GPU detected — Layer 1 unavailable; lexicon (Layer 2) runs everything. Fully supported."
    $CpuOnly = $true
}
 $ramGB = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
Ok "RAM: $ramGB GB"
if ($ramGB -lt 8) { Warn "Under 8 GB RAM — ML stack will be uncomfortable." }
 $freeGB = [math]::Round((Get-PSDrive C).Free / 1GB, 1)
Ok "Free disk (C:): $freeGB GB (needs ~7 GB for deps + models)"
if ($freeGB -lt 10) { Warn "Low disk — models + wheels need roughly 7 GB." }
 $Summary["GPU"] = if ($gpuName) { "$gpuName, $([math]::Round($vramMB/1024,1))GB" } else { "none (CPU mode)" }

# ---------------------------------------------------------------- 3. VENV
Step 3 "Virtual environment"
if ($Reset -and (Test-Path ".venv")) {
    Remove-Item -Recurse -Force ".venv"
    Ok "Removed existing .venv (-Reset)"
}
if (Test-Path $VenvPy) {
    Ok ".venv already exists — reusing (add -Reset to rebuild)"
} else {
    Invoke-Expression "$pyCmd -m venv .venv"
    if (Test-Path $VenvPy) { Ok "Created .venv with Python $pyVer" }
    else { Fail "venv creation failed"; exit 1 }
}

# ---------------------------------------------------------------- 4. DEPENDENCIES
Step 4 "Dependencies (base + ML stack + torch)"
& $VenvPy -m pip install -r requirements.txt *> $null
if ($LASTEXITCODE -ne 0) { Fail "base requirements failed — run manually to see error"; exit 1 }
Ok "Base requirements installed"

& $VenvPy -m pip install chromadb sentence-transformers "transformers>=4.40" nltk *> $null
if ($LASTEXITCODE -eq 0) { Ok "ML core installed (chromadb, sentence-transformers, transformers)" }
else { Fail "ML core install failed — pipeline cannot run"; exit 1 }

& $VenvPy -m pip install umap-learn hdbscan *> $null
if ($LASTEXITCODE -eq 0) { Ok "UMAP + HDBSCAN installed (Step-5 discovery)" }
else { Warn "umap-learn/hdbscan failed — clustering falls back to one-cluster mode (non-fatal)" }

# --- torch: CPU vs CUDA, with the '+cpu masquerade' check ---
 $wantCuda = (-not $CpuOnly) -and $gpuName
if ($wantCuda) {
    $wantCuda = (AskDefault "Install CUDA torch (~2.5 GB download) to enable Layer 1?" "Y") -notmatch '^[Nn]'
}
 $torchVer = (& $VenvPy -c "import torch; print(torch.__version__)" 2>$null)
 $installed = ($LASTEXITCODE -eq 0)
if ($wantCuda) {
    $done = $false
    foreach ($idx in @("cu126","cu128")) {
        # --force-reinstall defeats the "already satisfied" trap when a +cpu wheel exists
        & $VenvPy -m pip install torch --index-url "https://download.pytorch.org/whl/$idx" --force-reinstall --no-deps *> $null
        if ($LASTEXITCODE -eq 0) { Ok "CUDA torch installed from $idx index"; $done = $true; break }
        Warn "No wheel on $idx index for this Python — trying next"
    }
    if (-not $done) { Warn "No CUDA wheel available (Python too new?) — falling back to CPU torch"; $wantCuda = $false }
}
if (-not $wantCuda -and -not $installed) {
    & $VenvPy -m pip install torch *> $null
    Ok "CPU torch installed"
}
# --- verdict: the actual truth, not pip's opinion ---
 $tout = (& $VenvPy -c "import torch; print(torch.cuda.is_available()); print(torch.__version__)" 2>$null) -join "`n"
if ($tout -match '^(True|False)\r?\n(.+)') {
    $cuda = ($Matches[1] -eq 'True'); $tver = $Matches[2].Trim()
    if ($cuda) { Ok "TORCH VERIFIED: $tver — CUDA active, Layer 1 enabled"; $Summary["Layer 1"] = "ENABLED ($tver)" }
    elseif ($gpuName -and $tver -like '*+cpu*') {
        # the exact trap we hit: GPU present, CPU wheel installed
        Warn "GPU present but torch is $tver (CPU build). Attempting one forced CUDA reinstall..."
        & $VenvPy -m pip install torch --index-url "https://download.pytorch.org/whl/cu126" --force-reinstall --no-deps *> $null
        $t2 = (& $VenvPy -c "import torch; print(torch.cuda.is_available())" 2>$null)
        if ("$t2" -match 'True') { Ok "Fixed — CUDA now active"; $Summary["Layer 1"] = "ENABLED (after fix)" }
        else { Warn "Still CPU — manual fix needed (see README Phase 2)"; $Summary["Layer 1"] = "CPU-only" }
    }
    else { Ok "TORCH: $tver (CPU) — Layer 2 lexicon handles all classification (supported mode)"; $Summary["Layer 1"] = "CPU-only" }
}

# ---------------------------------------------------------------- 5. OLLAMA
Step 5 "Ollama (Step-5 titles + briefings — optional)"
 $ollamaUp = $false; $hasModel = $false
try {
    $r = Invoke-RestMethod "http://127.0.0.1:11434/api/tags" -TimeoutSec 3
    $ollamaUp = $true
    $hasModel = ($r.models.name -contains "qwen2.5:3b-instruct")
} catch {}
if ($ollamaUp -and $hasModel) { Ok "Ollama up, qwen2.5:3b-instruct present" }
elseif ($ollamaUp) {
    Ok "Ollama daemon up"
    if ((AskDefault "Pull qwen2.5:3b-instruct now (~2 GB)?" "Y") -notmatch '^[Nn]') {
        & ollama pull qwen2.5:3b-instruct
        $hasModel = ($LASTEXITCODE -eq 0)
    }
}
elseif (Get-Command ollama -ErrorAction SilentlyContinue) {
    Warn "Ollama installed but daemon not responding — start it (system tray), then re-run this script"
} else {
    Warn "Ollama not installed — download from https://ollama.com and re-run. Everything else works without it (keyword titles, no briefing)."
}
 $Summary["Ollama"] = if ($ollamaUp -and $hasModel) { "UP + model" } elseif ($ollamaUp) { "UP (no model)" } else { "absent (optional)" }

# ---------------------------------------------------------------- 6. API KEYS
Step 6 "API keys (.env)"
function Mask($s) { if ($s) { $s.Substring(0, [math]::Min(6, $s.Length)) + "..." } else { "(empty)" } }
if (Test-Path ".env") {
    $env_ = Get-Content ".env" -Raw
    $tgId = if ($env_ -match 'TELEGRAM_API_ID=(\S+)') { $Matches[1] } else { "" }
    $tgH  = if ($env_ -match 'TELEGRAM_API_HASH=(\S+)') { $Matches[1] } else { "" }
    $xTok = if ($env_ -match 'X_BEARER_TOKEN=(\S+)') { $Matches[1] } else { "" }
    Ok ".env exists — Telegram: $(Mask $tgId)/$(Mask $tgH)  X: $(Mask $xTok)"
    if (-not $tgId -or -not $tgH) { Warn "Telegram keys incomplete — collector cannot start" }
} else {
    Write-Host "  Get keys: my.telegram.org -> API Development Tools (required)."
    Write-Host "            developer.x.com -> Bearer Token (optional; free tier is write-only)."
    $tgId = Read-Host "  ? TELEGRAM_API_ID"
    $tgH  = Read-Host "  ? TELEGRAM_API_HASH"
    $xTok = Read-Host "  ? X_BEARER_TOKEN (blank = Telegram-only)"
    "TELEGRAM_API_ID=$tgId", "TELEGRAM_API_HASH=$tgH", "X_BEARER_TOKEN=$xTok" |
        Set-Content ".env" -Encoding ascii
    Ok ".env written (never commit it — .gitignore already excludes it)"
}
 $Summary["Keys"] = if ($tgId) { "Telegram set" } else { "MISSING" }
if ($xTok) { $Summary["Keys"] += " + X" }

# ---------------------------------------------------------------- 7. CONFIG
Step 7 "config.yaml sanity"
if (-not (Test-Path "config.yaml")) { Fail "config.yaml missing — restore from repo"; exit 1 }
 $cfg = Get-Content "config.yaml" -Raw
 $pipe = if ($cfg -match 'pipeline:\s*(\S+)') { $Matches[1] } else { "(unset)" }
Ok "pipeline: $pipe"
if ($pipe -ne "five_step") { Warn "pipeline is '$pipe' — five_step enables the full 5-step plan" }
 $tgTargets = ([regex]::Matches($cfg, '(?m)^\s*-\s*"(\w+)"')).Count
if ($tgTargets -gt 0) { Ok "Telegram targets present" }
else { Warn "telegram.targets looks empty — edit config.yaml (public channels/groups you have joined)" }

# ---------------------------------------------------------------- 8. SMOKE TEST
Step 8 "Vault + ledger core (smoke test)"
 $smoke = & $VenvPy smoke_test.py 2>&1 | Out-String
if ($LASTEXITCODE -eq 0 -and $smoke -match 'OK') { Ok "Smoke test PASSED — vault, ledger, tamper-guard verified" }
else { Fail "Smoke test failed:"; Write-Host $smoke; exit 1 }

# ---------------------------------------------------------------- 9. TELEGRAM LOGIN
Step 9 "Telegram session (the one interactive step)"
if (Test-Path "collector.session") { Ok "collector.session exists — already logged in" }
else {
    if ((AskDefault "Run Telegram login now (phone + code prompt)?" "Y") -notmatch '^[Nn]') {
        & $VenvPy main.py --login
        if (Test-Path "collector.session") { Ok "Logged in — session saved, runs headless forever" }
        else { Warn "Login did not complete — run: python main.py --login" }
    } else { Warn "Skipped — run 'python main.py --login' before starting the collector" }
}

# ---------------------------------------------------------------- DONE
Write-Host ""
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host " SETUP COMPLETE" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
foreach ($k in $Summary.Keys) { Write-Host ("  {0,-10} {1}" -f $k, $Summary[$k]) }
Write-Host ""
Write-Host "  DAILY DRIVER:" -ForegroundColor White
Write-Host "   Terminal 1:  .venv\Scripts\Activate.ps1 ; python main.py"
Write-Host "   Terminal 2:  .venv\Scripts\Activate.ps1 ; uvicorn dashboard.server:app --port 8080"
Write-Host "   Browser:     http://localhost:8080"
Write-Host ""
Write-Host "  PROOF COMMANDS:  python check_engines.py   (engine attribution)"
Write-Host "                   python audit.py verify     (ledger integrity)"
Write-Host "================================================="
