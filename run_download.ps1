# Ensure we’re in the repo
$Repo = "C:\dibbs-automation"
Set-Location $Repo

# Where is python? (adjust if you installed elsewhere)
$Python = (Get-Command python.exe).Source

# Make sure folders exist
New-Item -ItemType Directory -Force -Path "$Repo\logs","$Repo\staging","$Repo\output" | Out-Null

# Log file (yyMMdd_HHmm)
$ts  = Get-Date -Format "yyMMdd_HHmm"
$log = Join-Path "$Repo\logs" ("run_{0}.log" -f $ts)

# Run downloader (defaults to yesterday in America/Los_Angeles)
& $Python ".\download.py" 2>&1 | Tee-Object -FilePath $log

# Keep a constant “latest” pointer
Copy-Item $log "$Repo\logs\latest.log" -Force
