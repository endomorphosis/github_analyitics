$ErrorActionPreference = 'Stop'

# Bootstrap installer for this repo.
# - Installs required CLI tools (best-effort via winget/choco)
# - Creates/updates a local .venv and installs Python dependencies

function Has-Command($name) {
  return [bool](Get-Command $name -ErrorAction SilentlyContinue)
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

& "$scriptDir\scripts\install_cli_tools.ps1"

if (-not (Has-Command 'python')) {
  Write-Host "Python is not installed or not in PATH. Install Python 3.10+ first."
  exit 1
}

$venvDir = Join-Path $scriptDir '.venv'
$venvPy = Join-Path $venvDir 'Scripts\python.exe'

if (-not (Test-Path $venvPy)) {
  Write-Host "Creating virtual environment at $venvDir"
  python -m venv $venvDir
}

if (-not (Test-Path $venvPy)) {
  Write-Host "Failed to create venv at $venvDir"
  exit 1
}

& $venvPy -m pip install --upgrade pip
& $venvPy -m pip install -r (Join-Path $scriptDir 'requirements.txt')

Write-Host "Done. Next: gh auth login; then run python -m github_analyitics.reporting.github_analytics"
