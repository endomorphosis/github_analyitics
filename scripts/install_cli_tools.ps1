$ErrorActionPreference = 'Stop'

function Has-Command($name) {
  return [bool](Get-Command $name -ErrorAction SilentlyContinue)
}

$missing = @()
if (-not (Has-Command 'gh')) { $missing += 'gh' }
if (-not (Has-Command 'git')) { $missing += 'git' }

if ($missing.Count -eq 0) {
  Write-Host "All required CLI tools are already installed."
  gh --version | Select-Object -First 1
  git --version
  exit 0
}

Write-Host "Missing CLI tools: $($missing -join ', ')"

if (Has-Command 'winget') {
  if ($missing -contains 'gh') {
    Write-Host "Installing GitHub CLI via winget..."
    winget install --id GitHub.cli --silent --accept-package-agreements --accept-source-agreements
  }
  if ($missing -contains 'git') {
    Write-Host "Installing Git via winget..."
    winget install --id Git.Git --silent --accept-package-agreements --accept-source-agreements
  }
  exit 0
}

if (Has-Command 'choco') {
  if ($missing -contains 'gh') {
    Write-Host "Installing GitHub CLI via chocolatey..."
    choco install gh -y
  }
  if ($missing -contains 'git') {
    Write-Host "Installing Git via chocolatey..."
    choco install git -y
  }
  exit 0
}

Write-Host "No supported installer found (winget/choco). Please install manually:"
Write-Host "- GitHub CLI (gh): https://cli.github.com/"
Write-Host "- Git: https://git-scm.com/downloads"
exit 1
