$ErrorActionPreference = 'Stop'

# Backward-compatible wrapper.
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$scriptDir\install_cli_tools.ps1"
