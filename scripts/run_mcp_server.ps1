<#
.SYNOPSIS
    Launches the Agentic Delta Guard MCP Server for Gemini CLI / MCP clients.

.DESCRIPTION
    Runs the FastMCP server over standard I/O using the project's virtual environment.
#>

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $PythonExe)) {
    $PythonExe = "python"
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "🛡️  Starting Agentic Delta Guard MCP Server (Gemini CLI)" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Project Root : $ProjectRoot"
Write-Host "Python Exec  : $PythonExe"
Write-Host "Transport    : stdio (Model Context Protocol)"
Write-Host "============================================================" -ForegroundColor Cyan

$env:PYTHONPATH = $ProjectRoot

& $PythonExe "$ProjectRoot\run_mcp_server.py"
