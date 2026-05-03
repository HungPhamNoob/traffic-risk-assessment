<#
.SYNOPSIS
    Setup GCP resources for Capstone Team 4 (Windows PowerShell)
.USAGE
    .\scripts\setup_gcp.ps1 -ProjectId "capstone-team4" -Region "us-central1"
#>

param(
    [string]$ProjectId = "capstone-team4",
    [string]$Region = "us-central1"
)

# ... (dán toàn bộ code các bước trên vào đây) ...

Write-Host "`n🎉 Setup complete!" -ForegroundColor Green
Write-Host "📁 Key location: $env:USERPROFILE\.gcp\capstone-sa-key.json" -ForegroundColor Cyan
Write-Host "🔐 Next: Add GOOGLE_APPLICATION_CREDENTIALS to your .env file" -ForegroundColor Yellow

# # Mở PowerShell, cd vào folder project
# cd C:\path\to\capstone-team4

# # Chạy script (có thể cần bypass execution policy lần đầu)
# Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
# .\scripts\setup_gcp.ps1 -ProjectId "capstone-team4"