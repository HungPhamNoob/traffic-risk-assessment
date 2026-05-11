# scripts/gcp/setup_gcp.ps1
# Run from the project root:
# powershell -ExecutionPolicy Bypass -File .\scripts\gcp\setup_gcp.ps1

param(
    [string]$ProjectId = "big-data-group-4",
    [string]$Region = "us-central1",
    [string]$Zone = "us-central1-a",
    [string]$MachineType = "e2-medium",
    [string]$DiskSize = "20GB",
    [string]$ServiceAccount = "team4-sa@$ProjectId.iam.gserviceaccount.com"
)

$ErrorActionPreference = "Stop"
Write-Host "Starting GCP setup for Node 1 control plane..." -ForegroundColor Cyan

# ==========================================
# 1. ENABLE APIS
# ==========================================
Write-Host "Enabling required APIs..." -ForegroundColor Yellow
$apis = @("compute.googleapis.com", "storage.googleapis.com", "artifactregistry.googleapis.com", "iap.googleapis.com")
foreach ($api in $apis) {
    gcloud services enable $api --project $ProjectId --quiet 2>$null
}
Write-Host "APIs enabled." -ForegroundColor Green

# ==========================================
# 2. SETUP FIREWALL & IAP
# ==========================================
Write-Host "Configuring firewall rules..." -ForegroundColor Yellow
$allowRules = @("tcp:5432", "tcp:8080", "tcp:9090", "tcp:3000", "tcp:5000", "icmp")
gcloud compute firewall-rules create capstone-internal-allow `
    --project $ProjectId `
    --allow ($allowRules -join ",") `
    --source-ranges "10.128.0.0/9" `
    --network "default" `
    --quiet 2>$null

gcloud compute firewall-rules create capstone-iap-allow `
    --project $ProjectId `
    --allow tcp:22 `
    --source-ranges "35.235.240.0/20" `
    --network "default" `
    --quiet 2>$null

# Grant IAP tunnel access to the current gcloud user.
$currentUser = gcloud config get-value account
gcloud projects add-iam-policy-binding $ProjectId `
    --member "user:$currentUser" `
    --role "roles/iap.tunnelResourceAccessor" `
    --quiet 2>$null
Write-Host "Firewall and IAP configured." -ForegroundColor Green

# ==========================================
# 3. CREATE STARTUP SCRIPT
# ==========================================
Write-Host "Using startup script directory..." -ForegroundColor Yellow
$startupDir = "scripts/gcp"
if (!(Test-Path $startupDir)) { New-Item -Path $startupDir -ItemType Directory -Force | Out-Null }

$startupPath = Join-Path $startupDir "startup-node1.sh"
@'
#!/bin/bash
set -e
echo "Node 1 startup script running..."
# Install Docker
if ! command -v docker &> /dev/null; then
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  usermod -aG docker $(whoami)
  rm get-docker.sh
fi
# Install Docker Compose v2
if ! command -v docker compose &> /dev/null; then
  DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
  mkdir -p $DOCKER_CONFIG/cli-plugins
  curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
  chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
fi
# Configure Docker for Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet
echo "Node 1 startup complete!"
'@ | Set-Content -Path $startupPath -Encoding UTF8
Write-Host "Startup script created at: $startupPath" -ForegroundColor Green

# ==========================================
# 4. ENSURE SERVICE ACCOUNT EXISTS
# ==========================================
Write-Host "Checking service account..." -ForegroundColor Yellow
$saExists = gcloud iam service-accounts describe $ServiceAccount --project $ProjectId 2>$null
if (!$saExists) {
    Write-Host "Service account not found. Creating $ServiceAccount..." -ForegroundColor Yellow
    gcloud iam service-accounts create team4-sa --display-name "Capstone Team 4 SA" --project $ProjectId --quiet
    gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$ServiceAccount" --role "roles/compute.instanceAdmin.v1" --quiet 2>$null
    gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$ServiceAccount" --role "roles/storage.objectAdmin" --quiet 2>$null
}
Write-Host "Service account ready." -ForegroundColor Green

# ==========================================
# 5. CREATE VM NODE 1
# ==========================================
Write-Host "Creating VM: node1-control..." -ForegroundColor Yellow
$vmExists = gcloud compute instances describe node1-control --zone=$Zone --project=$ProjectId 2>$null
if ($vmExists) {
    Write-Host "node1-control already exists. Skipping creation." -ForegroundColor Yellow
} else {
    gcloud compute instances create node1-control `
        --project $ProjectId `
        --zone $Zone `
        --machine-type $MachineType `
        --boot-disk-size $DiskSize `
        --boot-disk-type pd-balanced `
        --no-address `
        --service-account $ServiceAccount `
        --scopes cloud-platform,storage-rw `
        --metadata-from-file startup-script=$startupPath `
        --tags capstone-control `
        --quiet
    
    Write-Host "VM node1-control created successfully." -ForegroundColor Green
}

# ==========================================
# 6. VERIFY & NEXT STEPS
# ==========================================
Write-Host "`nVerifying VM status..." -ForegroundColor Cyan
gcloud compute instances describe node1-control `
    --zone $Zone `
    --project $ProjectId `
    --format 'table(name,status,machineType)'

Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "  1. Wait ~2-3 mins for startup script to finish installing Docker." -ForegroundColor White
Write-Host "  2. Test SSH via IAP:" -ForegroundColor White
Write-Host "     gcloud compute ssh node1-control --zone=$Zone --tunnel-through-iap" -ForegroundColor Yellow
Write-Host "  3. Clone repo & deploy:" -ForegroundColor White
Write-Host "     git clone <repo-url> && cd deployment/node1-control && docker-compose up -d" -ForegroundColor Yellow
