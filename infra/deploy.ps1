<#
    Provision the AWS backend and publish the Lambda code.

    Only the AWS CLI is required. No SAM, no CDK, no Docker.

    Prerequisites:
      * AWS CLI v2      https://awscli.amazonaws.com/AWSCLIV2.msi
      * Credentials configured:  aws configure
        (AWS Academy: paste the keys from "AWS Details" in the lab page into
         %USERPROFILE%\.aws\credentials, including aws_session_token)

    Usage, from the project root:
        .\infra\deploy.ps1
        .\infra\deploy.ps1 -StackName fogedge -Region us-east-1
        .\infra\deploy.ps1 -LabRoleArn arn:aws:iam::123456789012:role/LabRole
#>

param(
    [string]$StackName  = "fogedge",
    [string]$Region     = "us-east-1",
    [string]$ApiKey     = "",
    [string]$LabRoleArn = ""      # AWS Academy only
)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    $ApiKey = -join ((1..32) | ForEach-Object { "0123456789abcdef"[(Get-Random -Maximum 16)] })
}

Write-Host "==> checking the AWS CLI and your credentials" -ForegroundColor Cyan
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    throw "The AWS CLI is not on PATH. Install it from https://awscli.amazonaws.com/AWSCLIV2.msi"
}
$identity = aws sts get-caller-identity --output json 2>$null | ConvertFrom-Json
if (-not $identity) { throw "No valid AWS credentials. Run 'aws configure' first." }
Write-Host "    account $($identity.Account) as $($identity.Arn)"

# --------------------------------------------------------------- packaging
# Every handler and the dashboard page go in one zip. boto3 ships with the
# Lambda runtime, so there are no dependencies to vendor.
$bucket  = "$StackName-code-$($identity.Account)-$Region"
$zipPath = Join-Path $env:TEMP "$StackName-lambda.zip"

Write-Host "==> packaging the Lambda code" -ForegroundColor Cyan
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path (Join-Path $root "backend\aws\*") -DestinationPath $zipPath
Write-Host "    $([math]::Round((Get-Item $zipPath).Length / 1KB, 1)) KB"

Write-Host "==> ensuring the code bucket $bucket exists" -ForegroundColor Cyan
$exists = aws s3api head-bucket --bucket $bucket --region $Region 2>&1
if ($LASTEXITCODE -ne 0) {
    if ($Region -eq "us-east-1") {
        aws s3api create-bucket --bucket $bucket --region $Region | Out-Null
    } else {
        aws s3api create-bucket --bucket $bucket --region $Region `
            --create-bucket-configuration LocationConstraint=$Region | Out-Null
    }
}
# A unique key per deploy, otherwise CloudFormation sees no change to the
# code and silently keeps the old version of your functions.
$codeKey = "lambda-$(Get-Date -Format yyyyMMddHHmmss).zip"
aws s3 cp $zipPath "s3://$bucket/$codeKey" --region $Region | Out-Null

# ------------------------------------------------------------ deployment
Write-Host "==> deploying the CloudFormation stack (3 to 5 minutes)" -ForegroundColor Cyan
$params = @("CodeBucket=$bucket", "CodeKey=$codeKey", "ApiKey=$ApiKey")
if ($LabRoleArn) { $params += "ExistingRoleArn=$LabRoleArn" }

aws cloudformation deploy `
    --template-file (Join-Path $PSScriptRoot "template.yaml") `
    --stack-name $StackName `
    --region $Region `
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM `
    --parameter-overrides $params
if ($LASTEXITCODE -ne 0) { throw "CloudFormation deployment failed. See the output above." }

$outputs = aws cloudformation describe-stacks --stack-name $StackName --region $Region `
    --query "Stacks[0].Outputs" --output json | ConvertFrom-Json
$ingest    = ($outputs | Where-Object { $_.OutputKey -eq "IngestUrl" }).OutputValue
$dashboard = ($outputs | Where-Object { $_.OutputKey -eq "DashboardUrl" }).OutputValue

Write-Host ""
Write-Host "Deployment complete." -ForegroundColor Green
Write-Host "  ingest endpoint : $ingest"
Write-Host "  dashboard       : $dashboard"
Write-Host "  api key         : $ApiKey"
Write-Host ""
Write-Host "Point the fog node at the cloud:" -ForegroundColor Yellow
Write-Host "  `$env:FOG_INGEST_URL = `"$ingest`""
Write-Host "  `$env:FOG_API_KEY    = `"$ApiKey`""
Write-Host "  python -m fog.node        # in one terminal"
Write-Host "  python -m sensors.runner  # in another"
Write-Host ""
Write-Host "Screenshot $dashboard for your report and demo." -ForegroundColor Yellow
