#!/usr/bin/env bash
# Provision the AWS backend and publish the Lambda code. AWS CLI only.
#
#   ./infra/deploy.sh
#   STACK=fogedge REGION=us-east-1 ./infra/deploy.sh
#   LAB_ROLE_ARN=arn:aws:iam::123456789012:role/LabRole ./infra/deploy.sh
set -euo pipefail

STACK="${STACK:-fogedge}"
REGION="${REGION:-us-east-1}"
LAB_ROLE_ARN="${LAB_ROLE_ARN:-}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

command -v aws >/dev/null || { echo "AWS CLI not found"; exit 1; }
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "==> account $ACCOUNT, region $REGION"

# Reuse the key already deployed, if any. Minting a fresh one on every deploy
# silently invalidates the key the fog nodes are holding, which shows up as a
# flood of 401s rather than as anything obviously key-related.
if [ -z "${API_KEY:-}" ]; then
  API_KEY=$(aws lambda get-function-configuration --function-name "$STACK-ingest" \
              --region "$REGION" --query "Environment.Variables.FOG_API_KEY" \
              --output text 2>/dev/null || true)
fi
if [ -z "$API_KEY" ] || [ "$API_KEY" = "None" ]; then
  API_KEY=$(openssl rand -hex 16)
  echo "==> generated a new API key"
else
  echo "==> reusing the API key already deployed"
fi

BUCKET="$STACK-code-$ACCOUNT-$REGION"
ZIP="$(mktemp -d)/lambda.zip"

echo "==> packaging the Lambda code"
(cd "$ROOT/backend/aws" && zip -qr "$ZIP" .)

echo "==> ensuring the code bucket $BUCKET exists"
if ! aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
  fi
fi
# Unique key per deploy so CloudFormation actually updates the function code.
CODE_KEY="lambda-$(date +%Y%m%d%H%M%S).zip"
aws s3 cp "$ZIP" "s3://$BUCKET/$CODE_KEY" --region "$REGION" >/dev/null

echo "==> deploying the CloudFormation stack (3 to 5 minutes)"
PARAMS=("CodeBucket=$BUCKET" "CodeKey=$CODE_KEY" "ApiKey=$API_KEY")
[ -n "$LAB_ROLE_ARN" ] && PARAMS+=("ExistingRoleArn=$LAB_ROLE_ARN")

aws cloudformation deploy \
  --template-file "$ROOT/infra/template.yaml" \
  --stack-name "$STACK" --region "$REGION" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides "${PARAMS[@]}"

OUT=$(aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
        --query "Stacks[0].Outputs" --output json)
INGEST=$(echo "$OUT" | python3 -c "import sys,json;print([o['OutputValue'] for o in json.load(sys.stdin) if o['OutputKey']=='IngestUrl'][0])")
DASH=$(echo "$OUT"   | python3 -c "import sys,json;print([o['OutputValue'] for o in json.load(sys.stdin) if o['OutputKey']=='DashboardUrl'][0])")

echo
echo "  ingest endpoint : $INGEST"
echo "  dashboard       : $DASH"
echo "  api key         : $API_KEY"
echo
echo "Point the fog node at the cloud with:"
echo "  export FOG_INGEST_URL=$INGEST FOG_API_KEY=$API_KEY"
echo "  python -m fog.node        # in one terminal"
echo "  python -m sensors.runner  # in another"
