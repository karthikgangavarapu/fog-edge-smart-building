#!/usr/bin/env bash
# One-shot provision + deploy. Requires: az CLI, Azure Functions Core Tools v4.
set -euo pipefail

RG="${RG:-rg-fogedge}"
LOCATION="${LOCATION:-northeurope}"
PREFIX="${PREFIX:-fogedge}"
API_KEY="${API_KEY:-$(openssl rand -hex 16)}"

echo "==> resource group $RG in $LOCATION"
az group create -n "$RG" -l "$LOCATION" -o none

echo "==> provisioning infrastructure (bicep)"
OUT=$(az deployment group create -g "$RG" -f infra/main.bicep \
        -p namePrefix="$PREFIX" apiKey="$API_KEY" --query properties.outputs -o json)

APP=$(echo "$OUT"  | python3 -c "import sys,json;print(json.load(sys.stdin)['functionAppName']['value'])")
ING=$(echo "$OUT"  | python3 -c "import sys,json;print(json.load(sys.stdin)['ingestUrl']['value'])")
DASH=$(echo "$OUT" | python3 -c "import sys,json;print(json.load(sys.stdin)['dashboardUrl']['value'])")

echo "==> publishing function app $APP"
(cd backend/azure_functions && func azure functionapp publish "$APP" --python)

echo
echo "  ingest endpoint : $ING"
echo "  dashboard       : $DASH"
echo "  api key         : $API_KEY"
echo
echo "Point the fog node at the cloud with:"
echo "  export FOG_INGEST_URL=$ING FOG_API_KEY=$API_KEY && python -m fog.node"
