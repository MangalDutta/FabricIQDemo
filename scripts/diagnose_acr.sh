#!/bin/bash
# Comprehensive ACR and App Service diagnostics

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========== ACR & APP SERVICE DIAGNOSTIC ==========${NC}"
echo ""

# Get inputs
RG="${1:-}"
ACR_NAME="${2:-}"

if [ -z "$RG" ] || [ -z "$ACR_NAME" ]; then
    echo -e "${RED}Usage: $0 <resource-group> <acr-name>${NC}"
    echo "Example: $0 fabric-customer360-rg acrcust360devh1234"
    exit 1
fi

echo -e "${YELLOW}[1] Checking ACR exists and admin status...${NC}"
ACR_INFO=$(az acr show --resource-group "$RG" --name "$ACR_NAME" 2>/dev/null || echo "")

if [ -z "$ACR_INFO" ]; then
    echo -e "${RED}❌ ACR '$ACR_NAME' not found in RG '$RG'${NC}"
    exit 1
fi

ADMIN_ENABLED=$(echo "$ACR_INFO" | jq -r '.adminUserEnabled')
echo -e "${GREEN}✓ ACR found: $ACR_NAME${NC}"
echo -e "  Admin Enabled: ${ADMIN_ENABLED}"

if [ "$ADMIN_ENABLED" != "true" ]; then
    echo -e "${RED}❌ PROBLEM: Admin user is DISABLED. Cannot retrieve credentials.${NC}"
    echo -e "${YELLOW}FIX: Run: az acr update --resource-group $RG --name $ACR_NAME --admin-enabled true${NC}"
else
    echo -e "${GREEN}✓ Admin user is ENABLED${NC}"
fi

echo ""
echo -e "${YELLOW}[2] Attempting to retrieve ACR credentials...${NC}"
ACR_CREDS=$(az acr credential show --resource-group "$RG" --name "$ACR_NAME" 2>&1 || echo "FAILED")

if echo "$ACR_CREDS" | grep -q "FAILED\|error\|Error"; then
    echo -e "${RED}❌ PROBLEM: Could not retrieve credentials${NC}"
    echo "Error details: $ACR_CREDS"
    exit 1
fi

ACR_USER=$(echo "$ACR_CREDS" | jq -r '.username')
ACR_PASS=$(echo "$ACR_CREDS" | jq -r '.passwords[0].value')
ACR_LOGIN=$(az acr show --resource-group "$RG" --name "$ACR_NAME" | jq -r '.loginServer')

echo -e "${GREEN}✓ Credentials retrieved${NC}"
echo "  Username: $ACR_USER"
echo "  Password (first 10 chars): ${ACR_PASS:0:10}***"
echo "  Login Server: $ACR_LOGIN"

if [ -z "$ACR_USER" ] || [ -z "$ACR_PASS" ]; then
    echo -e "${RED}❌ PROBLEM: Username or password is empty${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}[3] Listing images in ACR...${NC}"
IMAGES=$(az acr repository list --resource-group "$RG" --name "$ACR_NAME" --output json)
echo "Images in registry:"
echo "$IMAGES" | jq -r '.[]' | while read -r img; do
    TAGS=$(az acr repository show-tags --resource-group "$RG" --name "$ACR_NAME" --repository "$img" --output json)
    echo "  - $img: $(echo "$TAGS" | jq -r '.[]' | tr '\n' ',')"
done

echo ""
echo -e "${YELLOW}[4] Checking if frontend image exists...${NC}"
if echo "$IMAGES" | jq -r '.[]' | grep -q "fabric-customer360-frontend"; then
    echo -e "${GREEN}✓ Frontend image exists in ACR${NC}"
    FRONTEND_TAGS=$(az acr repository show-tags --resource-group "$RG" --name "$ACR_NAME" --repository "fabric-customer360-frontend" --output json)
    echo "  Tags: $(echo "$FRONTEND_TAGS" | jq -r '.[]' | tr '\n' ',')"
else
    echo -e "${RED}❌ PROBLEM: Frontend image NOT found in ACR${NC}"
    echo "  Available images: $(echo "$IMAGES" | jq -r '.[]' | tr '\n' ',')"
fi

echo ""
echo -e "${YELLOW}[5] Checking App Services...${NC}"
FE_APP=$(az webapp list -g "$RG" --query "[?contains(name,'frontend')].name" -o tsv)
BE_APP=$(az webapp list -g "$RG" --query "[?contains(name,'backend')].name" -o tsv)

echo "Frontend App: $FE_APP"
echo "Backend App: $BE_APP"

if [ -z "$FE_APP" ]; then
    echo -e "${RED}❌ Frontend App Service not found${NC}"
else
    echo -e "${YELLOW}[5a] Frontend App Service container config...${NC}"
    FE_CONFIG=$(az webapp config container show --resource-group "$RG" --name "$FE_APP" 2>/dev/null || echo "")

    if [ -z "$FE_CONFIG" ]; then
        echo -e "${YELLOW}⚠ No container config found (might be using default)${NC}"
    else
        echo "$FE_CONFIG" | jq '.'
    fi

    echo -e "${YELLOW}[5b] Frontend App Service app settings (docker-related)...${NC}"
    SETTINGS=$(az webapp config appsettings list -g "$RG" -n "$FE_APP" | jq '.[] | select(.name | contains("DOCKER") or contains("docker"))')
    if [ -z "$SETTINGS" ]; then
        echo -e "${YELLOW}⚠ No DOCKER settings found${NC}"
    else
        echo "$SETTINGS" | jq '.'
    fi
fi

echo ""
echo -e "${YELLOW}[6] Testing credentials manually with docker login...${NC}"
echo -n "Testing with docker: "
if docker login "$ACR_LOGIN" --username "$ACR_USER" --password "$ACR_PASS" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Docker login successful${NC}"

    echo -e "${YELLOW}[6a] Attempting to pull frontend image...${NC}"
    if docker pull "${ACR_LOGIN}/fabric-customer360-frontend:latest" 2>&1 | grep -q "Status: Downloaded\|already exists"; then
        echo -e "${GREEN}✓ Image pull successful${NC}"
    else
        echo -e "${RED}❌ Image pull failed${NC}"
    fi

    docker logout "$ACR_LOGIN" > /dev/null 2>&1
else
    echo -e "${RED}❌ Docker login failed with provided credentials${NC}"
    echo -e "${RED}PROBLEM: The credentials don't work. ACR admin user might not be properly enabled.${NC}"
fi

echo ""
echo -e "${BLUE}========== DIAGNOSTIC COMPLETE ==========${NC}"
