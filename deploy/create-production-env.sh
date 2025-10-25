#!/bin/bash
# Helper script to create .env.production from your local .env file

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOCAL_ENV="$PROJECT_ROOT/.env"
PRODUCTION_ENV="$SCRIPT_DIR/.env.production"
TEMPLATE_ENV="$SCRIPT_DIR/.env.production.template"

echo "Creating production environment file..."

if [ ! -f "$LOCAL_ENV" ]; then
    echo "Error: Local .env file not found at $LOCAL_ENV"
    exit 1
fi

# Copy template
cp "$TEMPLATE_ENV" "$PRODUCTION_ENV"

# Function to update or add environment variable
update_env_var() {
    local key=$1
    local value=$2
    local file=$3

    if grep -q "^${key}=" "$file"; then
        # macOS compatible sed
        sed -i.bak "s|^${key}=.*|${key}=${value}|" "$file" && rm "${file}.bak"
    else
        echo "${key}=${value}" >> "$file"
    fi
}

# Copy API keys from local .env
echo "Copying API keys from local environment..."

while IFS='=' read -r key value; do
    # Skip comments and empty lines
    [[ "$key" =~ ^#.*$ ]] && continue
    [[ -z "$key" ]] && continue

    # Only copy specific keys, not local paths
    case "$key" in
        FRED_API_KEY|BLS_API_KEY|BEA_API_KEY|CENSUS_API_KEY|FBI_API_KEY|NHTSA_API_KEY|FEMA_API_KEY|OPENALEX_API_KEY|HUD_TOKEN|HUD_USERNAME|HUD_PASSWORD|ALPHA_VANTAGE_KEY)
            # Remove any quotes and whitespace
            value=$(echo "$value" | sed -e 's/^"//' -e 's/"$//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
            if [ ! -z "$value" ] && [ "$value" != "your-*-api-key-here" ]; then
                update_env_var "$key" "$value" "$PRODUCTION_ENV"
                echo "  ✓ Copied $key"
            fi
            ;;
    esac
done < "$LOCAL_ENV"

echo ""
echo "Production environment file created at: $PRODUCTION_ENV"
echo ""
echo "⚠️  IMPORTANT: You still need to:"
echo "  1. Generate secure API_KEYS (currently set to CHANGE_ME_*)"
echo "  2. Generate secure JWT_SECRET_KEY (currently set to CHANGE_ME_*)"
echo "  3. Review and update MinIO credentials if needed"
echo ""
echo "To generate secure keys, run:"
echo "  openssl rand -hex 32  # For API keys"
echo "  openssl rand -hex 64  # For JWT secret"
echo ""
echo "Then edit $PRODUCTION_ENV and replace the CHANGE_ME_* placeholders"
