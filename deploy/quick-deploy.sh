#!/bin/bash
# Quick deployment script for GovData MCP Server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${BLUE}==> $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_step "Checking prerequisites..."

    if ! command -v terraform &> /dev/null; then
        print_error "Terraform not found. Please install Terraform first."
        exit 1
    fi

    if ! command -v az &> /dev/null; then
        print_error "Azure CLI not found. Please install Azure CLI first."
        exit 1
    fi

    if ! az account show &> /dev/null; then
        print_error "Not logged in to Azure. Run 'az login' first."
        exit 1
    fi

    if [ ! -f "$HOME/.ssh/id_rsa.pub" ]; then
        print_warning "SSH public key not found at ~/.ssh/id_rsa.pub"
        echo "Generate one with: ssh-keygen -t rsa -b 4096"
        exit 1
    fi

    print_success "All prerequisites met"
}

# Create production environment file
create_env_file() {
    print_step "Creating production environment file..."

    if [ ! -f "$SCRIPT_DIR/.env.production" ]; then
        cd "$SCRIPT_DIR"
        ./create-production-env.sh
        print_warning "Please edit deploy/.env.production and set secure keys before continuing"
        echo "Press Enter when ready to continue..."
        read
    else
        print_success "Production environment file already exists"
    fi
}

# Deploy infrastructure
deploy_infrastructure() {
    print_step "Deploying Azure infrastructure..."

    cd "$PROJECT_ROOT/terraform"

    if [ ! -d ".terraform" ]; then
        terraform init
    fi

    terraform plan -out=tfplan

    echo ""
    print_warning "Review the plan above. Press Enter to apply or Ctrl+C to cancel..."
    read

    terraform apply tfplan
    rm tfplan

    print_success "Infrastructure deployed"

    # Get outputs
    export VM_IP=$(terraform output -raw public_ip_address)
    export VM_FQDN=$(terraform output -raw fqdn)
    export DOMAIN=$(terraform output -raw domain)
    export ADMIN_EMAIL=$(terraform output -raw admin_email)

    echo ""
    print_success "VM IP Address: $VM_IP"
    print_success "VM FQDN: $VM_FQDN"
    print_success "Domain: $DOMAIN"
    echo ""
}

# Create deployment package
create_package() {
    print_step "Creating deployment package..."

    cd "$PROJECT_ROOT"

    tar -czf govdata-mcp-deploy.tar.gz \
        --exclude='.git' \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        --exclude='.env' \
        --exclude='venv' \
        --exclude='terraform/.terraform' \
        --exclude='*.tar.gz' \
        src/ deploy/ govdata-model.json requirements.txt pyproject.toml

    print_success "Deployment package created: govdata-mcp-deploy.tar.gz"
}

# Upload files
upload_files() {
    print_step "Uploading files to VM..."

    if [ -z "$VM_IP" ]; then
        cd "$PROJECT_ROOT/terraform"
        export VM_IP=$(terraform output -raw public_ip_address)
    fi

    echo "Uploading application package..."
    scp "$PROJECT_ROOT/govdata-mcp-deploy.tar.gz" "azureuser@$VM_IP:/tmp/"

    # Check for Calcite JAR
    if [ -n "$CALCITE_JAR_PATH" ] && [ -f "$CALCITE_JAR_PATH" ]; then
        echo "Uploading Calcite JAR..."
        scp "$CALCITE_JAR_PATH" "azureuser@$VM_IP:/tmp/calcite-govdata.jar"
        print_success "Files uploaded"
    else
        print_warning "CALCITE_JAR_PATH not set or file not found"
        print_warning "You'll need to manually upload the Calcite JAR file"
        echo "Set CALCITE_JAR_PATH=/path/to/jar and re-run, or upload manually:"
        echo "  scp /path/to/calcite.jar azureuser@$VM_IP:/tmp/calcite-govdata.jar"
    fi
}

# Setup on VM
setup_vm() {
    print_step "Setting up application on VM..."

    if [ -z "$VM_IP" ]; then
        cd "$PROJECT_ROOT/terraform"
        export VM_IP=$(terraform output -raw public_ip_address)
    fi

    ssh "azureuser@$VM_IP" << 'ENDSSH'
sudo -i
cd /opt/govdata-mcp
tar -xzf /tmp/govdata-mcp-deploy.tar.gz
if [ -f /tmp/calcite-govdata.jar ]; then
    mv /tmp/calcite-govdata.jar /opt/calcite/calcite-govdata.jar
fi
chown -R root:root /opt/govdata-mcp
ENDSSH

    print_success "Application setup complete"
}

# Obtain SSL certificate
setup_ssl() {
    print_step "Obtaining SSL certificate..."

    if [ -z "$VM_IP" ] || [ -z "$DOMAIN" ] || [ -z "$ADMIN_EMAIL" ]; then
        cd "$PROJECT_ROOT/terraform"
        export VM_IP=$(terraform output -raw public_ip_address)
        export DOMAIN=$(terraform output -raw domain)
        export ADMIN_EMAIL=$(terraform output -raw admin_email)
    fi

    echo ""
    print_warning "Make sure DNS is configured before continuing!"
    echo "Add an A record for $DOMAIN pointing to $VM_IP"
    echo "Press Enter when DNS is configured and propagated..."
    read

    ssh "azureuser@$VM_IP" << ENDSSH
sudo certbot certonly --standalone \
    -d $DOMAIN \
    --email $ADMIN_EMAIL \
    --agree-tos \
    --non-interactive \
    --pre-hook "docker stop govdata-nginx 2>/dev/null || true" \
    --post-hook "docker start govdata-nginx 2>/dev/null || true"
ENDSSH

    print_success "SSL certificate obtained"
}

# Start services
start_services() {
    print_step "Starting services..."

    if [ -z "$VM_IP" ]; then
        cd "$PROJECT_ROOT/terraform"
        export VM_IP=$(terraform output -raw public_ip_address)
    fi

    ssh "azureuser@$VM_IP" << 'ENDSSH'
cd /opt/govdata-mcp/deploy
sudo docker compose up -d
sudo docker compose ps
ENDSSH

    print_success "Services started"
}

# Verify deployment
verify_deployment() {
    print_step "Verifying deployment..."

    if [ -z "$DOMAIN" ]; then
        cd "$PROJECT_ROOT/terraform"
        export DOMAIN=$(terraform output -raw domain)
    fi

    sleep 10

    if curl -f -s https://$DOMAIN/health > /dev/null; then
        print_success "MCP Server is responding!"
    else
        print_warning "Health check failed. Check logs with:"
        echo "  ssh azureuser@$VM_IP 'cd /opt/govdata-mcp/deploy && sudo docker compose logs'"
    fi
}

# Main execution
main() {
    echo -e "${BLUE}"
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║        GovData MCP Server - Quick Deploy Script          ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"

    check_prerequisites
    create_env_file
    deploy_infrastructure
    create_package
    upload_files
    setup_vm
    setup_ssl
    start_services
    verify_deployment

    echo ""
    echo -e "${GREEN}"
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║                Deployment Complete!                       ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    echo ""
    echo "Your MCP server is available at: https://$DOMAIN"
    echo "VM IP: $VM_IP"
    echo ""
    echo "Useful commands:"
    echo "  SSH to VM:    ssh azureuser@$VM_IP"
    echo "  View logs:    ssh azureuser@$VM_IP 'cd /opt/govdata-mcp/deploy && sudo docker compose logs -f'"
    echo "  Restart:      ssh azureuser@$VM_IP 'cd /opt/govdata-mcp/deploy && sudo docker compose restart'"
    echo ""
}

# Handle script arguments
case "${1:-}" in
    --check-only)
        check_prerequisites
        ;;
    --infra-only)
        check_prerequisites
        deploy_infrastructure
        ;;
    --upload-only)
        create_package
        upload_files
        setup_vm
        ;;
    --help)
        echo "Usage: $0 [OPTION]"
        echo ""
        echo "Options:"
        echo "  (none)          Run full deployment"
        echo "  --check-only    Only check prerequisites"
        echo "  --infra-only    Only deploy infrastructure"
        echo "  --upload-only   Only upload and setup application"
        echo "  --help          Show this help"
        ;;
    *)
        main
        ;;
esac
