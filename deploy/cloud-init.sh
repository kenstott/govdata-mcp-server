#!/bin/bash
set -e

# Cloud-init script for MinIO MCP Server on Azure with FileBrowser
# This script runs on first boot to configure the VM

# Variables from Terraform
DOMAIN="${domain}"
ADMIN_EMAIL="${admin_email}"
MINIO_ROOT_USER="${minio_root_user}"
MINIO_ROOT_PASSWORD="${minio_root_password}"

# Constants
MINIO_DATA_DIR="/mnt/minio-data"
MINIO_USER="minio-user"
PUBLIC_FILES_DIR="/var/www/public-files"
LOG_FILE="/var/log/cloud-init-custom.log"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "Starting cloud-init configuration for MinIO MCP Server with FileBrowser"

# Update system
log "Updating system packages..."
apt-get update
apt-get upgrade -y

# Install required packages
log "Installing required packages..."
apt-get install -y \
    curl \
    wget \
    jq \
    certbot \
    python3-certbot-nginx \
    nginx \
    unzip \
    parted \
    xfsprogs

# Configure data disk (should be attached as /dev/sdc)
log "Configuring data disk..."
DATA_DISK="/dev/sdc"

if [ -b "$DATA_DISK" ]; then
    log "Data disk found at $DATA_DISK"

    # Check if disk is already formatted
    if ! blkid "$DATA_DISK"; then
        log "Formatting data disk with XFS..."
        parted -s "$DATA_DISK" mklabel gpt
        parted -s "$DATA_DISK" mkpart primary xfs 0% 100%
        mkfs.xfs -f "$${DATA_DISK}1"
    else
        log "Data disk already formatted"
    fi

    # Create mount point
    mkdir -p "$MINIO_DATA_DIR"

    # Get UUID of the partition
    DISK_UUID=$(blkid -s UUID -o value "$${DATA_DISK}1")

    # Add to fstab if not already present
    if ! grep -q "$DISK_UUID" /etc/fstab; then
        echo "UUID=$DISK_UUID $MINIO_DATA_DIR xfs defaults,nofail 0 2" >> /etc/fstab
        log "Added data disk to fstab"
    fi

    # Mount the disk
    mount -a
    log "Data disk mounted at $MINIO_DATA_DIR"
else
    log "WARNING: Data disk not found at $DATA_DISK"
    mkdir -p "$MINIO_DATA_DIR"
fi

# Create MinIO user
log "Creating MinIO user..."
if ! id "$MINIO_USER" &>/dev/null; then
    useradd -r -s /bin/false "$MINIO_USER"
    log "MinIO user created"
fi

# Set permissions on data directory
chown -R "$MINIO_USER:$MINIO_USER" "$MINIO_DATA_DIR"
chmod 750 "$MINIO_DATA_DIR"

# Install MinIO
log "Installing MinIO..."
wget https://dl.min.io/server/minio/release/linux-amd64/minio -O /usr/local/bin/minio
chmod +x /usr/local/bin/minio

# Create MinIO configuration directory
mkdir -p /etc/minio

# Create MinIO environment file
log "Configuring MinIO environment..."
cat > /etc/default/minio <<EOF
# MinIO configuration
MINIO_ROOT_USER="$MINIO_ROOT_USER"
MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD"
MINIO_VOLUMES="$MINIO_DATA_DIR"
MINIO_OPTS="--console-address :9001"
MINIO_SERVER_URL="http://localhost:9000"
MINIO_BROWSER_REDIRECT_URL="https://$DOMAIN/console/"
EOF

# Create MinIO systemd service
log "Creating MinIO systemd service..."
cat > /etc/systemd/system/minio.service <<'SERVICEEOF'
[Unit]
Description=MinIO Object Storage
Documentation=https://docs.min.io
Wants=network-online.target
After=network-online.target
AssertFileIsExecutable=/usr/local/bin/minio

[Service]
WorkingDirectory=/usr/local
User=minio-user
Group=minio-user
EnvironmentFile=/etc/default/minio
ExecStart=/usr/local/bin/minio server $MINIO_OPTS $MINIO_VOLUMES
Restart=always
LimitNOFILE=65536
TasksMax=infinity
TimeoutStopSec=infinity
SendSIGKILL=no

[Install]
WantedBy=multi-user.target
SERVICEEOF

# Install FileBrowser
log "Installing FileBrowser..."
curl -fsSL https://raw.githubusercontent.com/filebrowser/get/master/get.sh | bash

# Create directory for shared files
log "Creating public files directory..."
mkdir -p "$PUBLIC_FILES_DIR"
chown azureuser:azureuser "$PUBLIC_FILES_DIR"

# Create FileBrowser config
log "Configuring FileBrowser..."
cat > /etc/filebrowser.json <<'FBCONFIGEOF'
{
  "port": 8080,
  "baseURL": "/files",
  "address": "127.0.0.1",
  "log": "stdout",
  "database": "/etc/filebrowser.db",
  "root": "/var/www/public-files",
  "noauth": false,
  "signup": false
}
FBCONFIGEOF

# Create FileBrowser systemd service
log "Creating FileBrowser systemd service..."
cat > /etc/systemd/system/filebrowser.service <<'FBSERVICEEOF'
[Unit]
Description=File Browser
After=network.target

[Service]
ExecStart=/usr/local/bin/filebrowser -c /etc/filebrowser.json
Restart=always
User=azureuser
Group=azureuser

[Install]
WantedBy=multi-user.target
FBSERVICEEOF

# Create a welcome file in public files
cat > "$PUBLIC_FILES_DIR/README.txt" <<READMEEOF
Welcome to the Public Files Directory
======================================

This is a FileBrowser instance hosted at:
https://$DOMAIN/files/

You can upload, download, and manage files here.

Default credentials (CHANGE IMMEDIATELY):
Username: admin
Password: admin

MinIO Console is also available at:
https://$DOMAIN/console/

READMEEOF

# Configure Nginx as reverse proxy (HTTP only initially)
log "Configuring Nginx for HTTP..."
cat > /etc/nginx/sites-available/minio <<NGINXEOF
server {
    listen 80;
    server_name $DOMAIN;

    # For Let's Encrypt challenge
    location /.well-known/acme-challenge/ {
        root /var/www/html;
    }

    # Root - simple landing page
    location = / {
        return 200 '<!DOCTYPE html>
<html>
<head>
    <title>GRASP Intelligence Services</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            max-width: 800px;
            margin: 100px auto;
            padding: 20px;
            line-height: 1.6;
        }
        h1 { color: #333; }
        .service {
            background: #f5f5f5;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
        }
        a { color: #0066cc; text-decoration: none; }
        a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <h1>GRASP Intelligence Services</h1>
    <p>Welcome! This server hosts the following services:</p>

    <div class="service">
        <h3><a href="/mcp">MCP Server</a></h3>
        <p>Model Context Protocol server for AI applications</p>
    </div>

    <div class="service">
        <h3><a href="/files/">File Browser</a></h3>
        <p>Upload and download files</p>
    </div>

    <div class="service">
        <h3><a href="/console/">MinIO Console</a></h3>
        <p>Object storage management console</p>
    </div>
</body>
</html>';
        add_header Content-Type text/html;
    }

    # MCP Server (MinIO S3 API)
    location /mcp/ {
        rewrite ^/mcp/(.*) /\$1 break;
        proxy_pass http://localhost:9000;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";

        # Timeouts
        proxy_connect_timeout 300;
        proxy_send_timeout 300;
        proxy_read_timeout 300;
        send_timeout 300;

        # Allow large uploads
        client_max_body_size 1000M;
    }

    # MinIO Console
    location /console/ {
        proxy_pass http://localhost:9001/;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # MinIO Console static assets
    location ~ ^/(static|styles|images|Loader\.svg|manifest\.json) {
        proxy_pass http://localhost:9001;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # MinIO Console API endpoints
    location /api/ {
        proxy_pass http://localhost:9001;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # MinIO Console websocket
    location /ws/ {
        proxy_pass http://localhost:9001;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # MinIO Admin API
    location /minio/admin/ {
        proxy_pass http://localhost:9001;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # FileBrowser
    location /files/ {
        proxy_pass http://localhost:8080/files/;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";

        client_max_body_size 1000M;
    }
}
NGINXEOF

# Enable Nginx site
ln -sf /etc/nginx/sites-available/minio /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default

# Test Nginx configuration
log "Testing Nginx configuration..."
nginx -t

# Start and enable MinIO
log "Starting MinIO service..."
systemctl daemon-reload
systemctl enable minio
systemctl start minio

# Create FileBrowser database with proper permissions
log "Creating FileBrowser database..."
touch /etc/filebrowser.db
chown azureuser:azureuser /etc/filebrowser.db

# Start and enable FileBrowser
log "Starting FileBrowser service..."
systemctl enable filebrowser
systemctl start filebrowser

# Restart Nginx
systemctl restart nginx

# Wait for services to start
log "Waiting for services to start..."
sleep 10

# Check if services are running
if systemctl is-active --quiet minio; then
    log "MinIO is running successfully"
else
    log "ERROR: MinIO failed to start"
    journalctl -u minio -n 50 >> "$LOG_FILE"
fi

if systemctl is-active --quiet filebrowser; then
    log "FileBrowser is running successfully"
else
    log "ERROR: FileBrowser failed to start"
    journalctl -u filebrowser -n 50 >> "$LOG_FILE"
fi

# Set up firewall (ufw)
log "Configuring firewall..."
ufw --force enable
ufw allow 22/tcp   # SSH
ufw allow 80/tcp   # HTTP (for Let's Encrypt)
ufw allow 443/tcp  # HTTPS

# Obtain SSL certificate
log "Obtaining SSL certificate from Let's Encrypt..."
log "Waiting for DNS propagation..."
sleep 30

# Try to get certificate (with retry logic)
CERT_RETRY=0
MAX_CERT_RETRIES=5
until certbot --nginx -d "$DOMAIN" \
    --non-interactive \
    --agree-tos \
    --email "$ADMIN_EMAIL" \
    --redirect || [ $CERT_RETRY -eq $MAX_CERT_RETRIES ]; do
    CERT_RETRY=$((CERT_RETRY+1))
    log "Certificate attempt $CERT_RETRY failed, waiting 60 seconds before retry..."
    sleep 60
done

if [ $CERT_RETRY -eq $MAX_CERT_RETRIES ]; then
    log "WARNING: Failed to obtain SSL certificate after $MAX_CERT_RETRIES attempts"
    log "You can manually run: sudo certbot --nginx -d $DOMAIN"
else
    log "SSL certificate obtained successfully"

    # Set up automatic certificate renewal
    systemctl enable certbot.timer
    systemctl start certbot.timer
fi

# Install MinIO client (mc) for management
log "Installing MinIO client..."
wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc
chmod +x /usr/local/bin/mc

# Configure mc for local MinIO
/usr/local/bin/mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Create a default bucket for MCP data
log "Creating default bucket..."
/usr/local/bin/mc mb local/mcp-data || true

# Clean up
apt-get autoremove -y
apt-get clean

# Create status file
cat > /var/log/cloud-init-status.txt <<STATUSEOF
MinIO MCP Server Configuration Complete
=======================================
Date: $(date)
Domain: $DOMAIN
MinIO Data Directory: $MINIO_DATA_DIR
Public Files Directory: $PUBLIC_FILES_DIR

Service URLs:
- Landing Page: https://$DOMAIN/
- MCP Server: https://$DOMAIN/mcp/
- FileBrowser: https://$DOMAIN/files/
- MinIO Console: https://$DOMAIN/console/

Services Status:
- MinIO: $(systemctl is-active minio)
- FileBrowser: $(systemctl is-active filebrowser)
- Nginx: $(systemctl is-active nginx)
- Certbot: $(systemctl is-active certbot.timer)

SSL Certificate Status:
$(if [ -d "/etc/letsencrypt/live/$DOMAIN" ]; then echo "Certificate installed successfully"; else echo "Certificate pending - run: sudo certbot --nginx -d $DOMAIN"; fi)

Login Credentials:
===================
MinIO Console (https://$DOMAIN/console/):
  Username: $MINIO_ROOT_USER
  Password: [as configured]

FileBrowser (https://$DOMAIN/files/):
  Username: admin
  Password: admin
  ⚠️  CHANGE THIS PASSWORD IMMEDIATELY!

Next Steps:
1. Update DNS A record for $DOMAIN to point to this server's public IP
2. Visit https://$DOMAIN/ to see all available services
3. Access FileBrowser at https://$DOMAIN/files/ and change admin password
4. Access MinIO Console at https://$DOMAIN/console/
5. Configure your MCP client to use: https://$DOMAIN/mcp/

For logs, check:
- /var/log/cloud-init-output.log
- /var/log/cloud-init-custom.log
- journalctl -u minio
- journalctl -u filebrowser
STATUSEOF

log "Cloud-init configuration complete!"
log "Landing page at https://$DOMAIN"
log "MCP Server at https://$DOMAIN/mcp/"
log "MinIO Console at https://$DOMAIN/console/"
log "FileBrowser at https://$DOMAIN/files/"

log "Setup complete!"
