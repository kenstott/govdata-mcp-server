# Deployment Files

This directory contains all the files needed to deploy the GovData MCP Server to Azure.

## Files

- **Dockerfile**: Multi-stage Docker build for the MCP server
- **docker-compose.yml**: Orchestrates MCP server, MinIO, nginx, and certbot services
- **nginx.conf**: Nginx reverse proxy configuration with SSL and rate limiting
- **cloud-init.sh**: VM initialization script (used by Terraform)
- **.env.production.template**: Template for production environment variables
- **create-production-env.sh**: Helper script to generate production env file

## Quick Start

1. **Create production environment file:**
   ```bash
   ./create-production-env.sh
   # Edit .env.production to set secure keys
   ```

2. **Deploy infrastructure:**
   ```bash
   cd ../terraform
   terraform init
   terraform apply
   ```

3. **Upload application:**
   ```bash
   # Create deployment package
   tar -czf govdata-mcp-deploy.tar.gz \
     --exclude='.git' --exclude='__pycache__' \
     ../src/ ../deploy/ ../govdata-model.json ../requirements.txt ../pyproject.toml

   # Upload to VM (replace with your VM IP)
   scp govdata-mcp-deploy.tar.gz azureuser@<VM-IP>:/tmp/
   scp /path/to/calcite-govdata.jar azureuser@<VM-IP>:/tmp/
   ```

4. **Configure on VM:**
   ```bash
   ssh azureuser@<VM-IP>
   sudo -i
   cd /opt/govdata-mcp
   tar -xzf /tmp/govdata-mcp-deploy.tar.gz
   mv /tmp/calcite-govdata.jar /opt/calcite/
   ```

5. **Obtain SSL certificate:**
   ```bash
   certbot certonly --standalone \
     -d your-domain.com \
     --email your-email@example.com \
     --agree-tos --non-interactive
   ```

6. **Start services:**
   ```bash
   cd /opt/govdata-mcp/deploy
   docker compose up -d
   ```

## Service Ports

- **80**: HTTP (redirects to HTTPS)
- **443**: HTTPS (public access)
- **8080**: MCP Server (internal only)
- **9000**: MinIO S3 API (internal only)
- **9001**: MinIO Console (internal only)

## Useful Commands

```bash
# View logs
docker compose logs -f

# Check status
docker compose ps

# Restart a service
docker compose restart mcp-server

# Update configuration
docker compose down
# ... make changes ...
docker compose up -d

# View MinIO console (via SSH tunnel)
ssh -L 9001:localhost:9001 azureuser@<VM-IP>
# Open http://localhost:9001 in browser
```

## Environment Variables

Key variables in `.env.production`:

- `API_KEYS`: Comma-separated API keys for authentication
- `JWT_SECRET_KEY`: Secret for JWT token signing
- `MINIO_ROOT_USER/PASSWORD`: MinIO credentials
- Government API keys: FRED, BLS, BEA, CENSUS, etc.

## Security Notes

1. Never commit `.env.production` to git
2. Use strong, randomly generated API keys
3. Change default MinIO credentials
4. Restrict SSH access by IP in Terraform config
5. Keep Docker images and system packages updated

## Troubleshooting

**Services won't start:**
```bash
docker compose logs
```

**SSL issues:**
```bash
certbot renew --dry-run
cat /var/log/letsencrypt/letsencrypt.log
```

**Disk full:**
```bash
docker system prune -a
du -sh /mnt/data/*
```

For more details, see [DEPLOYMENT.md](../DEPLOYMENT.md)
