variable "resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "eastus"
}

variable "prefix" {
  description = "Prefix for resource names"
  type        = string
  default     = "govdata-mcp"
}

variable "vm_size" {
  description = "Size of the virtual machine"
  type        = string
  default     = "Standard_D4s_v3"
}

variable "data_disk_size_gb" {
  description = "Size of the data disk in GB for MinIO storage"
  type        = number
  default     = 2048
}

variable "admin_username" {
  description = "Admin username for the VM"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key_path" {
  description = "Path to SSH public key file"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "admin_source_ip" {
  description = "Source IP address allowed for SSH access (use 'your.ip.address/32' or '*' for any)"
  type        = string
  default     = "*"
}

variable "domain" {
  description = "Domain name for the MCP server"
  type        = string
}

variable "dns_label" {
  description = "DNS label for the public IP (creates <label>.eastus.cloudapp.azure.com)"
  type        = string
}

variable "admin_email" {
  description = "Email address for Let's Encrypt certificate notifications"
  type        = string
}

variable "minio_root_user" {
  description = "MinIO root username"
  type        = string
  default     = "minioadmin"
}

variable "minio_root_password" {
  description = "MinIO root password"
  type        = string
  sensitive   = true
  default     = "minioadmin"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Environment = "production"
    Project     = "govdata-mcp-server"
    ManagedBy   = "terraform"
  }
}
