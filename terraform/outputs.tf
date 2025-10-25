output "public_ip_address" {
  description = "Public IP address of the VM"
  value       = azurerm_public_ip.main.ip_address
}

output "fqdn" {
  description = "Fully qualified domain name"
  value       = azurerm_public_ip.main.fqdn
}

output "ssh_command" {
  description = "SSH command to connect to the VM"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.main.ip_address}"
}

output "dns_configuration" {
  description = "DNS configuration instructions"
  value       = <<-EOT
    Add the following A record to your DNS provider for ${var.domain}:

    Type: A
    Name: @ (or leave blank for root domain)
    Value: ${azurerm_public_ip.main.ip_address}
    TTL: 300 (or your provider's default)

    After DNS propagation, your MCP server will be available at:
    https://${var.domain}
  EOT
}

output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.main.name
}

output "vm_id" {
  description = "ID of the virtual machine"
  value       = azurerm_linux_virtual_machine.main.id
}

output "domain" {
  description = "Domain name for the MCP server"
  value       = var.domain
}

output "admin_email" {
  description = "Admin email for SSL certificates"
  value       = var.admin_email
}
