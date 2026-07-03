output "cluster_endpoint" {
  description = "Writer endpoint for the Aurora cluster"
  value       = aws_rds_cluster.this.endpoint
}

output "reader_endpoint" {
  description = "Reader endpoint for the Aurora cluster"
  value       = aws_rds_cluster.this.reader_endpoint
}

output "port" {
  description = "Port Aurora listens on"
  value       = var.port
}

output "database_name" {
  description = "Primary database name"
  value       = var.database_name
}

output "master_username" {
  description = "Master username"
  value       = var.master_username
}

output "master_password" {
  description = "Master password (generated if not provided)"
  value       = local.master_password
  sensitive   = true
}

output "security_group_id" {
  description = "Security group protecting Aurora"
  value       = aws_security_group.this.id
}

output "secret_arn" {
  description = "Secrets Manager secret ARN containing connection info"
  value       = aws_secretsmanager_secret.aurora.arn
}
