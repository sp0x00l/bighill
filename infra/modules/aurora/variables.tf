variable "env_name" {
  description = "Environment name (used in naming)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for the Aurora security group"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for the DB subnet group"
  type        = list(string)
}

variable "engine_version" {
  description = "Aurora PostgreSQL engine version"
  type        = string
  default     = "17.5"
}

variable "database_name" {
  description = "Initial database name"
  type        = string
  default     = "bighill"
}

variable "master_username" {
  description = "Master username for the cluster"
  type        = string
  default     = "bighill_admin"
}

variable "master_password" {
  description = "Master password for the cluster (if null, one is generated)"
  type        = string
  default     = null
  sensitive   = true
}

variable "instance_class" {
  description = "Instance class for the Aurora instances"
  type        = string
  default     = "db.t4g.medium"
}

variable "instance_count" {
  description = "Number of Aurora instances to create"
  type        = number
  default     = 1
}

variable "backup_retention_period" {
  description = "Number of days to retain backups"
  type        = number
  default     = 7
}

variable "preferred_backup_window" {
  description = "Backup window (UTC)"
  type        = string
  default     = "04:00-05:00"
}

variable "preferred_maintenance_window" {
  description = "Maintenance window (UTC)"
  type        = string
  default     = "Sun:05:00-Sun:06:00"
}

variable "deletion_protection" {
  description = "Enable deletion protection"
  type        = bool
  default     = false
}

variable "skip_final_snapshot" {
  description = "Skip final snapshot on deletion (set false for prod)"
  type        = bool
  default     = true
}

variable "recovery_window_in_days" {
  description = "Secrets Manager recovery window (use 0 for ephemeral envs to force-delete)"
  type        = number
  default     = 7
}

variable "secret_name_override" {
  description = "Optional override for the Secrets Manager secret name; otherwise a unique name is generated"
  type        = string
  default     = ""
}

variable "apply_immediately" {
  description = "Apply modifications immediately (true for dev/test)"
  type        = bool
  default     = true
}

variable "port" {
  description = "Database port"
  type        = number
  default     = 5432
}

variable "allowed_security_group_ids" {
  description = "Security groups allowed to connect"
  type        = list(string)
  default     = []
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to connect"
  type        = list(string)
  default     = []
}
