terraform {
  required_providers {
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

locals {
  master_password = var.master_password != null ? var.master_password : random_password.master[0].result
}

resource "random_password" "master" {
  count = var.master_password == null ? 1 : 0

  length  = 32
  special = true
  # AWS RDS disallows '/', '@', '\"', and space in passwords
  override_special = "!#$%&*()-_=+[]{}<>:?~"
}

resource "aws_db_subnet_group" "this" {
  name       = "bighill-${var.env_name}-aurora"
  subnet_ids = var.private_subnet_ids

  tags = {
    Name        = "bighill-${var.env_name}-aurora"
    Environment = var.env_name
  }
}

resource "aws_security_group" "this" {
  name        = "bighill-${var.env_name}-aurora"
  description = "Aurora PostgreSQL access"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "bighill-${var.env_name}-aurora"
    Environment = var.env_name
  }
}

resource "aws_security_group_rule" "from_security_groups" {
  count = length(var.allowed_security_group_ids)

  type                     = "ingress"
  description              = "Allow Postgres from SG ${var.allowed_security_group_ids[count.index]}"
  security_group_id        = aws_security_group.this.id
  source_security_group_id = var.allowed_security_group_ids[count.index]
  from_port                = var.port
  to_port                  = var.port
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "from_cidr_blocks" {
  count = length(var.allowed_cidr_blocks)

  type              = "ingress"
  description       = "Allow Postgres from CIDR ${var.allowed_cidr_blocks[count.index]}"
  security_group_id = aws_security_group.this.id
  cidr_blocks       = [var.allowed_cidr_blocks[count.index]]
  from_port         = var.port
  to_port           = var.port
  protocol          = "tcp"
}

resource "aws_rds_cluster" "this" {
  cluster_identifier = "bighill-${var.env_name}-aurora"
  engine             = "aurora-postgresql"
  engine_version     = var.engine_version

  database_name   = var.database_name
  master_username = var.master_username
  master_password = local.master_password

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.this.id]
  port                   = var.port

  storage_encrypted            = true
  backup_retention_period      = var.backup_retention_period
  preferred_backup_window      = var.preferred_backup_window
  preferred_maintenance_window = var.preferred_maintenance_window
  deletion_protection          = var.deletion_protection
  copy_tags_to_snapshot        = true
  skip_final_snapshot          = var.skip_final_snapshot

  tags = {
    Name        = "bighill-${var.env_name}-aurora"
    Environment = var.env_name
  }
}

resource "aws_rds_cluster_instance" "this" {
  count = var.instance_count

  identifier         = "bighill-${var.env_name}-aurora-${count.index}"
  cluster_identifier = aws_rds_cluster.this.id

  instance_class             = var.instance_class
  engine                     = aws_rds_cluster.this.engine
  engine_version             = aws_rds_cluster.this.engine_version
  db_subnet_group_name       = aws_db_subnet_group.this.name
  publicly_accessible        = false
  auto_minor_version_upgrade = true
  apply_immediately          = var.apply_immediately

  tags = {
    Name        = "bighill-${var.env_name}-aurora-${count.index}"
    Environment = var.env_name
  }
}

resource "aws_secretsmanager_secret" "aurora" {
  name                    = var.secret_name_override != "" ? var.secret_name_override : "bighill/${var.env_name}/aurora-${random_id.secret_suffix.hex}"
  recovery_window_in_days = var.recovery_window_in_days

  tags = {
    Name        = "bighill-${var.env_name}-aurora"
    Environment = var.env_name
  }
}

resource "random_id" "secret_suffix" {
  byte_length = 4
}

resource "aws_secretsmanager_secret_version" "aurora" {
  secret_id = aws_secretsmanager_secret.aurora.id
  secret_string = jsonencode({
    engine       = aws_rds_cluster.this.engine
    host         = aws_rds_cluster.this.endpoint
    reader_host  = aws_rds_cluster.this.reader_endpoint
    port         = var.port
    database     = var.database_name
    username     = var.master_username
    password     = local.master_password
    security_gid = aws_security_group.this.id
  })
}
