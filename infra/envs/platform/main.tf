terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  backend "s3" {}
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

data "aws_ec2_managed_prefix_list" "s3" {
  name = "com.amazonaws.${var.region}.s3"
}

resource "terraform_data" "cluster_admin_guard" {
  input = var.env_name

  lifecycle {
    precondition {
      condition     = !contains(["staging", "prod"], var.env_name) || length(var.cluster_admin_arns) > 0
      error_message = "cluster_admin_arns must contain at least one IAM principal before applying staging/prod. EKS cluster-creator admin access is intentionally disabled."
    }
  }
}

locals {
  namespace = var.kubernetes_namespace != "" ? var.kubernetes_namespace : "ml-ops-${var.env_name}"
  tags = {
    Environment = var.env_name
    Terraform   = "true"
    Project     = "bighill"
  }
}

module "network" {
  source = "../../modules/network"

  env_name        = var.env_name
  region          = var.region
  vpc_cidr        = var.vpc_cidr
  azs             = var.azs
  public_subnets  = var.public_subnets
  private_subnets = var.private_subnets
}

module "iam" {
  source   = "../../modules/iam"
  env_name = var.env_name
}

module "eks" {
  source = "../../modules/eks"

  env_name             = var.env_name
  cluster_version      = var.cluster_version
  kubernetes_namespace = local.namespace

  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids

  cluster_role_arn = module.iam.cluster_role_arn
  node_role_arn    = module.iam.node_role_arn

  endpoint_private_access      = var.endpoint_private_access
  endpoint_public_access       = var.endpoint_public_access
  endpoint_public_access_cidrs = var.endpoint_public_access_cidrs

  node_instance_types = var.node_instance_types
  node_min_size       = var.node_min_size
  node_max_size       = var.node_max_size
  node_desired_size   = var.node_desired_size

  gpu_node_enabled        = var.gpu_node_enabled
  gpu_node_instance_types = var.gpu_node_instance_types
  gpu_node_ami_type       = var.gpu_node_ami_type
  gpu_node_min_size       = var.gpu_node_min_size
  gpu_node_max_size       = var.gpu_node_max_size
  gpu_node_desired_size   = var.gpu_node_desired_size

  object_store_bucket_arns      = [aws_s3_bucket.object_store.arn]
  object_store_service_accounts = var.object_store_service_accounts
}

module "addons" {
  source = "../../modules/addons"

  env_name = var.env_name
  region   = var.region

  internal_domain = var.internal_domain

  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids

  route53_zone_id  = local.internal_public_zone_id
  route53_zone_arn = local.internal_public_zone_arn

  route53_private_zone_id  = aws_route53_zone.internal_private.zone_id
  route53_private_zone_arn = "arn:aws:route53:::hostedzone/${aws_route53_zone.internal_private.zone_id}"

  route53_public_env_zone_arn = local.public_env_zone_id != "" ? "arn:aws:route53:::hostedzone/${local.public_env_zone_id}" : ""

  eks = module.eks
}

module "aurora" {
  source = "../../modules/aurora"

  env_name           = var.env_name
  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids

  engine_version  = var.aurora_engine_version
  database_name   = var.aurora_database_name
  master_username = var.aurora_master_username
  master_password = var.aurora_master_password
  instance_class  = var.aurora_instance_class
  instance_count  = var.aurora_instance_count

  backup_retention_period      = var.aurora_backup_retention_period
  preferred_backup_window      = var.aurora_preferred_backup_window
  preferred_maintenance_window = var.aurora_preferred_maintenance_window
  deletion_protection          = var.aurora_deletion_protection
  skip_final_snapshot          = var.aurora_skip_final_snapshot
  apply_immediately            = var.aurora_apply_immediately

  allowed_security_group_ids = [
    module.eks.node_security_group_id,
    module.eks.cluster_security_group_id,
  ]
  allowed_cidr_blocks = [module.network.vpc_cidr_block]

  recovery_window_in_days = var.aurora_recovery_window_in_days
  secret_name_override    = var.aurora_secret_name_override
}

module "codeartifact" {
  count = var.create_codeartifact ? 1 : 0

  source = "../../modules/codeartifact"

  domain_name     = var.codeartifact_domain_name
  repository_name = var.codeartifact_repository_name
  tags            = local.tags
}

resource "aws_eks_access_entry" "cluster_admins" {
  for_each = toset(var.cluster_admin_arns)

  cluster_name  = module.eks.cluster_name
  principal_arn = each.value
  type          = "STANDARD"

  tags = local.tags
}

resource "aws_eks_access_policy_association" "cluster_admins_admin" {
  for_each = aws_eks_access_entry.cluster_admins

  cluster_name  = each.value.cluster_name
  principal_arn = each.value.principal_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }
}

resource "random_password" "grafana_admin" {
  length  = 24
  special = false
}

resource "aws_ssm_parameter" "grafana_admin_password" {
  name        = "/bighill/${var.env_name}/grafana-admin-password"
  description = "Grafana admin password for ${var.env_name}"
  type        = "SecureString"
  value       = random_password.grafana_admin.result
  tags        = local.tags
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "namespace" {
  value = local.namespace
}

output "aurora_secret_arn" {
  value = module.aurora.secret_arn
}

output "jwt_signing_key_id" {
  value = module.eks.jwt_signing_key_id
}

output "tenant_service_role_arn" {
  value = module.eks.tenant_service_role_arn
}

output "object_store_bucket_name" {
  value = aws_s3_bucket.object_store.bucket
}

output "object_store_service_role_arns" {
  value = module.eks.object_store_service_role_arns
}

output "alb_controller_role_arn" {
  value = module.addons.alb_controller_role_arn
}

output "external_dns_role_arn" {
  value = module.addons.external_dns_role_arn
}

output "grafana_admin_password_ssm_path" {
  value = aws_ssm_parameter.grafana_admin_password.name
}
