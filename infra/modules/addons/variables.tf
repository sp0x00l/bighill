variable "env_name" {
  description = "Environment name"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "internal_domain" {
  description = "Internal domain root"
  type        = string
}

variable "route53_zone_id" {
  description = "Route53 zone ID for internal domain"
  type        = string
}

variable "route53_zone_arn" {
  description = "Route53 zone ARN for internal domain (public)"
  type        = string
}

variable "route53_private_zone_id" {
  description = "Route53 private zone ID for VPC-internal DNS"
  type        = string
  default     = ""
}

variable "route53_private_zone_arn" {
  description = "Route53 private zone ARN for VPC-internal DNS"
  type        = string
  default     = ""
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

variable "eks" {
  description = "EKS module outputs"
  type = object({
    cluster_name                       = string
    cluster_endpoint                   = string
    cluster_certificate_authority_data = string
    oidc_provider_arn                  = string
    oidc_provider_host                 = string
  })
}

variable "route53_public_env_zone_arn" {
  description = "Route53 zone ARN for public environment domain (e.g., staging.northern.bighill)"
  type        = string
  default     = ""
}
