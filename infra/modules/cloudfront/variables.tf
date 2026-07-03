variable "env_name" {
  description = "Environment name used for resource naming"
  type        = string
}

variable "site_name" {
  description = "Short identifier for the site (used in bucket naming)"
  type        = string
}

variable "domain_name" {
  description = "Fully qualified domain name for the site (e.g., app.staging.northern.bighill)"
  type        = string
}

variable "zone_id" {
  description = "Route53 hosted zone ID for the domain_name"
  type        = string
}

variable "enabled" {
  description = "Whether to create resources for the site"
  type        = bool
  default     = true
}

variable "bucket_name" {
  description = "Override S3 bucket name"
  type        = string
  default     = ""
}

variable "force_destroy" {
  description = "Allow bucket destroy without manual object cleanup"
  type        = bool
  default     = false
}

variable "default_root_object" {
  description = "Default root object for the CloudFront distribution"
  type        = string
  default     = "index.html"
}

variable "cache_policy_id" {
  description = "CloudFront cache policy ID"
  type        = string
  default     = "658327ea-f89d-4fab-a63d-7e88639e58f6" # Managed-CachingOptimized
}

variable "price_class" {
  description = "CloudFront price class"
  type        = string
  default     = "PriceClass_100"
}

variable "enable_spa_routing" {
  description = "Enable SPA routing by serving index.html on 403/404"
  type        = bool
  default     = true
}
