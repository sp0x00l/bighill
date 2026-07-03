variable "env_name" {
  description = "Environment name, for example staging or prod"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "kubernetes_namespace" {
  description = "Kubernetes namespace for BigHill services"
  type        = string
  default     = ""
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "azs" {
  description = "Availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "public_subnets" {
  description = "Public subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnets" {
  description = "Private subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24"]
}

variable "cluster_version" {
  description = "EKS Kubernetes version"
  type        = string
  default     = "1.33"
}

variable "cluster_admin_arns" {
  description = "IAM ARNs to grant EKS admin access"
  type        = list(string)
  default     = []
}

variable "endpoint_public_access" {
  description = "Expose the EKS API endpoint publicly"
  type        = bool
  default     = true
}

variable "endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "endpoint_private_access" {
  description = "Expose the EKS API endpoint inside the VPC"
  type        = bool
  default     = true
}

variable "node_instance_types" {
  description = "EC2 instance types for standard EKS worker nodes"
  type        = list(string)
  default     = ["m7g.large"]
}

variable "node_min_size" {
  description = "Minimum standard worker node count"
  type        = number
  default     = 1
}

variable "node_max_size" {
  description = "Maximum standard worker node count"
  type        = number
  default     = 2
}

variable "node_desired_size" {
  description = "Desired standard worker node count"
  type        = number
  default     = 1
}

variable "gpu_node_enabled" {
  description = "Create a GPU node group for KubeRay training and vLLM serving"
  type        = bool
  default     = false
}

variable "gpu_node_instance_types" {
  description = "EC2 instance types for GPU worker nodes"
  type        = list(string)
  default     = ["g5.xlarge"]
}

variable "gpu_node_ami_type" {
  description = "Accelerated EKS AMI type for GPU worker nodes"
  type        = string
  default     = "AL2023_x86_64_NVIDIA"
}

variable "gpu_node_min_size" {
  description = "Minimum GPU worker node count"
  type        = number
  default     = 0
}

variable "gpu_node_max_size" {
  description = "Maximum GPU worker node count"
  type        = number
  default     = 2
}

variable "gpu_node_desired_size" {
  description = "Desired GPU worker node count"
  type        = number
  default     = 0
}

variable "object_store_bucket_name" {
  description = "Override S3 bucket name for raw uploads, snapshots, model artifacts, evaluations, and preference datasets"
  type        = string
  default     = ""
}

variable "object_store_service_accounts" {
  description = "Service accounts granted object-store IRSA access"
  type        = list(string)
  default = [
    "data-ingestion-service",
    "data-stream-service",
    "feature-materializer-service",
    "inference-service",
    "model-registry-service",
    "training-service",
  ]
}

variable "object_store_staging_expiration_days" {
  description = "Days before incomplete direct-upload staging objects expire"
  type        = number
  default     = 3
}

variable "object_store_abort_multipart_days" {
  description = "Days before incomplete multipart uploads are aborted"
  type        = number
  default     = 7
}

variable "object_store_noncurrent_expiration_days" {
  description = "Days to retain noncurrent object versions"
  type        = number
  default     = 30
}

variable "object_store_noncurrent_versions" {
  description = "Number of newer noncurrent versions to retain"
  type        = number
  default     = 5
}

variable "internal_domain" {
  description = "Internal service DNS root"
  type        = string
  default     = "internal.bighill.example"
}

variable "internal_zone_id" {
  description = "Optional public Route53 hosted zone ID for internal_domain certificate validation"
  type        = string
  default     = ""
}

variable "public_domain_root" {
  description = "Public root domain for externally accessible endpoints"
  type        = string
  default     = "bighill.example"
}

variable "public_zone_id" {
  description = "Route53 hosted zone ID for env.public_domain_root when create_public_zone is false"
  type        = string
  default     = ""
}

variable "create_public_zone" {
  description = "Create a public Route53 hosted zone for env.public_domain_root"
  type        = bool
  default     = false
}

variable "api_gateway_subdomain_prefix" {
  description = "Subdomain prefix for the API Gateway custom domain"
  type        = string
  default     = "api"
}

variable "aurora_engine_version" {
  description = "Aurora PostgreSQL engine version"
  type        = string
  default     = "17.5"
}

variable "aurora_database_name" {
  description = "Initial Aurora database name"
  type        = string
  default     = "bighill"
}

variable "aurora_master_username" {
  description = "Aurora master username"
  type        = string
  default     = "bighill_admin"
}

variable "aurora_master_password" {
  description = "Aurora master password. If null, Terraform generates one."
  type        = string
  default     = null
  sensitive   = true
}

variable "aurora_instance_class" {
  description = "Aurora instance class"
  type        = string
  default     = "db.t4g.medium"
}

variable "aurora_instance_count" {
  description = "Aurora instance count"
  type        = number
  default     = 1
}

variable "aurora_backup_retention_period" {
  description = "Aurora backup retention in days"
  type        = number
  default     = 7
}

variable "aurora_preferred_backup_window" {
  description = "Aurora backup window"
  type        = string
  default     = "04:00-05:00"
}

variable "aurora_preferred_maintenance_window" {
  description = "Aurora maintenance window"
  type        = string
  default     = "Sun:05:00-Sun:06:00"
}

variable "aurora_deletion_protection" {
  description = "Enable Aurora deletion protection"
  type        = bool
  default     = false
}

variable "aurora_skip_final_snapshot" {
  description = "Skip final Aurora snapshot on destroy"
  type        = bool
  default     = true
}

variable "aurora_apply_immediately" {
  description = "Apply Aurora modifications immediately"
  type        = bool
  default     = true
}

variable "aurora_recovery_window_in_days" {
  description = "Secrets Manager recovery window for Aurora credentials"
  type        = number
  default     = 7
}

variable "aurora_secret_name_override" {
  description = "Optional Aurora secret name override"
  type        = string
  default     = ""
}

variable "create_codeartifact" {
  description = "Create CodeArtifact domain/repository for native library artifacts"
  type        = bool
  default     = true
}

variable "codeartifact_domain_name" {
  description = "CodeArtifact domain name"
  type        = string
  default     = "bighill"
}

variable "codeartifact_repository_name" {
  description = "CodeArtifact repository name"
  type        = string
  default     = "bighill-native-artifacts"
}

variable "deploy_api_gateway" {
  description = "Deploy the Lambda API Gateway stack. Requires built API Lambda zip artifacts."
  type        = bool
  default     = false
}

variable "lambda_artifacts_bucket_name" {
  description = "Override Lambda artifact bucket name"
  type        = string
  default     = ""
}

variable "data_registry_service_http_domain" {
  description = "Data registry service hostname reachable from API Lambda"
  type        = string
  default     = "data-registry-service.internal.bighill.example"
}

variable "data_registry_service_http_port" {
  description = "Data registry service HTTP port"
  type        = string
  default     = "80"
}

variable "data_ingestion_service_http_domain" {
  description = "Data ingestion service hostname reachable from API Lambda"
  type        = string
  default     = "data-ingestion-service.internal.bighill.example"
}

variable "data_ingestion_service_http_port" {
  description = "Data ingestion service HTTP port"
  type        = string
  default     = "80"
}

variable "profile_service_http_domain" {
  description = "Profile service hostname reachable from API Lambda"
  type        = string
  default     = "profile-service.internal.bighill.example"
}

variable "profile_service_http_port" {
  description = "Profile service HTTP port"
  type        = string
  default     = "80"
}

variable "redis_host" {
  description = "Redis hostname reachable from API Lambda"
  type        = string
  default     = "redis.internal.bighill.example"
}

variable "redis_port" {
  description = "Redis port"
  type        = number
  default     = 6379
}

variable "redis_tls" {
  description = "Enable Redis TLS as true/false string"
  type        = string
  default     = "false"
}

variable "redis_username" {
  description = "Redis username"
  type        = string
  default     = ""
}

variable "redis_password" {
  description = "Redis password"
  type        = string
  default     = ""
  sensitive   = true
}

variable "otlp_collector_url" {
  description = "OTLP collector endpoint for API Lambda traces"
  type        = string
  default     = "http://otel-collector-opentelemetry-collector.observability.svc.cluster.local:4318"
}

variable "otlp_collector_port" {
  description = "OTLP collector port"
  type        = number
  default     = 4318
}
