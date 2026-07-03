variable "env_name" {
  type = string
}

variable "cluster_version" {
  type    = string
  default = "1.33"
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "cluster_role_arn" {
  type = string
}

variable "node_role_arn" {
  type = string
}

variable "endpoint_public_access" {
  description = "Whether the EKS API endpoint is publicly accessible"
  type        = bool
  default     = false
}

variable "endpoint_public_access_cidrs" {
  description = "CIDRs allowed to access the public endpoint (used when endpoint_public_access is true)"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "endpoint_private_access" {
  description = "Whether the EKS API endpoint is privately accessible"
  type        = bool
  default     = true
}

variable "auth_kms_key_id" {
  description = "KMS key ID for JWT signing"
  type        = string
  default     = ""
}

variable "route53_private_zone_id" {
  description = "Route53 private hosted zone ID for internal DNS"
  type        = string
  default     = ""
}

variable "route53_private_zone_arn" {
  description = "Route53 private hosted zone ARN for IAM policies"
  type        = string
  default     = ""
}

variable "node_instance_types" {
  description = "EC2 instance types for EKS worker nodes"
  type        = list(string)
  default     = ["m7g.large"]
}

variable "node_min_size" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "node_max_size" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 2
}

variable "node_desired_size" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 1
}

variable "kubernetes_namespace" {
  description = "Kubernetes namespace used by BigHill services for IRSA trust policies"
  type        = string
  default     = ""
}

variable "object_store_bucket_arns" {
  description = "S3 bucket ARNs used for raw uploads, lakehouse snapshots, model artifacts, and preference datasets"
  type        = list(string)
  default     = []
}

variable "object_store_service_accounts" {
  description = "Service accounts that need object-store access"
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

variable "gpu_node_enabled" {
  description = "Create a GPU managed node group for KubeRay/vLLM workloads"
  type        = bool
  default     = false
}

variable "gpu_node_instance_types" {
  description = "EC2 instance types for the GPU managed node group"
  type        = list(string)
  default     = ["g5.xlarge"]
}

variable "gpu_node_ami_type" {
  description = "EKS optimized accelerated AMI type for the GPU node group"
  type        = string
  default     = "AL2023_x86_64_NVIDIA"
}

variable "gpu_node_min_size" {
  description = "Minimum number of GPU worker nodes"
  type        = number
  default     = 0
}

variable "gpu_node_max_size" {
  description = "Maximum number of GPU worker nodes"
  type        = number
  default     = 2
}

variable "gpu_node_desired_size" {
  description = "Desired number of GPU worker nodes"
  type        = number
  default     = 0
}
