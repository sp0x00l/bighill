variable "env_name" {
  description = "Environment name (staging|prod)"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "template_path" {
  description = "Path to the SAM template.yaml"
  type        = string
}

variable "lambda_artifacts_bucket" {
  description = "S3 bucket for Lambda artifacts"
  type        = string
}

variable "auth_lambda_key" {
  description = "S3 key for the auth Lambda artifact"
  type        = string
}

variable "api_lambda_key" {
  description = "S3 key for the API Lambda artifact"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for Lambda VPC config"
  type        = list(string)
}

variable "lambda_security_group_id" {
  description = "Security group ID for Lambda functions"
  type        = string
}

variable "stage_name" {
  description = "API stage name"
  type        = string
  default     = "staging"
}

variable "bighill_api_function_memory_size" {
  description = "Memory size (MB) for BigHillApiFunction"
  type        = number
  default     = 2048
}

variable "bighill_api_function_timeout" {
  description = "Timeout (seconds) for BigHillApiFunction"
  type        = number
  default     = 60
}

variable "bighill_api_http_client_timeout_seconds" {
  description = "HTTP client timeout (seconds) for backend calls"
  type        = number
  default     = 10
}

variable "bighill_api_log_level" {
  description = "Log level for API/Auth Lambdas"
  type        = string
  default     = "debug"
}

variable "data_registry_service_http_domain" {
  description = "Data registry service host"
  type        = string
  default     = ""

  validation {
    condition     = length(trimspace(var.data_registry_service_http_domain)) > 0
    error_message = "Set data_registry_service_http_domain to the reachable hostname for the data registry service."
  }
}

variable "data_registry_service_http_port" {
  description = "Data registry service port"
  type        = string
  default     = "8081"
}

variable "ingestion_service_http_domain" {
  description = "Ingestion service host"
  type        = string
  default     = ""

  validation {
    condition     = length(trimspace(var.ingestion_service_http_domain)) > 0
    error_message = "Set ingestion_service_http_domain to the reachable hostname for the ingestion service."
  }
}

variable "ingestion_service_http_port" {
  description = "Ingestion service port"
  type        = string
  default     = "8086"
}

variable "model_registry_service_http_domain" {
  description = "Model registry service host"
  type        = string
  default     = ""

  validation {
    condition     = length(trimspace(var.model_registry_service_http_domain)) > 0
    error_message = "Set model_registry_service_http_domain to the reachable hostname for the model registry service."
  }
}

variable "model_registry_service_http_port" {
  description = "Model registry service port"
  type        = string
  default     = "8084"
}

variable "profile_service_http_domain" {
  description = "Profile service host"
  type        = string
  default     = ""

  validation {
    condition     = length(trimspace(var.profile_service_http_domain)) > 0
    error_message = "Set profile_service_http_domain to the reachable hostname for the profile service."
  }
}

variable "profile_service_http_port" {
  description = "Profile service port"
  type        = string
  default     = "8082"
}

variable "training_service_http_domain" {
  description = "Training service host"
  type        = string
  default     = ""

  validation {
    condition     = length(trimspace(var.training_service_http_domain)) > 0
    error_message = "Set training_service_http_domain to the reachable hostname for the training service."
  }
}

variable "training_service_http_port" {
  description = "Training service port"
  type        = string
  default     = "8085"
}

variable "inference_service_http_domain" {
  description = "Inference service host"
  type        = string
  default     = ""

  validation {
    condition     = length(trimspace(var.inference_service_http_domain)) > 0
    error_message = "Set inference_service_http_domain to the reachable hostname for the inference service."
  }
}

variable "inference_service_http_port" {
  description = "Inference service port"
  type        = string
  default     = "8087"
}

variable "otlp_collector_url" {
  description = "OTLP collector host or URL"
  type        = string
  default     = ""
}

variable "otlp_collector_port" {
  description = "OTLP collector port"
  type        = number
  default     = 4317
}

variable "redis_host" {
  description = "Redis host"
  type        = string
  default     = ""

  validation {
    condition     = length(trimspace(var.redis_host)) > 0
    error_message = "Set redis_host to the reachable hostname for Redis."
  }
}

variable "redis_port" {
  description = "Redis port"
  type        = string
  default     = "6379"
}

variable "redis_tls" {
  description = "Enable TLS for Redis (true/false as string)"
  type        = string
  default     = "false"
}

variable "redis_username" {
  description = "Redis username (optional)"
  type        = string
  default     = ""
}

variable "redis_password" {
  description = "Redis password (optional)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "auth_kms_key_id" {
  description = "KMS key ID for JWT signing/verification"
  type        = string
}
