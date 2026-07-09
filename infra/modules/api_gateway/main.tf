locals {
  private_subnet_ids_csv = join(",", var.private_subnet_ids)
  api_code_uri           = format("s3://%s/%s", var.lambda_artifacts_bucket, var.api_lambda_key)
  auth_code_uri          = format("s3://%s/%s", var.lambda_artifacts_bucket, var.auth_lambda_key)
}

resource "aws_cloudformation_stack" "api_gateway" {
  name = "bighill-${var.env_name}-api-gateway"

  capabilities = [
    "CAPABILITY_IAM",
    "CAPABILITY_AUTO_EXPAND",
  ]

  # The API Gateway stack is deployed from the source SAM template.
  # Lambda artifacts are uploaded separately by Terraform and injected via parameters.
  template_body = file(var.template_path)

  parameters = {
    StageNameParam                     = var.stage_name
    BighillApiFunctionMemorySize       = var.bighill_api_function_memory_size
    BighillApiFunctionTimeout          = var.bighill_api_function_timeout
    BighillApiHttpClientTimeoutSeconds = var.bighill_api_http_client_timeout_seconds
    BighillApiLogLevel                 = var.bighill_api_log_level

    BigHillApiCodeUri  = local.api_code_uri
    BigHillAuthCodeUri = local.auth_code_uri

    PrivateSubnetIds      = local.private_subnet_ids_csv
    LambdaSecurityGroupId = var.lambda_security_group_id

    DataRegistryServiceHttpDomain  = var.data_registry_service_http_domain
    DataRegistryServiceHttpPort    = var.data_registry_service_http_port
    IngestionServiceHttpDomain     = var.ingestion_service_http_domain
    IngestionServiceHttpPort       = var.ingestion_service_http_port
    ModelRegistryServiceHttpDomain = var.model_registry_service_http_domain
    ModelRegistryServiceHttpPort   = var.model_registry_service_http_port
    TenantServiceHttpDomain       = var.tenant_service_http_domain
    TenantServiceHttpPort         = var.tenant_service_http_port
    TrainingServiceHttpDomain      = var.training_service_http_domain
    TrainingServiceHttpPort        = var.training_service_http_port
    InferenceServiceHttpDomain     = var.inference_service_http_domain
    InferenceServiceHttpPort       = var.inference_service_http_port

    RedisHost     = var.redis_host
    RedisPort     = var.redis_port
    RedisTLS      = var.redis_tls
    RedisUsername = var.redis_username
    RedisPassword = var.redis_password

    OtlpCollectorUrl  = var.otlp_collector_url
    OtlpCollectorPort = var.otlp_collector_port

    AuthKmsKeyId = var.auth_kms_key_id

  }
}

output "api_gateway_stack_id" {
  description = "CloudFormation stack ID for the API Gateway SAM deployment"
  value       = aws_cloudformation_stack.api_gateway.id
}

output "api_gateway_stack_outputs" {
  description = "Outputs from the API Gateway CloudFormation stack"
  value       = aws_cloudformation_stack.api_gateway.outputs
}
