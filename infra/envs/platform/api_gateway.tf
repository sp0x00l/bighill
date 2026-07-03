resource "aws_security_group" "lambda" {
  count = var.deploy_api_gateway ? 1 : 0

  name        = "bighill-${var.env_name}-lambda-sg"
  description = "Lambda functions calling BigHill services"
  vpc_id      = module.network.vpc_id

  egress {
    description = "All traffic within VPC CIDR"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description     = "HTTPS to Amazon S3 via AWS-managed prefix list"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    prefix_list_ids = [data.aws_ec2_managed_prefix_list.s3.id]
  }

  egress {
    description = "HTTPS to AWS APIs via NAT"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, {
    Name = "bighill-${var.env_name}-lambda-sg"
  })
}

resource "aws_security_group_rule" "eks_http_from_lambda" {
  count = var.deploy_api_gateway ? 1 : 0

  type                     = "ingress"
  from_port                = 8080
  to_port                  = 8089
  protocol                 = "tcp"
  security_group_id        = module.eks.node_security_group_id
  source_security_group_id = aws_security_group.lambda[0].id
  description              = "Allow API Gateway Lambda to reach service HTTP ports"
}

resource "aws_security_group_rule" "redis_from_lambda" {
  count = var.deploy_api_gateway ? 1 : 0

  type                     = "ingress"
  from_port                = 6379
  to_port                  = 6379
  protocol                 = "tcp"
  security_group_id        = module.eks.node_security_group_id
  source_security_group_id = aws_security_group.lambda[0].id
  description              = "Allow API Gateway Lambda to reach Redis"
}

module "api_gateway" {
  count = var.deploy_api_gateway ? 1 : 0

  source = "../../modules/api_gateway"

  env_name   = var.env_name
  region     = var.region
  stage_name = var.env_name

  template_path = "${path.module}/../../../api_gateway/template.yml"

  lambda_artifacts_bucket = aws_s3_bucket.lambda_artifacts[0].bucket
  auth_lambda_key         = aws_s3_object.auth_lambda_zip[0].key
  api_lambda_key          = aws_s3_object.api_lambda_zip[0].key

  private_subnet_ids       = module.network.private_subnet_ids
  lambda_security_group_id = aws_security_group.lambda[0].id

  data_registry_service_http_domain  = var.data_registry_service_http_domain
  data_registry_service_http_port    = var.data_registry_service_http_port
  data_ingestion_service_http_domain = var.data_ingestion_service_http_domain
  data_ingestion_service_http_port   = var.data_ingestion_service_http_port
  profile_service_http_domain        = var.profile_service_http_domain
  profile_service_http_port          = var.profile_service_http_port

  redis_host     = var.redis_host
  redis_port     = tostring(var.redis_port)
  redis_tls      = var.redis_tls
  redis_username = var.redis_username
  redis_password = var.redis_password

  otlp_collector_url  = var.otlp_collector_url
  otlp_collector_port = var.otlp_collector_port

  auth_kms_key_id = module.eks.jwt_signing_key_id
}

output "lambda_security_group_id" {
  value = var.deploy_api_gateway ? aws_security_group.lambda[0].id : ""
}

output "api_gateway_stack_id" {
  value = var.deploy_api_gateway ? module.api_gateway[0].api_gateway_stack_id : ""
}

output "api_gateway_stack_outputs" {
  value = var.deploy_api_gateway ? module.api_gateway[0].api_gateway_stack_outputs : {}
}
