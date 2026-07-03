locals {
  lambda_artifacts_bucket_name = var.lambda_artifacts_bucket_name != "" ? var.lambda_artifacts_bucket_name : "bighill-${var.env_name}-${data.aws_caller_identity.current.account_id}-${var.region}-lambda-artifacts"
  auth_zip_path                = "${path.module}/../../../api_gateway/build/dist/auth.zip"
  api_zip_path                 = "${path.module}/../../../api_gateway/build/dist/api.zip"
}

resource "aws_s3_bucket" "lambda_artifacts" {
  count         = var.deploy_api_gateway ? 1 : 0
  bucket        = local.lambda_artifacts_bucket_name
  force_destroy = var.env_name != "prod"

  tags = merge(local.tags, {
    Name = local.lambda_artifacts_bucket_name
  })
}

resource "aws_s3_bucket_versioning" "lambda_artifacts" {
  count  = var.deploy_api_gateway ? 1 : 0
  bucket = aws_s3_bucket.lambda_artifacts[0].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lambda_artifacts" {
  count  = var.deploy_api_gateway ? 1 : 0
  bucket = aws_s3_bucket.lambda_artifacts[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lambda_artifacts" {
  count  = var.deploy_api_gateway ? 1 : 0
  bucket = aws_s3_bucket.lambda_artifacts[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lambda_artifacts" {
  count  = var.deploy_api_gateway ? 1 : 0
  bucket = aws_s3_bucket.lambda_artifacts[0].id

  rule {
    id     = "cleanup-old-versions"
    status = "Enabled"

    filter {
      prefix = ""
    }

    noncurrent_version_expiration {
      noncurrent_days           = 30
      newer_noncurrent_versions = 5
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_object" "auth_lambda_zip" {
  count = var.deploy_api_gateway ? 1 : 0

  bucket      = aws_s3_bucket.lambda_artifacts[0].bucket
  key         = "lambda/auth.zip"
  source      = local.auth_zip_path
  source_hash = filemd5(local.auth_zip_path)

  lifecycle {
    precondition {
      condition     = fileexists(local.auth_zip_path)
      error_message = "Build the auth Lambda artifact at ${local.auth_zip_path} before planning/applying with deploy_api_gateway=true."
    }
  }
}

resource "aws_s3_object" "api_lambda_zip" {
  count = var.deploy_api_gateway ? 1 : 0

  bucket      = aws_s3_bucket.lambda_artifacts[0].bucket
  key         = "lambda/api.zip"
  source      = local.api_zip_path
  source_hash = filemd5(local.api_zip_path)

  lifecycle {
    precondition {
      condition     = fileexists(local.api_zip_path)
      error_message = "Build the API Lambda artifact at ${local.api_zip_path} before planning/applying with deploy_api_gateway=true."
    }
  }
}

output "lambda_artifacts_bucket_name" {
  value = var.deploy_api_gateway ? aws_s3_bucket.lambda_artifacts[0].bucket : ""
}
