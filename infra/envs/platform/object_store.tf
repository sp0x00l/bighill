locals {
  object_store_bucket_name = var.object_store_bucket_name != "" ? var.object_store_bucket_name : "bighill-${var.env_name}-${data.aws_caller_identity.current.account_id}-${var.region}-object-store"
}

resource "aws_s3_bucket" "object_store" {
  bucket        = local.object_store_bucket_name
  force_destroy = var.env_name != "prod"

  tags = merge(local.tags, {
    Name = local.object_store_bucket_name
  })
}

resource "aws_s3_bucket_versioning" "object_store" {
  bucket = aws_s3_bucket.object_store.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "object_store" {
  bucket = aws_s3_bucket.object_store.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "object_store" {
  bucket = aws_s3_bucket.object_store.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "object_store" {
  bucket = aws_s3_bucket.object_store.id

  rule {
    id     = "expire-staging-uploads"
    status = "Enabled"

    filter {
      prefix = "staging/"
    }

    expiration {
      days = var.object_store_staging_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = var.object_store_abort_multipart_days
    }
  }

  rule {
    id     = "cleanup-old-versions"
    status = "Enabled"

    filter {
      prefix = ""
    }

    noncurrent_version_expiration {
      noncurrent_days           = var.object_store_noncurrent_expiration_days
      newer_noncurrent_versions = var.object_store_noncurrent_versions
    }
  }
}
