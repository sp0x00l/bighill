locals {
  enabled         = var.enabled && var.domain_name != "" && var.zone_id != ""
  bucket_name     = var.bucket_name != "" ? var.bucket_name : "bighill-${var.env_name}-${var.site_name}"
  spa_error_codes = var.enable_spa_routing ? [403, 404] : []
}

resource "aws_s3_bucket" "site" {
  count  = local.enabled ? 1 : 0
  bucket = local.bucket_name

  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "site" {
  count  = local.enabled ? 1 : 0
  bucket = aws_s3_bucket.site[0].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "site" {
  count  = local.enabled ? 1 : 0
  bucket = aws_s3_bucket.site[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "site" {
  count  = local.enabled ? 1 : 0
  bucket = aws_s3_bucket.site[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_acm_certificate" "site" {
  count                     = local.enabled ? 1 : 0
  domain_name               = var.domain_name
  validation_method         = "DNS"
  subject_alternative_names = []

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "site_cert_validation" {
  for_each = local.enabled ? {
    for dvo in aws_acm_certificate.site[0].domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  } : {}

  zone_id = var.zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 300
  records = [each.value.record]
}

resource "aws_acm_certificate_validation" "site" {
  count = local.enabled ? 1 : 0

  certificate_arn         = aws_acm_certificate.site[0].arn
  validation_record_fqdns = [for record in aws_route53_record.site_cert_validation : record.fqdn]
}

resource "aws_cloudfront_origin_access_control" "site" {
  count                             = local.enabled ? 1 : 0
  name                              = "bighill-${var.env_name}-${var.site_name}-oac"
  description                       = "OAC for ${var.site_name} S3 bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_distribution" "site" {
  count = local.enabled ? 1 : 0

  enabled             = true
  price_class         = var.price_class
  default_root_object = var.default_root_object
  aliases             = [var.domain_name]

  origin {
    domain_name              = aws_s3_bucket.site[0].bucket_regional_domain_name
    origin_id                = "${var.site_name}-s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.site[0].id
  }

  default_cache_behavior {
    target_origin_id       = "${var.site_name}-s3"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true
    cache_policy_id        = var.cache_policy_id
  }

  dynamic "custom_error_response" {
    for_each = local.spa_error_codes
    content {
      error_code            = custom_error_response.value
      response_code         = 200
      response_page_path    = "/index.html"
      error_caching_min_ttl = 0
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate.site[0].arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }

  depends_on = [aws_acm_certificate_validation.site]
}

resource "aws_s3_bucket_policy" "site" {
  count  = local.enabled ? 1 : 0
  bucket = aws_s3_bucket.site[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowCloudFrontAccess"
        Effect = "Allow"
        Principal = {
          Service = "cloudfront.amazonaws.com"
        }
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.site[0].arn}/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.site[0].arn
          }
        }
      }
    ]
  })
}

resource "aws_route53_record" "site_alias" {
  count = local.enabled ? 1 : 0

  zone_id = var.zone_id
  name    = var.domain_name
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.site[0].domain_name
    zone_id                = "Z2FDTNDATAQYW2"
    evaluate_target_health = false
  }
}

output "site_domain" {
  description = "Hostname for the static site"
  value       = local.enabled ? var.domain_name : ""
}

output "cloudfront_domain" {
  description = "CloudFront distribution domain"
  value       = local.enabled ? aws_cloudfront_distribution.site[0].domain_name : ""
}

output "distribution_id" {
  description = "CloudFront distribution ID"
  value       = local.enabled ? aws_cloudfront_distribution.site[0].id : ""
}

output "bucket_name" {
  description = "S3 bucket name for the static site"
  value       = local.enabled ? aws_s3_bucket.site[0].bucket : ""
}
