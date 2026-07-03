locals {
  internal_public_zone_id  = var.internal_zone_id
  internal_public_zone_arn = local.internal_public_zone_id != "" ? "arn:aws:route53:::hostedzone/${local.internal_public_zone_id}" : ""

  public_env_domain       = "${var.env_name}.${var.public_domain_root}"
  public_env_wildcard     = "*.${local.public_env_domain}"
  public_env_zone_id      = var.create_public_zone ? aws_route53_zone.public_env[0].zone_id : var.public_zone_id
  public_env_cert_enabled = var.create_public_zone || var.public_zone_id != ""

  api_gateway_domain = "${var.api_gateway_subdomain_prefix}.${local.public_env_domain}"
}

data "aws_route53_zone" "internal_public" {
  count   = local.internal_public_zone_id != "" ? 1 : 0
  zone_id = local.internal_public_zone_id
}

resource "aws_route53_zone" "internal_private" {
  name = var.internal_domain

  vpc {
    vpc_id = module.network.vpc_id
  }

  tags = merge(local.tags, {
    Name = "${var.internal_domain}-private"
  })
}

resource "aws_route53_zone" "public_env" {
  count = var.create_public_zone ? 1 : 0
  name  = local.public_env_domain

  tags = merge(local.tags, {
    Name = local.public_env_domain
  })
}

resource "aws_acm_certificate" "internal" {
  count             = local.internal_public_zone_id != "" ? 1 : 0
  domain_name       = "*.${var.internal_domain}"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = merge(local.tags, {
    Name = var.internal_domain
  })
}

resource "aws_route53_record" "internal_cert_validation" {
  for_each = local.internal_public_zone_id != "" ? {
    for dvo in aws_acm_certificate.internal[0].domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  } : {}

  zone_id = local.internal_public_zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 300
  records = [each.value.record]
}

resource "aws_acm_certificate_validation" "internal" {
  count                   = local.internal_public_zone_id != "" ? 1 : 0
  certificate_arn         = aws_acm_certificate.internal[0].arn
  validation_record_fqdns = [for record in aws_route53_record.internal_cert_validation : record.fqdn]
}

resource "aws_acm_certificate" "public_env" {
  count             = local.public_env_cert_enabled ? 1 : 0
  domain_name       = local.public_env_wildcard
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = merge(local.tags, {
    Name = local.public_env_domain
  })
}

resource "aws_route53_record" "public_env_cert_validation" {
  for_each = local.public_env_cert_enabled ? {
    for dvo in aws_acm_certificate.public_env[0].domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  } : {}

  zone_id = local.public_env_zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 300
  records = [each.value.record]
}

resource "aws_acm_certificate_validation" "public_env" {
  count                   = local.public_env_cert_enabled ? 1 : 0
  certificate_arn         = aws_acm_certificate.public_env[0].arn
  validation_record_fqdns = [for record in aws_route53_record.public_env_cert_validation : record.fqdn]
}

output "internal_private_zone_id" {
  value = aws_route53_zone.internal_private.zone_id
}

output "internal_certificate_arn" {
  value = local.internal_public_zone_id != "" ? aws_acm_certificate.internal[0].arn : ""
}

output "public_env_domain" {
  value = local.public_env_domain
}

output "public_env_zone_id" {
  value = local.public_env_zone_id
}

output "public_env_zone_name_servers" {
  value = var.create_public_zone ? aws_route53_zone.public_env[0].name_servers : []
}

output "public_env_certificate_arn" {
  value = local.public_env_cert_enabled ? aws_acm_certificate.public_env[0].arn : ""
}

output "api_gateway_domain" {
  value = local.public_env_cert_enabled ? local.api_gateway_domain : ""
}
