terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

locals {
  alb_controller_sa = "aws-load-balancer-controller"
  external_dns_sa   = "external-dns"
}

data "aws_iam_policy_document" "alb_controller_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.eks.oidc_provider_host}:sub"
      values   = ["system:serviceaccount:kube-system:${local.alb_controller_sa}"]
    }
  }
}

resource "aws_iam_role" "alb_controller" {
  name               = "bighill-${var.env_name}-alb-controller"
  assume_role_policy = data.aws_iam_policy_document.alb_controller_assume.json
}

# ALB Controller needs EC2 permissions for subnet/AZ discovery
data "aws_iam_policy_document" "alb_controller_policy" {
  # EC2 permissions for subnet and AZ discovery
  statement {
    effect = "Allow"
    actions = [
      "ec2:DescribeAvailabilityZones",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeInstances",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeTags",
      "ec2:DescribeAccountAttributes",
      "ec2:DescribeAddresses",
      "ec2:DescribeInternetGateways",
      "ec2:DescribeCoipPools",
      "ec2:GetCoipPoolUsage"
    ]
    resources = ["*"]
  }

  # Security group management
  statement {
    effect = "Allow"
    actions = [
      "ec2:CreateSecurityGroup",
      "ec2:DeleteSecurityGroup",
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupIngress",
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:CreateTags",
      "ec2:DeleteTags"
    ]
    resources = ["*"]
  }

  # ELB permissions - full access for ALB/NLB management
  # The AWS Load Balancer Controller needs broad ELB permissions to manage ALBs, NLBs, Target Groups, etc.
  # Restricting these permissions causes hard-to-debug failures when new API actions are required.
  statement {
    effect    = "Allow"
    actions   = ["elasticloadbalancing:*"]
    resources = ["*"]
  }

  # IAM permissions for service-linked roles
  statement {
    effect = "Allow"
    actions = [
      "iam:CreateServiceLinkedRole"
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "iam:AWSServiceName"
      values   = ["elasticloadbalancing.amazonaws.com"]
    }
  }

  # ACM for TLS certificates
  statement {
    effect = "Allow"
    actions = [
      "acm:ListCertificates",
      "acm:DescribeCertificate"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "alb_controller" {
  name   = "bighill-${var.env_name}-alb-controller"
  policy = data.aws_iam_policy_document.alb_controller_policy.json
}

resource "aws_iam_role_policy_attachment" "alb_controller" {
  role       = aws_iam_role.alb_controller.name
  policy_arn = aws_iam_policy.alb_controller.arn
}

data "aws_iam_policy_document" "external_dns_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.eks.oidc_provider_host}:sub"
      values   = ["system:serviceaccount:kube-system:${local.external_dns_sa}"]
    }
  }
}

data "aws_iam_policy_document" "external_dns_policy" {
  statement {
    effect = "Allow"
    actions = [
      "route53:ChangeResourceRecordSets",
      "route53:ListResourceRecordSets"
    ]
    resources = compact([
      var.route53_zone_arn,
      var.route53_private_zone_arn,
      var.route53_public_env_zone_arn
    ])
  }

  statement {
    effect = "Allow"
    actions = [
      "route53:ListHostedZones",
      "route53:ListHostedZonesByName",
      "route53:ListTagsForResource"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "external_dns" {
  name               = "bighill-${var.env_name}-external-dns"
  assume_role_policy = data.aws_iam_policy_document.external_dns_assume.json
}

resource "aws_iam_policy" "external_dns" {
  name   = "bighill-${var.env_name}-external-dns"
  policy = data.aws_iam_policy_document.external_dns_policy.json
}

resource "aws_iam_role_policy_attachment" "external_dns" {
  role       = aws_iam_role.external_dns.name
  policy_arn = aws_iam_policy.external_dns.arn
}

output "alb_controller_role_arn" {
  description = "IAM role ARN for ALB controller"
  value       = aws_iam_role.alb_controller.arn
}

output "external_dns_role_arn" {
  description = "IAM role ARN for ExternalDNS"
  value       = aws_iam_role.external_dns.arn
}

output "alb_controller_service_account" {
  description = "Service account name for ALB controller"
  value       = local.alb_controller_sa
}


output "external_dns_service_account" {
  description = "Service account name for ExternalDNS"
  value       = local.external_dns_sa
}

# Note: Observability stack (Prometheus, Grafana, Tempo, OTEL Collector) is deployed
# via shell script (k8s-deploy-observability.sh) rather than Terraform.
# This avoids the chicken-and-egg problem with Kubernetes/Helm providers requiring
# cluster credentials during initial cluster creation.
