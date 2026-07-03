# KMS key for EKS secrets encryption
resource "aws_kms_key" "eks" {
  description             = "EKS secrets encryption key for bighill-${var.env_name}"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name        = "bighill-${var.env_name}-eks-secrets"
    Environment = var.env_name
  }
}

resource "aws_kms_alias" "eks" {
  name          = "alias/bighill-${var.env_name}-eks-secrets"
  target_key_id = aws_kms_key.eks.key_id
}

# KMS key for JWT signing (asymmetric RSA for sign/verify)
resource "aws_kms_key" "jwt_signing" {
  description              = "JWT signing key for bighill-${var.env_name}"
  deletion_window_in_days  = 7
  key_usage                = "SIGN_VERIFY"
  customer_master_key_spec = "RSA_2048"

  tags = {
    Name        = "bighill-${var.env_name}-jwt-signing"
    Environment = var.env_name
  }
}

resource "aws_kms_alias" "jwt_signing" {
  name          = "alias/bighill-${var.env_name}-jwt-signing"
  target_key_id = aws_kms_key.jwt_signing.key_id
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = "bighill-${var.env_name}"
  kubernetes_version = var.cluster_version

  endpoint_private_access      = var.endpoint_private_access
  endpoint_public_access       = var.endpoint_public_access
  endpoint_public_access_cidrs = var.endpoint_public_access ? var.endpoint_public_access_cidrs : null

  vpc_id     = var.vpc_id
  subnet_ids = var.private_subnet_ids

  create_iam_role = false
  iam_role_arn    = var.cluster_role_arn

  # Cluster admin principals are managed explicitly by the platform layer.
  # Deriving this from the current Terraform caller makes access drift between
  # SSO roles and IAM users depending on who applies.
  enable_cluster_creator_admin_permissions = false

  # Secrets encryption with KMS
  create_kms_key = false
  encryption_config = {
    provider_key_arn = aws_kms_key.eks.arn
    resources        = ["secrets"]
  }

  # Node groups: default ARM control/data plane plus optional GPU pool for
  # KubeRay training jobs and vLLM serving runtimes.
  eks_managed_node_groups = merge({
    default = {
      min_size     = var.node_min_size
      max_size     = var.node_max_size
      desired_size = var.node_desired_size

      instance_types = var.node_instance_types
      ami_type       = "AL2023_ARM_64_STANDARD"

      create_iam_role = false
      iam_role_arn    = var.node_role_arn
    }
    },
    var.gpu_node_enabled ? {
      gpu = {
        min_size     = var.gpu_node_min_size
        max_size     = var.gpu_node_max_size
        desired_size = var.gpu_node_desired_size

        instance_types = var.gpu_node_instance_types
        ami_type       = var.gpu_node_ami_type

        create_iam_role = false
        iam_role_arn    = var.node_role_arn

        labels = {
          workload = "gpu"
        }

        taints = {
          gpu = {
            key    = "nvidia.com/gpu"
            value  = "true"
            effect = "NO_SCHEDULE"
          }
        }
      }
    } : {}
  )

  tags = {
    Environment = var.env_name
    Terraform   = "true"
  }
}

locals {
  oidc_provider_host = element(
    split("oidc-provider/", module.eks.oidc_provider_arn),
    1
  )
  kubernetes_namespace = var.kubernetes_namespace != "" ? var.kubernetes_namespace : "ml-ops-${var.env_name}"
}

# IAM trust policy for aws-ebs-csi-driver service account
data "aws_iam_policy_document" "ebs_csi_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_host}:sub"
      values   = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
    }
  }
}

resource "aws_iam_role" "ebs_csi" {
  name               = "bighill-${var.env_name}-ebs-csi-driver"
  assume_role_policy = data.aws_iam_policy_document.ebs_csi_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}


resource "aws_eks_addon" "ebs_csi" {
  cluster_name             = module.eks.cluster_name
  addon_name               = "aws-ebs-csi-driver"
  service_account_role_arn = aws_iam_role.ebs_csi.arn

  depends_on = [
    aws_iam_role_policy_attachment.ebs_csi,
    aws_eks_addon.vpc_cni,    # networking first
    aws_eks_addon.kube_proxy, # kube-proxy before storage driver
  ]
}


# Core EKS add-ons – these create the kube-system pods
resource "aws_eks_addon" "vpc_cni" {
  cluster_name = module.eks.cluster_name
  addon_name   = "vpc-cni"
}

resource "aws_eks_addon" "kube_proxy" {
  cluster_name = module.eks.cluster_name
  addon_name   = "kube-proxy"
}

resource "aws_eks_addon" "coredns" {
  cluster_name = module.eks.cluster_name
  addon_name   = "coredns"
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "cluster_security_group_id" {
  description = "Security group ID of the EKS cluster (control plane ENIs)"
  value       = module.eks.cluster_security_group_id
}

output "cluster_certificate_authority_data" {
  description = "Cluster CA data (base64)"
  value       = module.eks.cluster_certificate_authority_data
}

# IRSA role for profile-service to access KMS for JWT signing
data "aws_iam_policy_document" "profile_service_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_host}:sub"
      values   = ["system:serviceaccount:${local.kubernetes_namespace}:profile-service"]
    }
  }
}

resource "aws_iam_role" "profile_service" {
  name               = "bighill-${var.env_name}-profile-service"
  assume_role_policy = data.aws_iam_policy_document.profile_service_assume_role.json
}

resource "aws_iam_policy" "profile_service_kms" {
  name        = "bighill-${var.env_name}-profile-service-kms"
  description = "Allow profile-service to sign JWTs with KMS"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Sign",
          "kms:GetPublicKey",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.jwt_signing.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "profile_service_kms" {
  role       = aws_iam_role.profile_service.name
  policy_arn = aws_iam_policy.profile_service_kms.arn
}

data "aws_iam_policy_document" "object_store_service_assume_role" {
  for_each = toset(var.object_store_service_accounts)

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_host}:sub"
      values   = ["system:serviceaccount:${local.kubernetes_namespace}:${each.value}"]
    }
  }
}

resource "aws_iam_role" "object_store_service" {
  for_each = toset(var.object_store_service_accounts)

  name               = "bighill-${var.env_name}-${each.value}"
  assume_role_policy = data.aws_iam_policy_document.object_store_service_assume_role[each.value].json
}

resource "aws_iam_policy" "object_store_access" {
  count = length(var.object_store_bucket_arns) > 0 ? 1 : 0

  name        = "bighill-${var.env_name}-object-store-access"
  description = "Allow ML services to read and write artifact, dataset, and snapshot objects"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [for arn in var.object_store_bucket_arns : "${arn}/*"]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = var.object_store_bucket_arns
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "object_store_access" {
  for_each = length(var.object_store_bucket_arns) > 0 ? aws_iam_role.object_store_service : {}

  role       = each.value.name
  policy_arn = aws_iam_policy.object_store_access[0].arn
}

output "node_security_group_id" {
  description = "Security group ID attached to worker nodes"
  value       = module.eks.node_security_group_id
}

output "profile_service_role_arn" {
  description = "IAM role ARN for profile-service IRSA"
  value       = aws_iam_role.profile_service.arn
}

output "object_store_service_role_arns" {
  description = "IAM role ARNs for object-store-enabled service accounts"
  value       = { for name, role in aws_iam_role.object_store_service : name => role.arn }
}

output "oidc_provider_arn" {
  description = "OIDC provider ARN for the EKS cluster"
  value       = module.eks.oidc_provider_arn
}

output "oidc_provider_host" {
  description = "OIDC provider host derived from ARN"
  value       = local.oidc_provider_host
}

output "jwt_signing_key_id" {
  description = "KMS key ID for JWT signing"
  value       = aws_kms_key.jwt_signing.key_id
}

output "jwt_signing_key_arn" {
  description = "KMS key ARN for JWT signing"
  value       = aws_kms_key.jwt_signing.arn
}
