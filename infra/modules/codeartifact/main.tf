# CodeArtifact domain and repository for storing pre-built binaries
# Used to cache C++ libraries (boost, bighill_lib) for CI/CD

resource "aws_codeartifact_domain" "this" {
  domain = var.domain_name

  tags = var.tags
}

resource "aws_codeartifact_repository" "this" {
  repository  = var.repository_name
  domain      = aws_codeartifact_domain.this.domain
  description = "Pre-built binaries for CI/CD (boost, bighill_lib)"

  tags = var.tags
}

# IAM policy for uploading artifacts (used by developers)
resource "aws_iam_policy" "upload" {
  name        = "${var.repository_name}-upload"
  description = "Allows uploading artifacts to CodeArtifact"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "codeartifact:GetAuthorizationToken",
          "codeartifact:GetRepositoryEndpoint",
          "codeartifact:PublishPackageVersion",
          "codeartifact:PutPackageMetadata"
        ]
        Resource = [
          aws_codeartifact_domain.this.arn,
          aws_codeartifact_repository.this.arn,
          "${aws_codeartifact_repository.this.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = "sts:GetServiceBearerToken"
        Resource = "*"
        Condition = {
          StringEquals = {
            "sts:AWSServiceName" = "codeartifact.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = var.tags
}

# IAM policy for downloading artifacts (used by CI/CD)
resource "aws_iam_policy" "download" {
  name        = "${var.repository_name}-download"
  description = "Allows downloading artifacts from CodeArtifact"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "codeartifact:GetAuthorizationToken",
          "codeartifact:GetRepositoryEndpoint",
          "codeartifact:ReadFromRepository",
          "codeartifact:GetPackageVersionAsset",
          "codeartifact:GetPackageVersionReadme",
          "codeartifact:ListPackages",
          "codeartifact:ListPackageVersions",
          "codeartifact:ListPackageVersionAssets"
        ]
        Resource = [
          aws_codeartifact_domain.this.arn,
          aws_codeartifact_repository.this.arn,
          "${aws_codeartifact_repository.this.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = "sts:GetServiceBearerToken"
        Resource = "*"
        Condition = {
          StringEquals = {
            "sts:AWSServiceName" = "codeartifact.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = var.tags
}
