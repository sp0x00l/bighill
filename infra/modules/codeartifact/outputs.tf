output "domain_name" {
  description = "CodeArtifact domain name"
  value       = aws_codeartifact_domain.this.domain
}

output "domain_arn" {
  description = "CodeArtifact domain ARN"
  value       = aws_codeartifact_domain.this.arn
}

output "repository_name" {
  description = "CodeArtifact repository name"
  value       = aws_codeartifact_repository.this.repository
}

output "repository_arn" {
  description = "CodeArtifact repository ARN"
  value       = aws_codeartifact_repository.this.arn
}

output "upload_policy_arn" {
  description = "ARN of the upload IAM policy"
  value       = aws_iam_policy.upload.arn
}

output "download_policy_arn" {
  description = "ARN of the download IAM policy"
  value       = aws_iam_policy.download.arn
}
