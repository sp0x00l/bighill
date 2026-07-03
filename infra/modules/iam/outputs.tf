output "cluster_role_name" {
  description = "Name of IAM role for the EKS cluster"
  value       = aws_iam_role.eks_cluster.name
}

output "node_role_name" {
  description = "Name of IAM role for EKS managed node groups"
  value       = aws_iam_role.node_group.name
}

output "node_role_arn" {
  description = "ARN of IAM role for EKS managed node groups"
  value       = aws_iam_role.node_group.arn
}

output "cluster_role_arn" {
  description = "ARN of the EKS cluster IAM role"
  value       = aws_iam_role.eks_cluster.arn
}
