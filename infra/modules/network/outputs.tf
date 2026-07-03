output "vpc_id" {
  description = "VPC ID for the EKS cluster and other resources"
  value       = aws_vpc.this.id
}

output "private_subnet_ids" {
  description = "Private subnets for EKS nodes"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnets for NLB / NAT / bastion"
  value       = aws_subnet.public[*].id
}

output "vpc_cidr_block" {
  description = "CIDR block for the VPC"
  value       = aws_vpc.this.cidr_block
}
