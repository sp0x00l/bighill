variable "domain_name" {
  description = "CodeArtifact domain name"
  type        = string
}

variable "repository_name" {
  description = "CodeArtifact repository name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
