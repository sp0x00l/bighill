# BigHill Terraform

This Terraform stack provisions the AWS substrate for the BigHill ML platform:

- VPC, public/private subnets, NAT, and AWS VPC endpoints.
- EKS with standard ARM nodes and an optional GPU node group for KubeRay/vLLM.
- IRSA roles for services that need S3 object-store access and KMS signing.
- Aurora PostgreSQL and a Secrets Manager connection secret.
- Shared S3 object store for raw uploads, lakehouse snapshots, model artifacts, evaluations, and preference datasets.
- Optional Lambda API Gateway deployment using the built API Gateway artifacts.
- Optional CodeArtifact repository for native library artifacts.
- Route53 private DNS and optional public environment DNS/certificates.

Kubernetes add-ons, observability, and service Helm releases are still deployed by `infra/scripts/*`.
That keeps initial cluster creation separate from Helm/Kubernetes provider bootstrapping.

## Usage

From `infra/envs/platform`:

```sh
tofu init -backend-config=staging-account.hcl
tofu plan -var-file=staging.tfvars
tofu apply -var-file=staging.tfvars
```

Create the backend config files per account/environment. Do not copy Terraform state or `.terraform/`
directories from another repo.

## Account Inputs

Set these before applying a real environment:

- `cluster_admin_arns`
- `internal_zone_id` if ACM validation for `internal_domain` should be managed here
- `public_zone_id` or `create_public_zone`
- service endpoint hostnames if they differ from the defaults
- `deploy_api_gateway=true` only after `api_gateway/build/dist/api.zip` and `auth.zip` exist

After apply, use the `object_store_service_role_arns` and `profile_service_role_arn` outputs as Helm
service account annotations:

```yaml
serviceAccount:
  create: true
  name: data-ingestion-service
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/bighill-staging-data-ingestion-service
```
