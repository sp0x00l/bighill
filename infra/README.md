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

## Deployment Contract

The deploy path is:

1. `tofu apply` in `infra/envs/platform` provisions AWS substrate.
2. `infra/scripts/k8s-install-addons.sh <env>` installs the AWS Load Balancer Controller,
   ExternalDNS, NVIDIA device plugin, and KubeRay operator.
3. `infra/scripts/k8s-deploy-infra.sh <env>` deploys the shared platform Helm chart
   `bighill-platform` from `infra/helm/platform/helm-chart` as release `bighill-infra`.
4. `infra/scripts/k8s-deploy-services.sh <env>` deploys the service Helm charts.
5. `infra/scripts/k8s-deploy-gateway.sh <env>` builds/deploys the Lambda API Gateway stack when
   gateway artifacts are available.

| Area | Contract |
|------|----------|
| EKS admin access | `cluster_admin_arns` must contain at least one IAM principal for staging/prod before apply. Cluster creator admin access is disabled intentionally. |
| Images | Services and migrations use one ECR repository, `bighill/mlops`; the service name is encoded in the tag, for example `inference-service-0.0.1-staging`. |
| Shared substrate | The `bighill-platform` chart owns Redis, Kafka, Temporal, Polaris object store/catalog/bootstrap, TEI-compatible embedding/reranking endpoints, and Postgres bootstrap/migration jobs. Aurora itself is provisioned by Terraform. |
| Gateway backends | API Gateway routes to internal ALB hostnames for data registry, ingestion, profile, model registry, training, and inference. Model serving remains internal. |
| Object store access | Terraform outputs `object_store_service_role_arns`; the deploy script applies those roles to service accounts that read/write artifacts, datasets, snapshots, and models. |
| GPU workloads | Terraform creates the optional tainted GPU node group; add-ons install the NVIDIA device plugin and KubeRay operator. Training RayJobs and vLLM pods request `nvidia.com/gpu`, tolerate the taint, and select `workload=gpu`. |
| Prod values | Every service chart has `staging-values.yaml` and `prod-values.yaml`; the deploy script fails if either target file is missing. |

Run `make k8s-validate` before applying or deploying. It checks shell syntax, Terraform formatting,
Helm lint/render for staging and prod, service image tag conventions, and stale copied-service
references. `tofu validate` runs when `infra/envs/platform` has initialized modules; otherwise the
script prints a skip message and leaves validation to the initialized environment.

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
  name: ingestion-service
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/bighill-staging-ingestion-service
```
