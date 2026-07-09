env_name = "staging"
region   = "eu-west-1"

# Add IAM users/roles that should administer the EKS cluster.
cluster_admin_arns = []

node_instance_types = ["m7g.2xlarge"]
node_min_size       = 2
node_max_size       = 4
node_desired_size   = 2

gpu_node_enabled        = true
gpu_node_instance_types = ["g5.xlarge"]
gpu_node_min_size       = 0
gpu_node_max_size       = 2
gpu_node_desired_size   = 0

internal_domain  = "internal.bighill.example"
internal_zone_id = ""

create_public_zone = false
public_zone_id     = ""
public_domain_root = "bighill.example"

data_registry_service_http_domain  = "data-registry.internal.bighill.example"
data_registry_service_http_port    = "80"
ingestion_service_http_domain      = "ingestion.internal.bighill.example"
ingestion_service_http_port        = "80"
model_registry_service_http_domain = "model-registry.internal.bighill.example"
model_registry_service_http_port   = "80"
tenant_service_http_domain        = "tenant.internal.bighill.example"
tenant_service_http_port          = "80"
training_service_http_domain       = "training.internal.bighill.example"
training_service_http_port         = "80"
inference_service_http_domain      = "inference.internal.bighill.example"
inference_service_http_port        = "80"

redis_host     = "redis.internal.bighill.example"
redis_port     = 6379
redis_tls      = "false"
redis_username = ""
redis_password = ""

deploy_api_gateway = true
