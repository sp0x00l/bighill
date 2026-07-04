env_name = "prod"
region   = "us-east-1"

cluster_admin_arns = []

node_instance_types = ["m7g.2xlarge"]
node_min_size       = 2
node_max_size       = 6
node_desired_size   = 2

gpu_node_enabled        = true
gpu_node_instance_types = ["g5.xlarge"]
gpu_node_min_size       = 0
gpu_node_max_size       = 4
gpu_node_desired_size   = 0

aurora_deletion_protection = true
aurora_skip_final_snapshot = false

internal_domain  = "internal.bighill.example"
internal_zone_id = ""

create_public_zone = false
public_zone_id     = ""
public_domain_root = "bighill.example"

data_registry_service_http_domain  = "data-registry-service.internal.bighill.example"
data_registry_service_http_port    = "80"
ingestion_service_http_domain = "ingestion-service.internal.bighill.example"
ingestion_service_http_port   = "80"
profile_service_http_domain        = "profile-service.internal.bighill.example"
profile_service_http_port          = "80"

redis_host     = "redis.internal.bighill.example"
redis_port     = 6379
redis_tls      = "false"
redis_username = ""
redis_password = ""

deploy_api_gateway = false
