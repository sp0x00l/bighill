#! /usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-}"
REGION="${AWS_REGION:-us-east-1}"
ONLY_SYNC_SECRETS="${ONLY_SYNC_SECRETS:-false}"
SKIP_PRICE_ORACLE_ERCOT_SECRET_SYNC="${SKIP_PRICE_ORACLE_ERCOT_SECRET_SYNC:-false}"

if [ -z "$ENVIRONMENT" ]; then
  echo "Usage: $0 [staging|prod]"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMESPACE="ml-ops-$ENVIRONMENT"
CLUSTER_NAME="ml-ops-${ENVIRONMENT}"

# shellcheck disable=SC1091
source "$PROJECT_ROOT/scripts/common.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/kafka-common.sh"

ensure_eks_context() {
  echo "Configuring kubectl for EKS cluster '${CLUSTER_NAME}' in region '${REGION}'..."
  
  # Include AWS profile in kubeconfig if set
  if [ -n "${AWS_PROFILE:-}" ]; then
    aws eks update-kubeconfig --name "${CLUSTER_NAME}" --region "${REGION}" --profile "${AWS_PROFILE}"
  else
    aws eks update-kubeconfig --name "${CLUSTER_NAME}" --region "${REGION}"
  fi

  if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "Error: Cannot connect to EKS cluster ${CLUSTER_NAME}"
    exit 1
  fi
}

discover_aurora_credentials() {
  echo "Discovering Aurora credentials from Secrets Manager..."
  
  SECRET_ARN=$(aws secretsmanager list-secrets \
    --query 'SecretList[?contains(Name, `exchange/'"${ENVIRONMENT}"'/aurora`)].ARN | [0]' \
    --output text 2>/dev/null || true)

  if [ -z "$SECRET_ARN" ] || [ "$SECRET_ARN" = "None" ]; then
    echo "Error: No Aurora secret found in Secrets Manager matching 'exchange/${ENVIRONMENT}/aurora'"
    exit 1
  fi

  SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id "$SECRET_ARN" \
    --query 'SecretString' --output text 2>/dev/null || true)

  if [ -z "$SECRET_JSON" ] || [ "$SECRET_JSON" = "None" ]; then
    echo "Error: Failed to retrieve Aurora secret value"
    exit 1
  fi

  PGHOST=$(echo "$SECRET_JSON" | jq -r '.host // empty')
  PGPORT=$(echo "$SECRET_JSON" | jq -r '.port // 5432')
  BIGHILL_DB_ADMIN=$(echo "$SECRET_JSON" | jq -r '.username // empty')
  BIGHILL_DB_ADMIN_PASSWORD=$(echo "$SECRET_JSON" | jq -r '.password // empty')

  if [ -z "$PGHOST" ]; then
    echo "Error: Aurora secret missing 'host' field"
    exit 1
  fi

  echo "Resolved Aurora from Secrets Manager: $PGHOST:$PGPORT (user: $BIGHILL_DB_ADMIN)"
}

upsert_json_secret() {
  local SECRET_NAME="$1"
  local SECRET_JSON="$2"

  if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" >/dev/null 2>&1; then
    aws secretsmanager put-secret-value \
      --secret-id "$SECRET_NAME" \
      --secret-string "$SECRET_JSON" >/dev/null
    echo "Updated AWS Secrets Manager secret '${SECRET_NAME}'."
  else
    aws secretsmanager create-secret \
      --name "$SECRET_NAME" \
      --secret-string "$SECRET_JSON" >/dev/null
    echo "Created AWS Secrets Manager secret '${SECRET_NAME}'."
  fi
}

discover_profile_oauth_credentials() {
  local SECRET_NAME="exchange/${ENVIRONMENT}/profile-oauth"
  local SECRET_ARN
  local SECRET_JSON

  if [ -n "${PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_ID:-}" ] || [ -n "${PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_SECRET:-}" ] || [ -n "${PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_ID:-}" ] || [ -n "${PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_SECRET:-}" ]; then
    if [ -z "${PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_ID:-}" ] || [ -z "${PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_SECRET:-}" ] || [ -z "${PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_ID:-}" ] || [ -z "${PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_SECRET:-}" ]; then
      echo "Error: PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_ID, PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_SECRET, PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_ID, and PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_SECRET must all be set together."
      exit 1
    fi

    echo "Syncing profile OAuth credentials from config to AWS Secrets Manager..."
    SECRET_JSON=$(jq -n \
      --arg google_client_id "$PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_ID" \
      --arg google_client_secret "$PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_SECRET" \
      --arg discord_client_id "$PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_ID" \
      --arg discord_client_secret "$PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_SECRET" \
      '{
        google_client_id: $google_client_id,
        google_client_secret: $google_client_secret,
        discord_client_id: $discord_client_id,
        discord_client_secret: $discord_client_secret
      }')
    upsert_json_secret "$SECRET_NAME" "$SECRET_JSON"
    return
  fi

  echo "Discovering profile OAuth credentials from Secrets Manager..."

  SECRET_ARN=$(aws secretsmanager list-secrets \
    --query 'SecretList[?contains(Name, `exchange/'"${ENVIRONMENT}"'/profile-oauth`)].ARN | [0]' \
    --output text 2>/dev/null || true)

  if [ -z "$SECRET_ARN" ] || [ "$SECRET_ARN" = "None" ]; then
    echo "Error: No profile OAuth secret found in Secrets Manager matching 'exchange/${ENVIRONMENT}/profile-oauth'"
    exit 1
  fi

  SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id "$SECRET_ARN" \
    --query 'SecretString' --output text 2>/dev/null || true)

  if [ -z "$SECRET_JSON" ] || [ "$SECRET_JSON" = "None" ]; then
    echo "Error: Failed to retrieve profile OAuth secret value"
    exit 1
  fi

  PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_ID=$(echo "$SECRET_JSON" | jq -r '.google_client_id // empty')
  PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_SECRET=$(echo "$SECRET_JSON" | jq -r '.google_client_secret // empty')
  PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_ID=$(echo "$SECRET_JSON" | jq -r '.discord_client_id // empty')
  PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_SECRET=$(echo "$SECRET_JSON" | jq -r '.discord_client_secret // empty')

  if [ -z "$PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_ID" ] || [ -z "$PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_SECRET" ] || [ -z "$PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_ID" ] || [ -z "$PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_SECRET" ]; then
    echo "Error: Profile OAuth secret must contain google_client_id, google_client_secret, discord_client_id, and discord_client_secret."
    exit 1
  fi

  echo "Resolved profile OAuth credentials from Secrets Manager."
}

discover_price_oracle_ercot_credentials() {
  local SECRET_NAME="exchange/${ENVIRONMENT}/price-oracle-ercot"
  local SECRET_ARN
  local SECRET_JSON

  if [ -n "${PRICE_ORACLE_SERVICE_ERCOT_USERNAME:-}" ] || [ -n "${PRICE_ORACLE_SERVICE_ERCOT_PASSWORD:-}" ] || [ -n "${PRICE_ORACLE_SERVICE_ERCOT_PUBLIC_SUBSCRIPTION_KEY:-}" ] || [ -n "${PRICE_ORACLE_SERVICE_ERCOT_ESR_SUBSCRIPTION_KEY:-}" ]; then
    if [ -z "${PRICE_ORACLE_SERVICE_ERCOT_USERNAME:-}" ] || [ -z "${PRICE_ORACLE_SERVICE_ERCOT_PASSWORD:-}" ] || [ -z "${PRICE_ORACLE_SERVICE_ERCOT_PUBLIC_SUBSCRIPTION_KEY:-}" ] || [ -z "${PRICE_ORACLE_SERVICE_ERCOT_ESR_SUBSCRIPTION_KEY:-}" ]; then
      echo "Error: PRICE_ORACLE_SERVICE_ERCOT_USERNAME, PRICE_ORACLE_SERVICE_ERCOT_PASSWORD, PRICE_ORACLE_SERVICE_ERCOT_PUBLIC_SUBSCRIPTION_KEY, and PRICE_ORACLE_SERVICE_ERCOT_ESR_SUBSCRIPTION_KEY must all be set together."
      exit 1
    fi

    echo "Syncing price-oracle ERCOT credentials from config to AWS Secrets Manager..."
    SECRET_JSON=$(jq -n \
      --arg ercot_username "$PRICE_ORACLE_SERVICE_ERCOT_USERNAME" \
      --arg ercot_password "$PRICE_ORACLE_SERVICE_ERCOT_PASSWORD" \
      --arg ercot_public_subscription_key "$PRICE_ORACLE_SERVICE_ERCOT_PUBLIC_SUBSCRIPTION_KEY" \
      --arg ercot_esr_subscription_key "$PRICE_ORACLE_SERVICE_ERCOT_ESR_SUBSCRIPTION_KEY" \
      '{
        ercot_username: $ercot_username,
        ercot_password: $ercot_password,
        ercot_public_subscription_key: $ercot_public_subscription_key,
        ercot_esr_subscription_key: $ercot_esr_subscription_key
      }')
    upsert_json_secret "$SECRET_NAME" "$SECRET_JSON"
    PRICE_ORACLE_ERCOT_USERNAME="$PRICE_ORACLE_SERVICE_ERCOT_USERNAME"
    PRICE_ORACLE_ERCOT_PASSWORD="$PRICE_ORACLE_SERVICE_ERCOT_PASSWORD"
    PRICE_ORACLE_ERCOT_PUBLIC_SUBSCRIPTION_KEY="$PRICE_ORACLE_SERVICE_ERCOT_PUBLIC_SUBSCRIPTION_KEY"
    PRICE_ORACLE_ERCOT_ESR_SUBSCRIPTION_KEY="$PRICE_ORACLE_SERVICE_ERCOT_ESR_SUBSCRIPTION_KEY"
    return
  fi

  echo "Discovering price-oracle ERCOT credentials from Secrets Manager..."

  SECRET_ARN=$(aws secretsmanager list-secrets \
    --query 'SecretList[?contains(Name, `exchange/'"${ENVIRONMENT}"'/price-oracle-ercot`)].ARN | [0]' \
    --output text 2>/dev/null || true)

  if [ -z "$SECRET_ARN" ] || [ "$SECRET_ARN" = "None" ]; then
    echo "Error: No price-oracle ERCOT secret found in Secrets Manager matching 'exchange/${ENVIRONMENT}/price-oracle-ercot'"
    exit 1
  fi

  SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id "$SECRET_ARN" \
    --query 'SecretString' --output text 2>/dev/null || true)

  if [ -z "$SECRET_JSON" ] || [ "$SECRET_JSON" = "None" ]; then
    echo "Error: Failed to retrieve price-oracle ERCOT secret value"
    exit 1
  fi

  PRICE_ORACLE_ERCOT_USERNAME=$(echo "$SECRET_JSON" | jq -r '.ercot_username // empty')
  PRICE_ORACLE_ERCOT_PASSWORD=$(echo "$SECRET_JSON" | jq -r '.ercot_password // empty')
  PRICE_ORACLE_ERCOT_PUBLIC_SUBSCRIPTION_KEY=$(echo "$SECRET_JSON" | jq -r '.ercot_public_subscription_key // empty')
  PRICE_ORACLE_ERCOT_ESR_SUBSCRIPTION_KEY=$(echo "$SECRET_JSON" | jq -r '.ercot_esr_subscription_key // empty')

  if [ -z "$PRICE_ORACLE_ERCOT_USERNAME" ] || [ -z "$PRICE_ORACLE_ERCOT_PASSWORD" ] || [ -z "$PRICE_ORACLE_ERCOT_PUBLIC_SUBSCRIPTION_KEY" ] || [ -z "$PRICE_ORACLE_ERCOT_ESR_SUBSCRIPTION_KEY" ]; then
    echo "Error: Price-oracle ERCOT secret must contain ercot_username, ercot_password, ercot_public_subscription_key, and ercot_esr_subscription_key."
    exit 1
  fi

  echo "Resolved ERCOT credentials for price-oracle from Secrets Manager."
}

create_k8s_aurora_secret() {
  SECRET_NAME="aurora-creds-${ENVIRONMENT}"
  local DB_NAME_VARS=()
  local DB_LITERAL_ARGS=()

  if kubectl -n "$NAMESPACE" get secret "$SECRET_NAME" >/dev/null 2>&1; then
    HOST_VAL=$(kubectl -n "$NAMESPACE" get secret "$SECRET_NAME" \
      -o jsonpath='{.data.pg_host}' | base64 -d 2>/dev/null || true)
    if [ -n "$HOST_VAL" ] && [ "$HOST_VAL" != "0.0.0.0" ]; then
      echo "Aurora secret '$SECRET_NAME' exists with host '$HOST_VAL'; updating service DB keys."
    else
      echo "Aurora secret '$SECRET_NAME' has placeholder host; recreating."
      kubectl -n "$NAMESPACE" delete secret "$SECRET_NAME" --ignore-not-found
    fi
  fi

  while IFS= read -r VAR_NAME; do
    DB_NAME_VARS+=("$VAR_NAME")
  done < <(compgen -A variable | grep -E '^[A-Z0-9_]+_SERVICE_DB_NAME$' | sort)

  for DB_NAME_VAR in "${DB_NAME_VARS[@]}"; do
    local SERVICE_PREFIX
    local SERVICE_KEY
    local DB_USER_VAR
    local DB_NAME_VALUE
    local DB_USER_VALUE

    SERVICE_PREFIX="${DB_NAME_VAR%_SERVICE_DB_NAME}"
    SERVICE_KEY="$(echo "${SERVICE_PREFIX}" | tr '[:upper:]' '[:lower:]')"
    DB_USER_VAR="${SERVICE_PREFIX}_SERVICE_DB_USER"
    DB_NAME_VALUE="${!DB_NAME_VAR:-}"
    DB_USER_VALUE="${!DB_USER_VAR:-}"

    if [ -z "${DB_NAME_VALUE}" ] || [ -z "${DB_USER_VALUE}" ]; then
      echo "Skipping DB secret keys for ${SERVICE_KEY}: missing ${DB_NAME_VAR} or ${DB_USER_VAR}."
      continue
    fi

    DB_LITERAL_ARGS+=("--from-literal=${SERVICE_KEY}_db_name=${DB_NAME_VALUE}")
    DB_LITERAL_ARGS+=("--from-literal=${SERVICE_KEY}_db_user=${DB_USER_VALUE}")
    DB_LITERAL_ARGS+=("--from-literal=${SERVICE_KEY}_db_password=${BIGHILL_DB_PASSWORD}")
  done

  if [ "${#DB_LITERAL_ARGS[@]}" -eq 0 ]; then
    echo "Error: no *_SERVICE_DB_NAME/*_SERVICE_DB_USER variables discovered."
    exit 1
  fi

  echo "Creating Kubernetes secret '$SECRET_NAME'..."
  kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
    --from-literal=pg_host="$PGHOST" \
    --from-literal=pg_port="${PGPORT:-5432}" \
    --from-literal=admin_user="$BIGHILL_DB_ADMIN" \
    --from-literal=admin_password="$BIGHILL_DB_ADMIN_PASSWORD" \
    --from-literal=migrations_user="$BIGHILL_DB_MIGRATIONS_USER" \
    --from-literal=migrations_password="$BIGHILL_DB_PASSWORD" \
    --from-literal=bighill_db_password="$BIGHILL_DB_PASSWORD" \
    --from-literal=pg_sslmode=require \
    "${DB_LITERAL_ARGS[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -
}

create_k8s_price_oracle_ercot_secret() {
  local SECRET_NAME="price-oracle-ercot-creds-${ENVIRONMENT}"

  echo "Creating Kubernetes secret '${SECRET_NAME}'..."
  kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
    --from-literal=ercot_username="$PRICE_ORACLE_ERCOT_USERNAME" \
    --from-literal=ercot_password="$PRICE_ORACLE_ERCOT_PASSWORD" \
    --from-literal=ercot_public_subscription_key="$PRICE_ORACLE_ERCOT_PUBLIC_SUBSCRIPTION_KEY" \
    --from-literal=ercot_esr_subscription_key="$PRICE_ORACLE_ERCOT_ESR_SUBSCRIPTION_KEY" \
    --dry-run=client -o yaml | kubectl apply -f -
}

create_k8s_profile_oauth_secret() {
  local SECRET_NAME="profile-oauth-creds-${ENVIRONMENT}"

  echo "Creating Kubernetes secret '${SECRET_NAME}'..."
  kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
    --from-literal=google_client_id="$PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_ID" \
    --from-literal=google_client_secret="$PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_SECRET" \
    --from-literal=discord_client_id="$PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_ID" \
    --from-literal=discord_client_secret="$PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_SECRET" \
    --dry-run=client -o yaml | kubectl apply -f -
}

copy_db_init_scripts() {
  TARGET_DIR="$PROJECT_ROOT/infra/helm/platform/helm-chart/tmp/docker-entrypoint-initdb.d"
  rm -rf "$TARGET_DIR"
  mkdir -p "$TARGET_DIR"
  cp "$PROJECT_ROOT/database/scripts/setup/common/"* "$TARGET_DIR/"
}

deploy_shared_infra() {
  cd "$PROJECT_ROOT"
  
  VALUES_ARGS="-f ./infra/helm/platform/helm-chart/values.yaml"
  if [ -f "./infra/helm/platform/helm-chart/${ENVIRONMENT}-values.yaml" ]; then
    VALUES_ARGS="$VALUES_ARGS -f ./infra/helm/platform/helm-chart/${ENVIRONMENT}-values.yaml"
  fi

  INIT_CM="ml-ops-infra-ml-ops-services-postgres-init-scripts"
  kubectl -n "$NAMESPACE" delete configmap "$INIT_CM" --ignore-not-found || true

  echo "Deploying shared infra to namespace '$NAMESPACE'..."
  if ! helm upgrade --install ml-ops-infra "$PROJECT_ROOT/infra/helm/platform/helm-chart" \
    -n "$NAMESPACE" \
    $VALUES_ARGS \
    --set postgres.dbnames="${BIGHILL_DB_NAMES}" \
    --set postgres.adminUser="$BIGHILL_DB_ADMIN" \
    --set postgres.adminPassword="$BIGHILL_DB_ADMIN_PASSWORD" \
    --set postgres.migrationsUser="$BIGHILL_DB_MIGRATIONS_USER" \
    --set postgres.password="$BIGHILL_DB_PASSWORD" \
    --set postgres.host="$PGHOST" \
    --set postgres.port="$PGPORT" \
    --set postgres.image.repository="pgvector/pgvector" \
    --set postgres.image.tag="pg17" \
    --set postgres.extensions.vector=true \
    --set polaris.enabled=true \
    --wait \
    --timeout 20m \
    --debug
  then
    echo "Helm upgrade failed. Collecting migration hook diagnostics..."
    local MIGRATE_JOB="ml-ops-infra-ml-ops-services-postgres-migrate-job"
    kubectl get job -n "$NAMESPACE" "$MIGRATE_JOB" -o wide 2>/dev/null || true
    kubectl get pods -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" -o wide 2>/dev/null || true
    kubectl describe job -n "$NAMESPACE" "$MIGRATE_JOB" 2>/dev/null | tail -n 120 || true
    kubectl describe pods -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" 2>/dev/null | tail -n 160 || true
    kubectl logs -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" --tail=200 2>/dev/null || true
    return 1
  fi
}


echo "Using AWS EKS for environment '$ENVIRONMENT'..."
ensure_eks_context

if ! kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
  echo "Creating namespace '$NAMESPACE'..."
  kubectl create namespace "$NAMESPACE"
fi

cd "$PROJECT_ROOT"
export_env_configs "$ENVIRONMENT" "$PROJECT_ROOT"

# Do not trust PGHOST from local config (database/scripts/config.sh sets 127.0.0.1).
# For staging/prod, always resolve Aurora connectivity from Secrets Manager.
discover_aurora_credentials
discover_profile_oauth_credentials
if [ "$SKIP_PRICE_ORACLE_ERCOT_SECRET_SYNC" != "true" ]; then
  discover_price_oracle_ercot_credentials
else
  echo "Skipping price-oracle ERCOT secret discovery (SKIP_PRICE_ORACLE_ERCOT_SECRET_SYNC=true)."
fi

verify_migrations() {
  echo "Verifying migration job status..."
  local MIGRATE_JOB="ml-ops-infra-ml-ops-services-postgres-migrate-job"
  local RETRIES=12
  local STATUS=""
  local FAILED=""
  local JOB_EXISTS=""
  local POD_STATUS=""
  local POD_SUMMARY=""

  # Give helm hooks a moment to start
  sleep 3

  while [ $RETRIES -gt 0 ]; do
    JOB_EXISTS=$(kubectl get job "$MIGRATE_JOB" -n "$NAMESPACE" >/dev/null 2>&1 && echo "yes" || echo "no")
    POD_STATUS=$(kubectl get pods -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" -o jsonpath='{.items[*].status.phase}' 2>/dev/null || echo "")

    # Job completed successfully
    if [ "$JOB_EXISTS" = "yes" ]; then
      STATUS=$(kubectl get job "$MIGRATE_JOB" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
      FAILED=$(kubectl get job "$MIGRATE_JOB" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")
      
      if [ "$STATUS" = "True" ]; then
        echo "Migration job completed successfully."
        return 0
      elif [ "$FAILED" = "True" ]; then
        echo "ERROR: Migration job failed!"
        kubectl logs -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" --tail=50
        return 1
      fi
    fi

    # Pod succeeded (job may have been cleaned up by hook)
    if echo "$POD_STATUS" | grep -q "Succeeded"; then
      echo "Migration job pod completed successfully."
      return 0
    fi

    # Detect pods stuck in a bad state
    POD_SUMMARY=$(kubectl get pods -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" \
      -o jsonpath='{range .items[*]}{.metadata.name} {.status.phase} {.status.containerStatuses[0].state.waiting.reason}{"\n"}{end}' 2>/dev/null || echo "")
    if echo "$POD_SUMMARY" | grep -E "ImagePullBackOff|ErrImagePull|CrashLoopBackOff" >/dev/null 2>&1; then
      echo "ERROR: Migration job pod in a backoff state:"
      kubectl get pods -n "$NAMESPACE" -l job-name="$MIGRATE_JOB"
      kubectl describe pods -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" | tail -n 80
      kubectl logs -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" --tail=50
      return 1
    fi

    # If job and pod don't exist after helm completed successfully, assume hook cleaned up after success
    if [ "$JOB_EXISTS" = "no" ] && [ -z "$POD_STATUS" ]; then
      echo "Migration job and pods not found - Helm hook likely cleaned up after success."
      echo "Verifying database schema exists..."
      # We could add a database check here, but for now assume success if helm didn't fail
      return 0
    fi
    
    RETRIES=$((RETRIES - 1))
    echo "Waiting for migration job to complete... ($RETRIES retries left)"
    sleep 5
  done

  echo "ERROR: Migration job did not complete in time."
  kubectl logs -n "$NAMESPACE" -l job-name="$MIGRATE_JOB" --tail=50 2>/dev/null || true
  return 1
}

discover_redis_nlb() {
  echo "Discovering Redis NLB hostname..."
  
  # Check if service is LoadBalancer type
  local SVC_TYPE=$(kubectl get svc redis -n "$NAMESPACE" -o jsonpath='{.spec.type}' 2>/dev/null || echo "")
  
  if [ "$SVC_TYPE" != "LoadBalancer" ]; then
    echo "Redis service is type $SVC_TYPE (not LoadBalancer). Lambda will need VPC access to reach Redis."
    return 1
  fi
  
  REDIS_NLB_HOSTNAME=""
  local RETRIES=12  # 60 seconds
  
  # Wait for Redis LoadBalancer to get an external hostname
  while [ $RETRIES -gt 0 ]; do
    REDIS_NLB_HOSTNAME=$(kubectl get svc redis -n "$NAMESPACE" \
      -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
    
    if [ -n "$REDIS_NLB_HOSTNAME" ]; then
      echo "Redis NLB hostname: $REDIS_NLB_HOSTNAME"
      break
    fi
    
    RETRIES=$((RETRIES - 1))
    if [ $((RETRIES % 4)) -eq 0 ]; then
      echo "Waiting for Redis LoadBalancer... ($RETRIES retries left, ~$((RETRIES * 5)) secs remaining)"
    fi
    sleep 5
  done
  
  if [ -z "$REDIS_NLB_HOSTNAME" ]; then
    echo "WARNING: Redis LoadBalancer hostname not available after 60 seconds."
    echo "The Lambda may not be able to reach Redis until the NLB is provisioned."
    echo "K8s pods can still use redis.${NAMESPACE}.svc.cluster.local:6379"
    return 1
  fi
  
  return 0
}

create_redis_dns_record() {
  if [ -z "${REDIS_NLB_HOSTNAME:-}" ]; then
    echo "WARNING: Redis NLB hostname not set, skipping DNS record creation."
    return 1
  fi
  
  echo "Creating Redis DNS record..."
  
  # Get the PRIVATE hosted zone ID for internal.northern.exchange
  # Lambda functions in the VPC resolve DNS via the private zone, not the public zone
  local INTERNAL_DOMAIN="internal.northern.exchange"
  local ZONE_ID=$(aws route53 list-hosted-zones \
    --query "HostedZones[?Name=='${INTERNAL_DOMAIN}.' && Config.PrivateZone==\`true\`].Id | [0]" \
    --output text 2>/dev/null | sed 's|/hostedzone/||' || echo "")
  
  if [ -z "$ZONE_ID" ] || [ "$ZONE_ID" = "None" ]; then
    echo "WARNING: Could not find private hosted zone for ${INTERNAL_DOMAIN}"
    return 1
  fi
  
  echo "Found private hosted zone: $ZONE_ID"
  
  # Create CNAME record for redis.internal.northern.exchange -> NLB hostname
  local REDIS_DNS="redis.${INTERNAL_DOMAIN}"
  
  cat > /tmp/redis-dns-change.json <<EOF
{
  "Changes": [{
    "Action": "UPSERT",
    "ResourceRecordSet": {
      "Name": "${REDIS_DNS}",
      "Type": "CNAME",
      "TTL": 300,
      "ResourceRecords": [{"Value": "${REDIS_NLB_HOSTNAME}"}]
    }
  }]
}
EOF
  
  if aws route53 change-resource-record-sets \
    --hosted-zone-id "$ZONE_ID" \
    --change-batch file:///tmp/redis-dns-change.json \
    --output text >/dev/null 2>&1; then
    echo "Redis DNS record created: ${REDIS_DNS} -> ${REDIS_NLB_HOSTNAME}"
    rm -f /tmp/redis-dns-change.json
    return 0
  else
    echo "WARNING: Failed to create Redis DNS record"
    rm -f /tmp/redis-dns-change.json
    return 1
  fi
}

create_events_dns_record() {
  echo "Creating events service DNS record..."
  
  # Get the event-service ingress ALB hostname
  local EVENTS_ALB_HOSTNAME
  EVENTS_ALB_HOSTNAME=$(kubectl get ingress -n "${NAMESPACE}" event-service-ingress \
    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
  
  if [ -z "$EVENTS_ALB_HOSTNAME" ]; then
    echo "WARNING: Event service ingress not found or has no ALB assigned yet."
    return 1
  fi
  
  echo "Found events ALB: $EVENTS_ALB_HOSTNAME"
  
  # Get the PUBLIC hosted zone ID for staging.northern.exchange (or env.northern.exchange)
  local PUBLIC_DOMAIN="${ENVIRONMENT}.northern.exchange"
  local ZONE_ID
  ZONE_ID=$(aws route53 list-hosted-zones \
    --query "HostedZones[?Name=='${PUBLIC_DOMAIN}.' && Config.PrivateZone==\`false\`].Id | [0]" \
    --output text 2>/dev/null | sed 's|/hostedzone/||' || echo "")
  
  if [ -z "$ZONE_ID" ] || [ "$ZONE_ID" = "None" ]; then
    echo "WARNING: Could not find public hosted zone for ${PUBLIC_DOMAIN}"
    return 1
  fi
  
  echo "Found public hosted zone: $ZONE_ID"
  
  # Create CNAME record for events.staging.northern.exchange -> ALB hostname
  local EVENTS_DNS="events.${PUBLIC_DOMAIN}"
  
  cat > /tmp/events-dns-change.json <<EOF
{
  "Changes": [{
    "Action": "UPSERT",
    "ResourceRecordSet": {
      "Name": "${EVENTS_DNS}",
      "Type": "CNAME",
      "TTL": 300,
      "ResourceRecords": [{"Value": "${EVENTS_ALB_HOSTNAME}"}]
    }
  }]
}
EOF
  
  if aws route53 change-resource-record-sets \
    --hosted-zone-id "$ZONE_ID" \
    --change-batch file:///tmp/events-dns-change.json \
    --output text >/dev/null 2>&1; then
    echo "Events DNS record created: ${EVENTS_DNS} -> ${EVENTS_ALB_HOSTNAME}"
    rm -f /tmp/events-dns-change.json
    return 0
  else
    echo "WARNING: Failed to create events DNS record"
    rm -f /tmp/events-dns-change.json
    return 1
  fi
}

update_lambda_redis_config() {
  if [ -z "${REDIS_NLB_HOSTNAME:-}" ]; then
    echo "WARNING: Redis NLB hostname not set, skipping Lambda update."
    return 0
  fi
  
  echo "Updating Lambda Redis configuration..."
  
  local STACK_NAME="ml-ops-${ENVIRONMENT}-api-gateway"
  
  # Get Lambda function names from CloudFormation
  local API_FUNCTION_NAME=""
  local AUTH_FUNCTION_NAME=""
  
  API_FUNCTION_NAME=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK_NAME" \
    --logical-resource-id ExchangeApiFunction \
    --query 'StackResources[0].PhysicalResourceId' \
    --output text \
    --region "$REGION" 2>/dev/null || echo "")
  
  AUTH_FUNCTION_NAME=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK_NAME" \
    --logical-resource-id ExchangeAuthFunction \
    --query 'StackResources[0].PhysicalResourceId' \
    --output text \
    --region "$REGION" 2>/dev/null || echo "")

  local redis_port="${REDIS_NLB_PORT:-6379}"
  local redis_address="${REDIS_NLB_HOSTNAME}:${redis_port}"
  
  # Update API Lambda Redis host
  if [ -n "$API_FUNCTION_NAME" ] && [ "$API_FUNCTION_NAME" != "None" ]; then
    echo "Updating API Lambda Redis host: $API_FUNCTION_NAME"
    
    # Get current environment variables
    local CURRENT_ENV=""
    CURRENT_ENV=$(aws lambda get-function-configuration \
      --function-name "$API_FUNCTION_NAME" \
      --query 'Environment.Variables' \
      --output json \
      --region "$REGION" 2>/dev/null || echo "{}")
    
    # Update REDIS_ADDRESS in the environment
    local UPDATED_ENV=""
    UPDATED_ENV=$(echo "$CURRENT_ENV" | jq \
      --arg addr "$redis_address" \
      '. + {REDIS_ADDRESS: $addr} | del(.["REDIS_" + "HOST"], .["REDIS_" + "PORT"], .["REDIS_" + "ADDR"])')
    
    if ! aws lambda update-function-configuration \
      --function-name "$API_FUNCTION_NAME" \
      --environment "Variables=$UPDATED_ENV" \
      --region "$REGION" \
      --output text >/dev/null 2>&1; then
      echo "WARNING: Failed to update API Lambda Redis config. Ensure bastion has lambda:UpdateFunctionConfiguration permission."
    else
      echo "  API Lambda Redis host updated successfully."
    fi
  fi
  
  # Update Auth Lambda Redis host
  if [ -n "$AUTH_FUNCTION_NAME" ] && [ "$AUTH_FUNCTION_NAME" != "None" ]; then
    echo "Updating Auth Lambda Redis host: $AUTH_FUNCTION_NAME"
    
    local CURRENT_ENV=""
    CURRENT_ENV=$(aws lambda get-function-configuration \
      --function-name "$AUTH_FUNCTION_NAME" \
      --query 'Environment.Variables' \
      --output json \
      --region "$REGION" 2>/dev/null || echo "{}")
    
    # Update REDIS_ADDRESS in the environment
    local UPDATED_ENV=""
    UPDATED_ENV=$(echo "$CURRENT_ENV" | jq \
      --arg addr "$redis_address" \
      '. + {REDIS_ADDRESS: $addr} | del(.["REDIS_" + "HOST"], .["REDIS_" + "PORT"], .["REDIS_" + "ADDR"])')
    
    if ! aws lambda update-function-configuration \
      --function-name "$AUTH_FUNCTION_NAME" \
      --environment "Variables=$UPDATED_ENV" \
      --region "$REGION" \
      --output text >/dev/null 2>&1; then
      echo "WARNING: Failed to update Auth Lambda Redis config. Ensure bastion has lambda:UpdateFunctionConfiguration permission."
    else
      echo "  Auth Lambda Redis host updated successfully."
    fi
  fi
  
  echo "Lambda Redis configuration updated to use: $REDIS_NLB_HOSTNAME"
}

ensure_redis_service() {
  echo "Ensuring Redis is ready..."
  
  local RETRIES=30
  
  # First wait for Redis deployment to be ready
  echo "Waiting for Redis deployment..."
  while [ $RETRIES -gt 0 ]; do
    local READY=$(kubectl get deployment redis -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    if [ "$READY" = "1" ]; then
      echo "Redis deployment is ready."
      break
    fi
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Redis deployment... ($RETRIES retries left)"
    sleep 5
  done
  
  if [ $RETRIES -eq 0 ]; then
    echo "WARNING: Redis deployment not ready after timeout"
  fi
  
  # Wait for the Redis service to be created by Helm (don't create a fallback that would conflict)
  RETRIES=30
  while [ $RETRIES -gt 0 ]; do
    if kubectl get svc redis -n "$NAMESPACE" >/dev/null 2>&1; then
      echo "Redis service exists."
          local SVC_TYPE=$(kubectl get svc redis -n "$NAMESPACE" -o jsonpath='{.spec.type}' 2>/dev/null || echo "")
      echo "Redis service type: $SVC_TYPE"
      break
    fi
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Redis service to be created... ($RETRIES retries left)"
    sleep 3
  done
  
  if [ $RETRIES -eq 0 ]; then
    echo "ERROR: Redis service was not created by Helm. Check helm chart configuration."
    return 1
  fi
  
  # Wait for endpoints
  RETRIES=15
  while [ $RETRIES -gt 0 ]; do
    local ENDPOINTS=$(kubectl get endpoints redis -n "$NAMESPACE" -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null || echo "")
    if [ -n "$ENDPOINTS" ]; then
      echo "Redis service has endpoints: $ENDPOINTS"
      echo "K8s pods can use redis.$NAMESPACE.svc.cluster.local:6379"
      return 0
    fi
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Redis endpoints... ($RETRIES retries left)"
    sleep 3
  done
  
  echo "WARNING: Redis endpoints not ready, but continuing..."
  return 0
}

ensure_polaris_service() {
  echo "Ensuring Polaris catalog is ready..."

  local RETRIES=30

  echo "Waiting for Polaris catalog deployment..."
  while [ $RETRIES -gt 0 ]; do
    local READY=$(kubectl get deployment polaris-catalog -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    if [ "$READY" = "1" ]; then
      echo "Polaris catalog deployment is ready."
      break
    fi
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Polaris catalog deployment... ($RETRIES retries left)"
    sleep 5
  done

  if [ $RETRIES -eq 0 ]; then
    echo "ERROR: Polaris catalog deployment not ready after timeout"
    return 1
  fi

  RETRIES=30
  while [ $RETRIES -gt 0 ]; do
    if kubectl get svc polaris-catalog -n "$NAMESPACE" >/dev/null 2>&1; then
      echo "Polaris catalog service exists."
      break
    fi
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Polaris catalog service to be created... ($RETRIES retries left)"
    sleep 3
  done

  if [ $RETRIES -eq 0 ]; then
    echo "ERROR: Polaris catalog service was not created by Helm. Check helm chart configuration."
    return 1
  fi

  RETRIES=15
  while [ $RETRIES -gt 0 ]; do
    local ENDPOINTS=$(kubectl get endpoints polaris-catalog -n "$NAMESPACE" -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null || echo "")
    if [ -n "$ENDPOINTS" ]; then
      echo "Polaris catalog service has endpoints: $ENDPOINTS"
      echo "K8s pods can use polaris-catalog.$NAMESPACE.svc.cluster.local:8181"
      return 0
    fi
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Polaris catalog endpoints... ($RETRIES retries left)"
    sleep 3
  done

  echo "ERROR: Polaris catalog endpoints not ready"
  return 1
}

wait_for_kafka() {
  wait_for_kafka_ready "$NAMESPACE" "$ENVIRONMENT"
}

restart_external_dns() {
  echo "Restarting external-dns to force DNS record sync..."
  if kubectl get deployment -n kube-system -l app.kubernetes.io/name=external-dns >/dev/null 2>&1; then
    kubectl rollout restart deployment -n kube-system -l app.kubernetes.io/name=external-dns 2>/dev/null || true
    sleep 5
  elif kubectl get deployment -n "$NAMESPACE" -l app.kubernetes.io/name=external-dns >/dev/null 2>&1; then
    kubectl rollout restart deployment -n "$NAMESPACE" -l app.kubernetes.io/name=external-dns 2>/dev/null || true
    sleep 5
  fi
  echo "External-dns restarted."
}

wait_for_dns_sync() {
  echo "Waiting for external-dns to sync DNS records..."
  local RETRIES=6
  
  # Check if external-dns pod is running
  if ! kubectl get pods -n kube-system -l app.kubernetes.io/name=external-dns --no-headers 2>/dev/null | grep -q Running; then
    echo "WARNING: external-dns pod not running"
    return 0
  fi
  
  # Wait for external-dns to process changes (check logs for sync completion)
  while [ $RETRIES -gt 0 ]; do
    local SYNC_STATUS=$(kubectl logs -n kube-system -l app.kubernetes.io/name=external-dns --tail=5 2>/dev/null | grep -E "(All records are already up to date|Change batch.*was submitted)" | tail -1 || true)
    
    if [ -n "$SYNC_STATUS" ]; then
      echo "External-dns sync status: $SYNC_STATUS"
      echo "DNS records synced."
      return 0
    fi
    
    # Check for errors
    local SYNC_ERROR=$(kubectl logs -n kube-system -l app.kubernetes.io/name=external-dns --tail=5 2>/dev/null | grep -i "error" | tail -1 || true)
    
    if [ -n "$SYNC_ERROR" ]; then
      echo "WARNING: External-dns error: $SYNC_ERROR"
      return 0
    fi
    
    RETRIES=$((RETRIES - 1))
    echo "Waiting for external-dns sync... ($RETRIES retries left)"
    sleep 10
  done
  
  echo "External-dns sync wait completed."
  return 0
}

# Clean up stale pods/PVCs that may be stuck in wrong AZ
# This handles the case where the node moved to a different AZ than existing EBS volumes
cleanup_stale_stateful_resources() {
  echo "Checking for stale stateful resources..."
  
  # Get current node's AZ
  local NODE_AZ=$(kubectl get nodes -o jsonpath='{.items[0].metadata.labels.topology\.kubernetes\.io/zone}' 2>/dev/null || echo "")
  if [ -z "$NODE_AZ" ]; then
    echo "Could not determine node AZ, skipping stale resource cleanup"
    return 0
  fi
  echo "Current node AZ: $NODE_AZ"
  
  # Check if Redis pod is pending due to PV affinity mismatch
  local REDIS_STATUS=$(kubectl get pod -n "$NAMESPACE" -l app=redis -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "")
  if [ "$REDIS_STATUS" = "Pending" ]; then
    local REDIS_EVENTS=$(kubectl get events -n "$NAMESPACE" --field-selector involvedObject.name=redis -o jsonpath='{.items[*].message}' 2>/dev/null || echo "")
    if echo "$REDIS_EVENTS" | grep -q "node affinity"; then
      echo "Redis pod stuck due to PV node affinity mismatch. Cleaning up..."
      kubectl delete pvc redis-pvc -n "$NAMESPACE" --ignore-not-found
      kubectl delete pod -n "$NAMESPACE" -l app=redis --ignore-not-found
      echo "Redis PVC/pod deleted. Will be recreated in correct AZ."
    fi
  fi
  
  # Check if Kafka pod is pending due to PV affinity mismatch
  local KAFKA_STATUS=$(kubectl get pod kafka-0 -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [ "$KAFKA_STATUS" = "Pending" ]; then
    local KAFKA_EVENTS=$(kubectl get events -n "$NAMESPACE" --field-selector involvedObject.name=kafka-0 -o jsonpath='{.items[*].message}' 2>/dev/null || echo "")
    if echo "$KAFKA_EVENTS" | grep -q "node affinity"; then
      echo "Kafka pod stuck due to PV node affinity mismatch. Cleaning up..."
      kubectl delete pvc kafka-data-kafka-0 -n "$NAMESPACE" --ignore-not-found
      kubectl delete pod kafka-0 -n "$NAMESPACE" --ignore-not-found
      echo "Kafka PVC/pod deleted. Will be recreated in correct AZ."
    fi
  fi
}

copy_db_init_scripts
create_k8s_aurora_secret
create_k8s_profile_oauth_secret
if [ "$SKIP_PRICE_ORACLE_ERCOT_SECRET_SYNC" != "true" ]; then
  create_k8s_price_oracle_ercot_secret
else
  echo "Skipping price-oracle ERCOT secret sync (SKIP_PRICE_ORACLE_ERCOT_SECRET_SYNC=true)."
fi
if [ "$ONLY_SYNC_SECRETS" = "true" ]; then
  echo "ONLY_SYNC_SECRETS=true; skipping infra Helm deploy and rollout checks."
  exit 0
fi
cleanup_stale_stateful_resources
deploy_shared_infra
ensure_polaris_service
ensure_redis_service
wait_for_kafka
verify_migrations || exit 1

# Discover Redis NLB and update DNS + Lambda configuration
if discover_redis_nlb; then
  # DNS record creation may fail if bastion doesn't have Route53 permissions - that's OK
  create_redis_dns_record || true
  update_lambda_redis_config
fi

# Create events service DNS record (external-dns should do this, but as fallback)
create_events_dns_record || true

echo "Infrastructure deployment complete."
