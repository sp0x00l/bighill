#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-}"
REGION="${AWS_REGION:-eu-west-1}"

if [ -z "$ENVIRONMENT" ]; then
  echo "Usage: $0 <staging|prod>"
  exit 1
fi

CLUSTER_NAME="ml-ops-${ENVIRONMENT}"
NAMESPACE="kube-system"
INTERNAL_ROOT_DOMAIN="${INTERNAL_ROOT_DOMAIN:-internal.bighill.example}"
PUBLIC_ROOT_DOMAIN="${PUBLIC_ROOT_DOMAIN:-bighill.example}"
INTERNAL_DOMAIN="${INTERNAL_ROOT_DOMAIN}"
PUBLIC_DOMAIN="${ENVIRONMENT}.${PUBLIC_ROOT_DOMAIN}"

configure_kubectl() {
  local CLUSTER="$1"
  local REGION_ARG="$2"

  echo "Configuring kubectl for EKS cluster '${CLUSTER}'..."
  if [ -n "${AWS_PROFILE:-}" ]; then
    aws eks update-kubeconfig --name "${CLUSTER}" --region "${REGION_ARG}" --profile "${AWS_PROFILE}"
  else
    aws eks update-kubeconfig --name "${CLUSTER}" --region "${REGION_ARG}"
  fi
}

fetch_role_arn() {
  local ROLE_NAME="$1"
  aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.Arn' --output text
}

get_vpc_id() {
  local CLUSTER="$1"
  aws eks describe-cluster --name "${CLUSTER}" --query 'cluster.resourcesVpcConfig.vpcId' --output text
}

find_zone() {
  local DOMAIN="$1"
  local PRIVATE_ZONE_ID=$(aws route53 list-hosted-zones --query "HostedZones[?Name=='${DOMAIN}.' && Config.PrivateZone==\`true\`].Id" --output text | sed 's|/hostedzone/||' | head -1)

  if [ -n "$PRIVATE_ZONE_ID" ] && [ "$PRIVATE_ZONE_ID" != "None" ]; then
    echo "$PRIVATE_ZONE_ID private"
    return
  fi

  local PUBLIC_ZONE_ID=$(aws route53 list-hosted-zones-by-name --dns-name "${DOMAIN}" --query "HostedZones[?Name=='${DOMAIN}.'].Id" --output text | sed 's|/hostedzone/||' | head -1)

  if [ -n "$PUBLIC_ZONE_ID" ] && [ "$PUBLIC_ZONE_ID" != "None" ]; then
    echo "$PUBLIC_ZONE_ID public"
  else
    echo " none"
  fi
}

install_alb_controller() {
  local CLUSTER="$1"
  local REGION_ARG="$2"
  local NAMESPACE_ARG="$3"
  local VPC_ID="$4"
  local ROLE_ARN="$5"

  echo "Installing AWS Load Balancer Controller..."
  helm repo add eks https://aws.github.io/eks-charts 2>/dev/null || true
  helm repo update eks

  helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller \
    --namespace "${NAMESPACE_ARG}" \
    --set clusterName="${CLUSTER}" \
    --set region="${REGION_ARG}" \
    --set vpcId="${VPC_ID}" \
    --set serviceAccount.create=true \
    --set serviceAccount.name=aws-load-balancer-controller \
    --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="${ROLE_ARN}" \
    --set enableShield=false \
    --set enableWaf=false \
    --set enableWafv2=false
}

wait_for_alb_webhook() {
  local NAMESPACE_ARG="$1"
  echo "Waiting for ALB controller to be ready..."
  local RETRIES=12
  until kubectl get endpoints aws-load-balancer-webhook-service -n "${NAMESPACE_ARG}" -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null | grep -q .; do
    RETRIES=$((RETRIES - 1))
    if [ $RETRIES -eq 0 ]; then
      echo "Error: ALB controller webhook not ready after 60s"
      exit 1
    fi
    echo "Waiting for ALB controller webhook, ${RETRIES} attempts remaining..."
    sleep 5
  done
  echo "ALB controller webhook is ready."
}

install_external_dns() {
  local ENV_ARG="$1"
  local NAMESPACE_ARG="$2"
  local INTERNAL_DOMAIN_ARG="$3"
  local PUBLIC_DOMAIN_ARG="$4"
  local ROLE_ARN="$5"

  echo "Installing ExternalDNS for domains: ${INTERNAL_DOMAIN_ARG}, ${PUBLIC_DOMAIN_ARG}..."
  helm repo add external-dns https://kubernetes-sigs.github.io/external-dns 2>/dev/null || true
  helm repo update external-dns

  # Configure external-dns to watch both private and public zones
  helm upgrade --install external-dns external-dns/external-dns \
    --namespace "${NAMESPACE_ARG}" \
    --set provider=aws \
    --set policy=sync \
    --set registry=txt \
    --set txtOwnerId="ml-ops-${ENV_ARG}-external-dns" \
    --set "domainFilters[0]=${INTERNAL_DOMAIN_ARG}" \
    --set "domainFilters[1]=${PUBLIC_DOMAIN_ARG}" \
    --set "sources[0]=ingress" \
    --set "sources[1]=service" \
    --set serviceAccount.create=true \
    --set serviceAccount.name=external-dns \
    --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="${ROLE_ARN}" \
    --set aws.preferCNAME=true \
    --set logLevel=debug
}

verify_installations() {
  local NAMESPACE_ARG="$1"
  echo "Verifying installations..."
  kubectl get pods -n "${NAMESPACE_ARG}" -l app.kubernetes.io/name=aws-load-balancer-controller
  kubectl get pods -n "${NAMESPACE_ARG}" -l app.kubernetes.io/name=external-dns
}

create_ebs_storage_class() {
  echo "Creating EBS StorageClass with WaitForFirstConsumer..."
  
  # Check if StorageClass already exists
  if kubectl get storageclass ebs-sc >/dev/null 2>&1; then
    echo "StorageClass 'ebs-sc' already exists."
    return 0
  fi
  
  # Create StorageClass with WaitForFirstConsumer to avoid AZ mismatch issues
  # This delays PV creation until a pod is scheduled, ensuring the PV is created
  # in the same AZ as the node where the pod will run.
  cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ebs-sc
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
parameters:
  type: gp3
  encrypted: "true"
EOF
  
  echo "StorageClass 'ebs-sc' created successfully."
}

configure_kubectl "$CLUSTER_NAME" "$REGION"

create_ebs_storage_class

ALB_ROLE_ARN=$(fetch_role_arn "ml-ops-${ENVIRONMENT}-alb-controller")
DNS_ROLE_ARN=$(fetch_role_arn "ml-ops-${ENVIRONMENT}-external-dns")

if [ -z "$ALB_ROLE_ARN" ] || [ "$ALB_ROLE_ARN" = "None" ]; then
  echo "Error: ALB controller IAM role not found. Run Terraform first."
  exit 1
fi

if [ -z "$DNS_ROLE_ARN" ] || [ "$DNS_ROLE_ARN" = "None" ]; then
  echo "Error: ExternalDNS IAM role not found. Run Terraform first."
  exit 1
fi

VPC_ID=$(get_vpc_id "$CLUSTER_NAME")

# Verify at least one zone exists
INTERNAL_ZONE_INFO=$(find_zone "$INTERNAL_DOMAIN")
INTERNAL_ZONE_ID=$(echo "$INTERNAL_ZONE_INFO" | awk '{print $1}')

if [ -z "$INTERNAL_ZONE_ID" ] || [ "$INTERNAL_ZONE_ID" = "none" ]; then
  echo "Warning: Route53 zone for ${INTERNAL_DOMAIN} not found. ExternalDNS may not work for internal services."
fi

install_alb_controller "$CLUSTER_NAME" "$REGION" "$NAMESPACE" "$VPC_ID" "$ALB_ROLE_ARN"
wait_for_alb_webhook "$NAMESPACE"
install_external_dns "$ENVIRONMENT" "$NAMESPACE" "$INTERNAL_DOMAIN" "$PUBLIC_DOMAIN" "$DNS_ROLE_ARN"
verify_installations "$NAMESPACE"

echo "Done. ALB Controller and ExternalDNS installed."
echo "ExternalDNS is configured for domains: ${INTERNAL_DOMAIN}, ${PUBLIC_DOMAIN}"

# Install Scalar API docs if enabled
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "${SCRIPT_DIR}/k8s-deploy-scalar.sh" ]; then
  echo ""
  echo "Installing Scalar API documentation..."
  "${SCRIPT_DIR}/k8s-deploy-scalar.sh" "${ENVIRONMENT}"
fi
