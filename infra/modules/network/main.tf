resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name        = "bighill-${var.env_name}-vpc"
    Environment = var.env_name
  }
}

resource "aws_subnet" "public" {
  count                   = length(var.public_subnets)
  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.public_subnets[count.index]
  availability_zone       = var.azs[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name                                            = "bighill-${var.env_name}-public-${count.index}"
    Environment                                     = var.env_name
    "kubernetes.io/cluster/bighill-${var.env_name}" = "shared"
    "kubernetes.io/role/elb"                        = "1"
  }
}

resource "aws_subnet" "private" {
  count             = length(var.private_subnets)
  vpc_id            = aws_vpc.this.id
  cidr_block        = var.private_subnets[count.index]
  availability_zone = var.azs[count.index]

  tags = {
    Name                                            = "bighill-${var.env_name}-private-${count.index}"
    Environment                                     = var.env_name
    "kubernetes.io/cluster/bighill-${var.env_name}" = "shared"
    "kubernetes.io/role/internal-elb"               = "1"
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name        = "bighill-${var.env_name}-igw"
    Environment = var.env_name
  }
}

# Public route table: 0.0.0.0/0 -> IGW
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name        = "bighill-${var.env_name}-public-rt"
    Environment = var.env_name
  }
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# NAT gateway for private subnets
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name        = "bighill-${var.env_name}-nat-eip"
    Environment = var.env_name
  }
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  # Ensure IGW exists before NAT creation
  depends_on = [aws_internet_gateway.this]

  tags = {
    Name        = "bighill-${var.env_name}-nat-gw"
    Environment = var.env_name
  }
}

# Private route table: 0.0.0.0/0 -> NAT
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name        = "bighill-${var.env_name}-private-rt"
    Environment = var.env_name
  }
}

resource "aws_route" "private_nat_gateway" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this.id
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# Security group for interface VPC endpoints
resource "aws_security_group" "vpce" {
  name        = "bighill-${var.env_name}-vpce-sg"
  description = "Allow HTTPS from VPC to interface VPC endpoints"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "HTTPS from VPC CIDR"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.this.cidr_block]
  }

  # Allow the endpoint ENIs to talk out to AWS services
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "bighill-${var.env_name}-vpce-sg"
    Environment = var.env_name
  }
}

# S3 gateway endpoint (for Lambda/EKS pulls, logs, etc.)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"

  # Adds a route using the S3 prefix list into this route table
  route_table_ids = [
    aws_route_table.private.id,
  ]

  tags = {
    Name        = "bighill-${var.env_name}-s3-endpoint"
    Environment = var.env_name
  }
}

# ECR API endpoint (DescribeImages, GetAuthorizationToken, etc.)
resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.ecr.api"
  vpc_endpoint_type = "Interface"

  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpce.id]

  private_dns_enabled = true

  tags = {
    Name        = "bighill-${var.env_name}-ecr-api-endpoint"
    Environment = var.env_name
  }
}

# ECR DKR (Docker registry) endpoint (actual image pulls)
resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.ecr.dkr"
  vpc_endpoint_type = "Interface"

  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpce.id]

  private_dns_enabled = true

  tags = {
    Name        = "bighill-${var.env_name}-ecr-dkr-endpoint"
    Environment = var.env_name
  }
}

# STS endpoint (for IAM role assumption)
resource "aws_vpc_endpoint" "sts" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.sts"
  vpc_endpoint_type = "Interface"

  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpce.id]

  private_dns_enabled = true

  tags = {
    Name        = "bighill-${var.env_name}-sts-endpoint"
    Environment = var.env_name
  }
}

# CloudWatch Logs endpoint
resource "aws_vpc_endpoint" "logs" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.logs"
  vpc_endpoint_type = "Interface"

  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpce.id]

  private_dns_enabled = true

  tags = {
    Name        = "bighill-${var.env_name}-logs-endpoint"
    Environment = var.env_name
  }
}

# SSM endpoint (for Session Manager)
resource "aws_vpc_endpoint" "ssm" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.ssm"
  vpc_endpoint_type = "Interface"

  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpce.id]

  private_dns_enabled = true

  tags = {
    Name        = "bighill-${var.env_name}-ssm-endpoint"
    Environment = var.env_name
  }
}

# SSM Messages endpoint (for Session Manager)
resource "aws_vpc_endpoint" "ssmmessages" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.ssmmessages"
  vpc_endpoint_type = "Interface"

  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpce.id]

  private_dns_enabled = true

  tags = {
    Name        = "bighill-${var.env_name}-ssmmessages-endpoint"
    Environment = var.env_name
  }
}

# EC2 Messages endpoint (for Session Manager)
resource "aws_vpc_endpoint" "ec2messages" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.ec2messages"
  vpc_endpoint_type = "Interface"

  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpce.id]

  private_dns_enabled = true

  tags = {
    Name        = "bighill-${var.env_name}-ec2messages-endpoint"
    Environment = var.env_name
  }
}
