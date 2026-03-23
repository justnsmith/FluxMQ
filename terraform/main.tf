# ─── FluxMQ 3-Broker Cluster on AWS ──────────────────────────────────────────
#
# Architecture:
#   - 1 VPC with a public subnet per AZ (one broker per AZ for fault tolerance)
#   - 3 EC2 instances running the FluxMQ Docker image
#   - 1 EFS file system mounted on all brokers for shared cluster coordination
#   - Security group allowing broker-to-broker, client, and metrics traffic
#
# Usage:
#   cd terraform/
#   terraform init
#   terraform plan -var="ssh_key_name=my-key"
#   terraform apply -var="ssh_key_name=my-key"
#   terraform destroy

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ─── Data Sources ────────────────────────────────────────────────────────────

data "aws_availability_zones" "available" {
  state = "available"
}

# Ubuntu 24.04 LTS AMI (same base as Dockerfile).
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ─── VPC ─────────────────────────────────────────────────────────────────────

resource "aws_vpc" "fluxmq" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "fluxmq-vpc" }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.fluxmq.id
  tags   = { Name = "fluxmq-igw" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.fluxmq.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }

  tags = { Name = "fluxmq-public-rt" }
}

resource "aws_subnet" "broker" {
  count                   = var.broker_count
  vpc_id                  = aws_vpc.fluxmq.id
  cidr_block              = cidrsubnet(aws_vpc.fluxmq.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index % length(data.aws_availability_zones.available.names)]
  map_public_ip_on_launch = true

  tags = { Name = "fluxmq-subnet-${count.index + 1}" }
}

resource "aws_route_table_association" "broker" {
  count          = var.broker_count
  subnet_id      = aws_subnet.broker[count.index].id
  route_table_id = aws_route_table.public.id
}

# ─── Security Group ──────────────────────────────────────────────────────────

resource "aws_security_group" "fluxmq" {
  name_prefix = "fluxmq-"
  description = "FluxMQ broker cluster"
  vpc_id      = aws_vpc.fluxmq.id

  # SSH
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  # Broker binary protocol (client access)
  ingress {
    description = "FluxMQ broker protocol"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.allowed_client_cidr]
  }

  # Metrics HTTP endpoint
  ingress {
    description = "FluxMQ Prometheus metrics"
    from_port   = 9093
    to_port     = 9093
    protocol    = "tcp"
    cidr_blocks = [var.allowed_client_cidr]
  }

  # Inter-broker communication (within VPC)
  ingress {
    description = "Inter-broker replication"
    from_port   = 9092
    to_port     = 9093
    protocol    = "tcp"
    self        = true
  }

  # EFS (NFS) within VPC
  ingress {
    description = "EFS / NFS"
    from_port   = 2049
    to_port     = 2049
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "fluxmq-sg" }
}

# ─── EFS (Shared Cluster Coordination Directory) ─────────────────────────────
#
# FluxMQ uses file-based coordination (flock) for broker registration and
# partition assignments.  EFS provides a POSIX-compatible shared filesystem
# that all brokers mount at /cluster.

resource "aws_efs_file_system" "cluster" {
  creation_token = "fluxmq-cluster"
  encrypted      = true

  tags = { Name = "fluxmq-cluster-efs" }
}

resource "aws_efs_mount_target" "cluster" {
  count           = var.broker_count
  file_system_id  = aws_efs_file_system.cluster.id
  subnet_id       = aws_subnet.broker[count.index].id
  security_groups = [aws_security_group.fluxmq.id]
}

# ─── EC2 Broker Instances ────────────────────────────────────────────────────

resource "aws_instance" "broker" {
  count = var.broker_count

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = var.ssh_key_name
  subnet_id              = aws_subnet.broker[count.index].id
  vpc_security_group_ids = [aws_security_group.fluxmq.id]

  root_block_device {
    volume_size = var.data_volume_size
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/user_data.sh.tpl", {
    broker_id          = count.index + 1
    broker_host        = "" # filled by instance private IP at boot
    replication_factor = var.replication_factor
    efs_id             = aws_efs_file_system.cluster.id
    aws_region         = var.aws_region
  })

  # Wait for EFS mount targets to be available before launching.
  depends_on = [aws_efs_mount_target.cluster]

  tags = {
    Name = "fluxmq-broker-${count.index + 1}"
    Role = "fluxmq-broker"
  }
}
