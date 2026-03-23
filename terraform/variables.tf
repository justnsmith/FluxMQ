# ─── FluxMQ Cluster Variables ─────────────────────────────────────────────────

variable "aws_region" {
  description = "AWS region to deploy the cluster in"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type for each broker"
  type        = string
  default     = "t3.medium"
}

variable "broker_count" {
  description = "Number of brokers in the cluster"
  type        = number
  default     = 3
}

variable "replication_factor" {
  description = "Default replication factor for new topics"
  type        = number
  default     = 3
}

variable "data_volume_size" {
  description = "Size (GB) of the EBS data volume per broker"
  type        = number
  default     = 20
}

variable "ssh_key_name" {
  description = "Name of the EC2 key pair for SSH access"
  type        = string
}

variable "allowed_ssh_cidr" {
  description = "CIDR block allowed to SSH into broker instances"
  type        = string
  default     = "0.0.0.0/0"
}

variable "allowed_client_cidr" {
  description = "CIDR block allowed to connect to the broker protocol port"
  type        = string
  default     = "0.0.0.0/0"
}
