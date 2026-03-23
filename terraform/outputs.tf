# ─── FluxMQ Cluster Outputs ──────────────────────────────────────────────────

output "broker_public_ips" {
  description = "Public IP addresses of each broker (for client connections)"
  value       = aws_instance.broker[*].public_ip
}

output "broker_private_ips" {
  description = "Private IP addresses of each broker (for inter-broker communication)"
  value       = aws_instance.broker[*].private_ip
}

output "broker_endpoints" {
  description = "Broker protocol endpoints (host:port)"
  value       = [for ip in aws_instance.broker[*].public_ip : "${ip}:9092"]
}

output "metrics_endpoints" {
  description = "Prometheus metrics endpoints"
  value       = [for ip in aws_instance.broker[*].public_ip : "http://${ip}:9093/metrics"]
}

output "ssh_commands" {
  description = "SSH commands to connect to each broker"
  value       = [for i, ip in aws_instance.broker[*].public_ip : "ssh -i ~/.ssh/${var.ssh_key_name}.pem ubuntu@${ip}"]
}

output "efs_id" {
  description = "EFS file system ID (shared cluster directory)"
  value       = aws_efs_file_system.cluster.id
}
