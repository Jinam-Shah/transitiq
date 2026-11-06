variable "project" {
  description = "Prefix for every resource name."
  type        = string
  default     = "transitiq"
}

variable "environment" {
  description = "Environment name, part of resource names."
  type        = string
  default     = "local"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "use_localstack" {
  description = "Point the AWS provider at LocalStack instead of real AWS."
  type        = bool
  default     = true
}

variable "localstack_endpoint" {
  type    = string
  default = "http://localhost:4566"
}

variable "lambda_runtime" {
  type    = string
  default = "python3.12"
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "ingest_schedules" {
  description = "EventBridge schedule per ingest source (the event sent is {source = <key>})."
  type        = map(string)
  default = {
    heartbeat          = "rate(1 day)"
    mbta_v3_alerts     = "rate(30 minutes)"
    mbta_v3_facilities = "rate(1 day)"
  }
}

variable "mbta_api_key" {
  description = "MBTA V3 API key for the ingest Lambda. Pass with TF_VAR_mbta_api_key; empty = keyless (lower rate limit)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "quarantine_expiry_days" {
  description = "Rejected records are kept this long for inspection."
  type        = number
  default     = 90
}
