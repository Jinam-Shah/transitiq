terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }

  # Local state: this stack only ever targets LocalStack (see docs/DECISIONS.md D-005).
  # On real AWS this would be an S3 backend with state locking.
}
