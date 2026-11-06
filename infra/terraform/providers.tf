# When use_localstack = true (the default), every AWS API call goes to LocalStack with fake
# credentials. Setting it to false would target real AWS with your normal credentials; this
# project never does that (it must stay $0).

locals {
  ls = var.use_localstack ? var.localstack_endpoint : null
}

provider "aws" {
  region = var.aws_region

  access_key = var.use_localstack ? "test" : null
  secret_key = var.use_localstack ? "test" : null

  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack
  s3_use_path_style           = var.use_localstack

  endpoints {
    s3     = local.ls
    lambda = local.ls
    iam    = local.ls
    sts    = local.ls
    sqs    = local.ls
    events = local.ls
    logs   = local.ls
  }

  default_tags {
    tags = {
      project     = var.project
      environment = var.environment
      managed_by  = "terraform"
    }
  }
}
