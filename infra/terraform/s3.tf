locals {
  name_prefix = "${var.project}-${var.environment}"
  buckets = {
    bronze     = "${local.name_prefix}-bronze"
    silver     = "${local.name_prefix}-silver"
    quarantine = "${local.name_prefix}-quarantine"
  }
}

resource "aws_s3_bucket" "lake" {
  for_each = local.buckets

  bucket = each.value
  # Local only: lets `terraform destroy` remove buckets that still hold test files.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  for_each = aws_s3_bucket.lake

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  for_each = aws_s3_bucket.lake

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Bronze is immutable raw data: versioning keeps every write recoverable.
resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.lake["bronze"].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "quarantine" {
  bucket = aws_s3_bucket.lake["quarantine"].id

  rule {
    id     = "expire-rejected-records"
    status = "Enabled"
    filter {}
    expiration {
      days = var.quarantine_expiry_days
    }
  }
}

# New bronze files trigger validation.
resource "aws_s3_bucket_notification" "bronze_to_validate" {
  bucket = aws_s3_bucket.lake["bronze"].id

  lambda_function {
    lambda_function_arn = aws_lambda_function.validate.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".jsonl"
  }

  depends_on = [aws_lambda_permission.s3_invoke_validate]
}
