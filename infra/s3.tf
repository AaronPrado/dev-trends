data "aws_caller_identity" "current" {}

locals {
  # Los nombres de bucket de S3 son únicos a nivel GLOBAL (no por cuenta ni región):
  # añadir el id de cuenta evita colisiones con buckets de terceros.
  medallion_bucket_name = "${var.project_name}-medallion-${data.aws_caller_identity.current.account_id}"
  athena_results_name   = "${var.project_name}-athena-results-${data.aws_caller_identity.current.account_id}"
}

# ---------- Bucket medallion (bronze / silver / gold como prefijos) ----------

resource "aws_s3_bucket" "medallion" {
  bucket = local.medallion_bucket_name
}

resource "aws_s3_bucket_public_access_block" "medallion" {
  bucket = aws_s3_bucket.medallion.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "medallion" {
  bucket = aws_s3_bucket.medallion.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "medallion" {
  bucket = aws_s3_bucket.medallion.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "medallion" {
  bucket = aws_s3_bucket.medallion.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  depends_on = [aws_s3_bucket_versioning.medallion]
}

# ---------- Bucket de resultados de Athena ----------

resource "aws_s3_bucket" "athena_results" {
  bucket = local.athena_results_name
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    expiration {
      days = 14
    }
  }
}