resource "aws_athena_workgroup" "main" {
  name        = "${var.project_name}-${var.environment}"
  description = "Consultas Athena sobre la capa Gold."

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false

    # Cancela cualquier query que escanee más de 1 GiB.
    bytes_scanned_cutoff_per_query = 1073741824

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  # Permite que `terraform destroy` borre el workgroup aunque tenga histórico de queries.
  force_destroy = true
}