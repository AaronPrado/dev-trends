output "medallion_bucket" {
  description = "Nombre del bucket S3 del medallion (prefijos bronze/silver/gold)."
  value       = aws_s3_bucket.medallion.bucket
}

output "athena_results_bucket" {
  description = "Nombre del bucket S3 para resultados de consultas Athena."
  value       = aws_s3_bucket.athena_results.bucket
}