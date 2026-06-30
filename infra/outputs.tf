output "medallion_bucket" {
  description = "Nombre del bucket S3 del medallion (prefijos bronze/silver/gold)."
  value       = aws_s3_bucket.medallion.bucket
}

output "athena_results_bucket" {
  description = "Nombre del bucket S3 para resultados de consultas Athena."
  value       = aws_s3_bucket.athena_results.bucket
}

output "glue_database" {
  description = "Nombre de la base de datos del Glue Data Catalog."
  value       = aws_glue_catalog_database.gold.name
}

output "athena_workgroup" {
  description = "Nombre del workgroup de Athena."
  value       = aws_athena_workgroup.main.name
}