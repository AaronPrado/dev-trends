locals {
  glue_database_name = replace(var.project_name, "-", "_")
}

resource "aws_glue_catalog_database" "gold" {
  name        = local.glue_database_name
  description = "Catálogo de las tablas consultables (capa Gold) del proyecto."
}