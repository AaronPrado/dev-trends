provider "aws" {
  region = var.aws_region

  # Etiqueta todos los recursos que soporten tags, para identificarlos y desglosar costes.
  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
      Env       = var.environment
    }
  }
}