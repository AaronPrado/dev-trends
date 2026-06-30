variable "aws_region" {
  description = "Región de AWS donde se crea toda la infraestructura."
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Nombre del proyecto. Se usa en tags y como prefijo de nombres de recursos."
  type        = string
  default     = "dev-trends"
}

variable "environment" {
  description = "Entorno lógico"
  type        = string
  default     = "v1"
}