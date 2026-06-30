# Plantilla de variables. Copiar a terraform.tfvars para sobreescribir los defaults.
# Las credenciales se leen de ~/.aws o de variables de entorno.

aws_region                = "eu-west-1"
project_name              = "dev-trends"
environment               = "v1"
budget_notification_email = "tu-email@example.com"
monthly_budget_limit      = "5"