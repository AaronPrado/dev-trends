data "aws_iam_policy_document" "pipeline" {
  # Nivel bucket: listar y localizar los dos buckets.
  statement {
    sid     = "S3BucketLevel"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.medallion.arn,
      aws_s3_bucket.athena_results.arn,
    ]
  }

  # Nivel objeto: leer/escribir/borrar dentro de los buckets.
  statement {
    sid     = "S3ObjectLevel"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.medallion.arn}/*",
      "${aws_s3_bucket.athena_results.arn}/*",
    ]
  }

  # Catálogo Glue: leer y gestionar las tablas de la base de datos del proyecto.
  statement {
    sid    = "GlueCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:CreatePartition", "glue:BatchCreatePartition", "glue:BatchDeletePartition",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.gold.name}",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.gold.name}/*",
    ]
  }

  # Athena: ejecutar consultas dentro del workgroup del proyecto.
  statement {
    sid    = "AthenaQueries"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution", "athena:StopQueryExecution",
      "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.main.arn]
  }
}

resource "aws_iam_policy" "pipeline" {
  name        = "${var.project_name}-pipeline"
  description = "Permisos mínimos del pipeline local (Spark/dbt): S3 medallion, Glue y Athena."
  policy      = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_user" "pipeline" {
  name = "${var.project_name}-pipeline"
}

resource "aws_iam_user_policy_attachment" "pipeline" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}