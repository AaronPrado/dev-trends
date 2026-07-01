#!/usr/bin/env bash
# Registra las tablas Delta del Gold en el catálogo Glue para consultarlas por Athena.
# Athena engine v3 lee Delta nativo: basta LOCATION + table_type=DELTA.
# Re-ejecutable: si una tabla ya está registrada, la deja tal cual (Athena no admite
# DROP TABLE sobre tablas Delta nativas, así que no se puede recrear con DROP+CREATE).
set -euo pipefail
export AWS_PAGER=""

: "${DEV_TRENDS_S3_BUCKET:?define DEV_TRENDS_S3_BUCKET (bucket medallion)}"
REGION="${AWS_REGION:-eu-west-1}"
WG="${DEV_TRENDS_ATHENA_WORKGROUP:-dev-trends-v1}"
DB="${DEV_TRENDS_GLUE_DB:-dev_trends}"
BUCKET="$DEV_TRENDS_S3_BUCKET"
TABLES="dim_technology dim_event_type dim_source dim_date fact_github_activity"

run() {  
  local qid st
  qid=$(aws athena start-query-execution --work-group "$WG" --region "$REGION" \
    --query-execution-context Database="$DB" --query-string "$1" \
    --query QueryExecutionId --output text)
  while :; do
    st=$(aws athena get-query-execution --region "$REGION" --query-execution-id "$qid" \
      --query 'QueryExecution.Status.State' --output text)
    case "$st" in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --region "$REGION" --query-execution-id "$qid" \
          --query 'QueryExecution.Status.StateChangeReason' --output text >&2
        return 1 ;;
    esac
    sleep 2
  done
}

for t in $TABLES; do
  echo "registrando $DB.$t ..."
  run "CREATE EXTERNAL TABLE IF NOT EXISTS $DB.$t LOCATION 's3://$BUCKET/gold/$t/' TBLPROPERTIES ('table_type'='DELTA')"
done
echo "OK: tablas registradas en Glue ($DB)."