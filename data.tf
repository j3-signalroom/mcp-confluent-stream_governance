data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  cloud                           = "AWS"
  secrets_insert                  = "mcp_server"

  # Secrets Manager Paths
  confluent_secrets_path_prefix   = "/confluent_cloud_resource/${local.secrets_insert}"
  
  service_account_name            = "${local.secrets_insert}_flink_sql_statements_runner"
  flink_rest_endpoint             = "https://flink.${var.aws_region}.${lower(local.cloud)}.confluent.cloud"

  # IAM Role names and ARNs
  tableflow_s3_glue_role_name     = "tableflow_s3_glue_role"
  tableflow_s3_glue_role_arn      = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.tableflow_s3_glue_role_name}"

  # Tableflow Topics S3 Base Path
  part_before_v1                = split("/v1/", confluent_tableflow_topic.stock_trades.table_path)
  tableflow_topics_s3_base_path = "${local.part_before_v1[0]}/v1/"
}