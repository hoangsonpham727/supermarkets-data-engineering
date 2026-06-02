output "raw_bucket" {
  description = "Raw-zone S3 bucket name"
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_s3_uri" {
  description = "Use this as databricks.yml var.source_root (append /raw)"
  value       = "s3://${aws_s3_bucket.raw.bucket}/raw"
}

output "uc_role_arn" {
  description = "IAM role ARN to paste into the Databricks UC storage credential"
  value       = aws_iam_role.uc.arn
}
