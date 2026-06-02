variable "aws_region" {
  description = "AWS region for the raw-zone bucket"
  type        = string
  default     = "ap-southeast-2" # Sydney — matches the Australian retail data
}

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for the raw landing zone"
  type        = string
}

variable "databricks_aws_account_id" {
  description = "Databricks' AWS account ID that Unity Catalog assumes the role from (414351767826 for commercial)"
  type        = string
  default     = "414351767826"
}

variable "unity_catalog_external_id" {
  description = "External ID from the Databricks storage credential (set after creating the credential, or use the Databricks TF provider to wire it automatically)"
  type        = string
}

variable "role_name" {
  description = "Name of the IAM role Unity Catalog assumes"
  type        = string
  default     = "databricks-uc-supermarket-raw"
}
