# IAM role assumed by Databricks Unity Catalog to read/write the raw bucket via a
# UC *storage credential* + *external location*. This is the documented
# cross-account trust pattern (Databricks AWS account + storage-credential
# external ID).

data "aws_iam_policy_document" "trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.databricks_aws_account_id}:root"]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.unity_catalog_external_id]
    }
  }

  # Self-assume statement required by Unity Catalog.
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.uc.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.unity_catalog_external_id]
    }
  }
}

resource "aws_iam_role" "uc" {
  name               = var.role_name
  assume_role_policy = data.aws_iam_policy_document.trust.json
}

data "aws_iam_policy_document" "access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "uc" {
  name   = "${var.role_name}-access"
  role   = aws_iam_role.uc.id
  policy = data.aws_iam_policy_document.access.json
}
