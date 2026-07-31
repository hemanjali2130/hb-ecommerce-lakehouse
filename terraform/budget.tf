# hb-ecommerce-lakehouse — cost ceiling
# Owner: Hemanjali Buchireddy
#
# This budget was created with the AWS CLI BEFORE any billable resource existed,
# then imported into Terraform state so `terraform destroy` still removes it:
#
#   terraform import aws_budgets_budget.ceiling 904233128322:hb-lakehouse-ceiling
#
# Ordering matters and is enforced, not assumed: every billable resource in this
# project carries `depends_on = [aws_budgets_budget.ceiling]`, so a fresh
# `terraform apply` into an empty account cannot create a single chargeable
# resource before the ceiling is in place.
#
# Honest limitation: AWS Budgets evaluates ACTUAL spend and lags by several hours.
# It is a backstop, not a guard. The real guards are the Athena workgroup scan
# cutoff (instant, hard) and the generator being off by default.
#
# Scope note: this budget is account-wide, not tag-filtered. It therefore also
# counts the pre-existing `hemanjali-snowflake-retail-*` S3 spend (~$0.001/mo).

resource "aws_budgets_budget" "ceiling" {
  name         = "hb-lakehouse-ceiling"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_ceiling_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Warn early, warn again, then warn on trajectory rather than waiting for the
  # month to actually blow through the ceiling.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
