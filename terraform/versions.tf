# hb-ecommerce-lakehouse — provider and version pinning
# Owner: Hemanjali Buchireddy
#
# State is local (terraform.tfstate in this directory). A remote S3 backend would
# be the production choice, but it creates a bucket that `terraform destroy` cannot
# remove (the backend can't delete the bucket holding its own state). Local state
# keeps the "fully removable via terraform destroy" guarantee intact.

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }
  }
}
