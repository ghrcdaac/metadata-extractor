terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

module "aws_lambda_function" {
  source                    = "./mdx_module"
  prefix                    = var.prefix
  region                    = var.region
  cumulus_lambda_role_arn   = var.cumulus_lambda_role_arn
  lambda_subnet_ids         = var.lambda_subnet_ids
  lambda_security_group_ids = var.lambda_security_group_ids
  env_variables             = var.env_variables
  layers                    = var.layers
  timeout                   = var.timeout
  tags                      = var.tags
}
