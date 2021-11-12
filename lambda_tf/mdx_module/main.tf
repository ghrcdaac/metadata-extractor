resource "aws_lambda_function" "mdx_lambda" {
  function_name    = "${var.prefix}-mdx_lambda"
  filename         = "../mdx_source.zip"
  handler          = "process_mdx.lambda_handler.handler"
  role             = var.cumulus_lambda_role_arn
  runtime          = "python3.8"
  timeout          = 120 # 2 minutes
  layers           = var.layers

  tags             = var.tags
  environment {
    variables = var.env_variables
  }

  vpc_config {
    subnet_ids         = var.lambda_subnet_ids
    security_group_ids = var.lambda_security_group_ids
  }
}
