variable "prefix" {
  description = "Stack prefix for cumulus deployment"
  type        = string
}

variable "region" {
  description = "Region to deploy resource in if not global"
  type        = string
  default     = "us-west-2"
}

variable "cumulus_lambda_role_arn" {
  description = "Cumulus lambda role arn for default lambda permissions"
  type        = string
}

variable "lambda_subnet_ids" {
  description = "Subnet ids which should be associated with lambda"
  type        = list(string)
  default     = null
}

variable "lambda_security_group_ids" {
  description = "Security groups which should be associated with lambda"
  type        = list(string)
  default     = null
}

variable "env_variables" {
  description = "Environment variables which should be associated with lambda"
  type        = map(string)
  default     = {}
}

variable "layers" {
  description = "Lambda Layers to be assocaited with lambda"
  type        = list(string)
  default     = []
}

variable "timeout" {
  description = "Lambda function time-out"
  type        = number
  default     = 120
}

variable "tags" {
  description = "User defined tags to associate with lambda"
  type        = list(string)
  default     = []
}
