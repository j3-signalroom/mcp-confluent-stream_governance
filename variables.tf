variable "aws_region" {
    description = "The AWS Region."
    type        = string
}

variable "aws_access_key_id" {
    description = "The AWS Access Key ID."
    type        = string
    default     = ""
}

variable "aws_secret_access_key" {
    description = "The AWS Secret Access Key."
    type        = string
    default     = ""
}

variable "aws_session_token" {
    description = "The AWS Session Token."
    type        = string
    default     = ""
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for the ECS service."
  type        = list(string)
}

variable "vpc_id" {
  description = "The VPC ID where the ECS service will be deployed."
  type        = string
}
