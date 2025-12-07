#!/bin/bash

#
# *** Script Syntax ***
# ./deploy.sh=<create | delete> --profile=<SSO_PROFILE_NAME>
#
#

# Check required command (create or delete) was supplied
case $1 in
  create)
    create_action=true;;
  delete)
    create_action=false;;
  *)
    echo
    echo "(Error Message 001)  You did not specify one of the commands: create | delete."
    echo
    echo "Usage:  Require all five arguments ---> `basename $0`=<create | delete> --profile=<SSO_PROFILE_NAME>"
    echo
    exit 85 # Common GNU/Linux Exit Code for 'Interrupted system call should be restarted'
    ;;
esac

# Default optional variables
day_count=30
debug=false

# Get the arguments passed by shift to remove the first word
# then iterate over the rest of the arguments
shift
for arg in "$@" # $@ sees arguments as separate words
do
    case $arg in
        *"--profile="*)
            AWS_PROFILE=$arg;;
    esac
done

# Check required --profile argument was supplied
if [ -z $AWS_PROFILE ]
then
    echo
    echo "(Error Message 002)  You did not include the proper use of the -- profile=<SSO_PROFILE_NAME> argument in the call."
    echo
    echo "Usage:  Require all five arguments ---> `basename $0 $1` --profile=<SSO_PROFILE_NAME>"
    echo
    exit 85 # Common GNU/Linux Exit Code for 'Interrupted system call should be restarted'
fi

# Get the AWS SSO credential variables that are used by the AWS CLI commands to authenicate
aws sso login $AWS_PROFILE
eval $(aws2-wrap $AWS_PROFILE --export)
export AWS_REGION=$(aws configure get region $AWS_PROFILE)

# Create terraform.tfvars file
printf "aws_region=\"${AWS_REGION}\"\
\naws_access_key_id=\"${AWS_ACCESS_KEY_ID}\"\
\naws_secret_access_key=\"${AWS_SECRET_ACCESS_KEY}\"\
\naws_session_token=\"${AWS_SESSION_TOKEN}\"" > terraform.tfvars

# Initialize the Terraform configuration
terraform init

if [ "$create_action" = true ]
then
    # Create/Update the Terraform configuration
    terraform plan -var-file=terraform.tfvars

    # Apply the Terraform configuration
    terraform apply -var-file=terraform.tfvars
else
    # Destroy the Terraform configuration
    terraform destroy -var-file=terraform.tfvars
fi