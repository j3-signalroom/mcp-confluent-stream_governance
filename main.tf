terraform {
    cloud {
      organization = "signalroom"

        workspaces {
            name = "mcp-server-confluent-cloud"
        }
  }

  required_providers {
        aws = {
            source  = "hashicorp/aws"
            version = "6.22.0"
        }
        confluent = {
            source  = "confluentinc/confluent"
            version = "2.53.0"
        }
    }
}
