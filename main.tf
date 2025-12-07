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
            version = "6.25.0"
        }
    }
}
