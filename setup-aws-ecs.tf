# main.tf
resource "aws_ecs_cluster" "mcp_cluster" {
  name = "mcp-server-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_task_definition" "mcp_task" {
  family                   = "mcp-server-task"
  requires_compatibilities = ["FARGATE"]
  network_mode            = "awsvpc"
  cpu                     = "256"
  memory                  = "512"
  execution_role_arn      = aws_iam_role.ecs_execution_role.arn
  task_role_arn           = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "mcp-server"
      image     = "${aws_ecr_repository.mcp_repo.repository_url}:latest"
      essential = true
      
      portMappings = [
        {
          containerPort = 8080
          protocol      = "tcp"
        }
      ]

      environment = [
        {
          name  = "ENV"
          value = "production"
        }
      ]

      secrets = [
        {
          name      = "API_KEY"
          valueFrom = aws_secretsmanager_secret.mcp_api_key.arn
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mcp_logs.name
          "awslogs-region"        = "us-east-1"
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "mcp_service" {
  name            = "mcp-server-service"
  cluster         = aws_ecs_cluster.mcp_cluster.id
  task_definition = aws_ecs_task_definition.mcp_task.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.mcp_sg.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.mcp_tg.arn
    container_name   = "mcp-server"
    container_port   = 8080
  }
}

resource "aws_security_group" "mcp_sg" {
  name        = "mcp-server-sg"
  description = "Security group for MCP server"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]  # Adjust as needed
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_cloudwatch_log_group" "mcp_logs" {
  name              = "/ecs/mcp-server"
  retention_in_days = 7
}