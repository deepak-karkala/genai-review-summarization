variable "faithfulness_threshold" {
  description = "The minimum acceptable faithfulness score before triggering an alert."
  type        = number
  default     = 0.95
}

resource "aws_sns_topic" "alerts_topic" {
  name = "LLM-Summarizer-Alerts-Topic"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.alerts_topic.arn
  protocol  = "email"
  endpoint  = "oncall-ml-team@example.com"
}

resource "aws_cloudwatch_metric_alarm" "faithfulness_alarm" {
  alarm_name          = "High-Hallucination-Rate-Alarm"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "AverageFaithfulness"
  namespace           = "LLMReviewSummarizer"
  period              = "86400" # 24 hours, matching the DAG schedule
  statistic           = "Average"
  threshold           = var.faithfulness_threshold
  alarm_description   = "This alarm triggers if the average summary faithfulness score drops below the acceptable threshold."
  
  alarm_actions = [aws_sns_topic.alerts_topic.arn]
  ok_actions    = [aws_sns_topic.alerts_topic.arn]
}

resource "aws_cloudwatch_dashboard" "summarizer_dashboard" {
  dashboard_name = "LLM-Review-Summarizer-Dashboard"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric",
        x      = 0,
        y      = 0,
        width  = 12,
        height = 6,
        properties = {
          metrics = [
            ["LLMReviewSummarizer", "AverageFaithfulness"]
          ],
          period = 300,
          stat   = "Average",
          region = "eu-west-1",
          title  = "Summary Faithfulness (Daily Average)"
          # Add horizontal annotation for the alarm threshold
        }
      },
      # ... other widgets for coherence, EKS GPU utilization, etc.
    ]
  })
}