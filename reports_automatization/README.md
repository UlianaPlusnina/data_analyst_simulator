# reports automatization

1. feed_report_bot

every day bot have to sent text message with key metrics (DAU, views, likes, CTR) and charts with metrics values for the last 7 days.

2. feed&messenger_report_bot

every day bot have to sent text messages and charts with key metrics for yesterday and text messages and charts with key metrics for a time period.

3. alert_report_bot

We need to create a real-time alerting system, that checks our data for anomalies live every 15 minutes and send reports to Telegram chat.

Metrics to check are: DAU, views, likes, CTR, messages.

To detect anomaly value I used Isolation Forest.