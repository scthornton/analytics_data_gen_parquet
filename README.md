# 📊 Analytics Data Generator

> Generate realistic user behavior data for testing, demos, and development

[![Python](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![Parquet](https://img.shields.io/badge/format-parquet-orange.svg)](https://parquet.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## 🚀 What is this?

Ever needed realistic user analytics data but didn't want to use real customer information? This tool generates synthetic analytics data that mirrors actual user behavior patterns, complete with sessions, page views, conversions, and user segments. The data gets saved as Parquet files - a columnar storage format that's perfect for analytics workloads.

### Why Parquet?

Parquet files are like the Swiss Army knife of data storage:
- **Columnar format**: Super fast for analytics queries
- **Compressed**: Takes up way less space than CSV
- **Schema included**: No more guessing data types
- **Works everywhere**: Spark, Pandas, BigQuery, Snowflake - you name it

## 📋 Features

- **Realistic user segments** with different behavior patterns
- **30 days of historical data** across 1000+ users
- **Multiple event types**: page views, conversions, bounces
- **Geographic and device diversity**
- **Time-based patterns** (because real users don't browse at 3 AM... usually)

## 🏃‍♂️ Quick Start

```bash
# Install dependencies
pip install pandas numpy pyarrow

# Run the generator
python analytics_generator.py
```

That's it! You'll get three shiny new Parquet files:

- `analytics_events.parquet` - Raw event stream data
- `daily_user_metrics.parquet` - Daily aggregations per user  
- `session_metrics.parquet` - Session-level summaries

## 📊 What's in the Data?

### User Segments

The generator creates three types of users (because not everyone is a power user):

| Segment | Distribution | Behavior |
|---------|-------------|----------|
| **Power Users** | 10% | 3-8 sessions/day, high engagement |
| **Regular Users** | 60% | 1-3 sessions/day, moderate activity |
| **Casual Users** | 30% | 0-2 sessions/day, quick visits |

### Event Schema

Each event includes:

```python
{
    'event_id': 'user_0023_20240115_2_4',
    'user_id': 'user_0023',
    'session_id': 'user_0023_20240115_2',
    'timestamp': '2024-01-15 14:32:10',
    'event_type': 'page_view',
    'page_category': 'product',
    'page_name': 'product_page_7',
    'time_on_page': 45,
    'bounce': False,
    'device_type': 'mobile',
    'country': 'US',
    'referrer': 'google',
    'user_segment': 'regular_user'
}
```

## 🛠️ Use Cases

### 1. **Testing Analytics Pipelines**
Perfect for making sure your data pipeline doesn't break in production. Test aggregations, transformations, and loading processes without touching real data.

### 2. **Building Demos & POCs**
Need to show off that new dashboard? This data looks real enough to impress stakeholders without the compliance headaches.

### 3. **Learning & Training**
Great for:
- Teaching SQL/Pandas/Spark
- Practicing data analysis
- Building portfolio projects
- Testing new tools

### 4. **Performance Testing**
Generate millions of events to stress-test your systems. Just bump up the `num_users` and `days` parameters.

### 5. **Machine Learning Development**
Train recommendation engines, predict user churn, or forecast conversion rates - all without privacy concerns.

## 🎛️ Customization

Want different patterns? Easy to tweak:

```python
# More users, longer timeframe
df = generate_analytics_data(num_users=5000, days=90)

# Adjust user segment distribution
user_segments = {
    'power_user': {'daily_sessions': (5, 12), ...},
    'regular_user': {'daily_sessions': (2, 5), ...},
    'casual_user': {'daily_sessions': (0, 1), ...}
}

# Change conversion rate (line ~160)
if random.random() < 0.25:  # 25% conversion rate
```

## 📈 Sample Queries

Once you've got your data, try these:

### Daily Active Users (SQL)
```sql
SELECT 
    date,
    COUNT(DISTINCT user_id) as dau
FROM analytics_events
GROUP BY date
ORDER BY date
```

### Conversion Funnel (Pandas)
```python
df = pd.read_parquet('analytics_events.parquet')
funnel = df.groupby('page_category')['user_id'].nunique()
print(funnel.sort_values(ascending=False))
```

## 🤝 Contributing

Found a bug? Want to add a feature? PRs welcome! Some ideas:
- Add more event types (clicks, scrolls, form submissions)
- Include A/B test variations
- Add seasonal patterns
- Generate mobile app events

## 📝 License

MIT - Use it however you want!

## 🙋‍♀️ FAQ

**Q: How realistic is this data?**  
A: Pretty realistic! It includes natural patterns like higher mobile usage, conversion rates around 10%, and user segments that behave differently. But remember - it's synthetic, so don't use it for actual business decisions!

**Q: Can I use this for commercial projects?**  
A: Absolutely! That's what the MIT license is for.

**Q: Why Parquet and not CSV?**  
A: Parquet is faster, smaller, and maintains data types. But if you really need CSV, just change the output format in the script.

**Q: Can it generate more than 30 days of data?**  
A: Yes! Just change the `days` parameter. Fair warning: things get big fast.

---

Made with ☕ and Python. Happy analyzing!
