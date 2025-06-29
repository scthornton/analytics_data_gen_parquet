import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import pyarrow as pa
import pyarrow.parquet as pq

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Generate sample data
def generate_analytics_data(num_users=1000, days=30):
    """
    Generate synthetic analytics data with realistic user behavior patterns
    """
    
    # User segments for behavioral modeling
    user_segments = {
        'power_user': {'daily_sessions': (3, 8), 'session_duration': (15, 45), 'pages_per_session': (10, 30)},
        'regular_user': {'daily_sessions': (1, 3), 'session_duration': (5, 20), 'pages_per_session': (3, 10)},
        'casual_user': {'daily_sessions': (0, 2), 'session_duration': (2, 10), 'pages_per_session': (1, 5)}
    }
    
    # Generate user profiles
    users = []
    for i in range(num_users):
        segment = np.random.choice(['power_user', 'regular_user', 'casual_user'], 
                                  p=[0.1, 0.6, 0.3])
        users.append({
            'user_id': f'user_{i:04d}',
            'segment': segment,
            'acquisition_date': datetime.now() - timedelta(days=random.randint(30, 365)),
            'country': np.random.choice(['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU'], 
                                      p=[0.4, 0.15, 0.1, 0.1, 0.1, 0.1, 0.05]),
            'device_type': np.random.choice(['mobile', 'desktop', 'tablet'], 
                                          p=[0.5, 0.4, 0.1])
        })
    
    # Generate events
    events = []
    start_date = datetime.now() - timedelta(days=days)
    
    for user in users:
        user_behavior = user_segments[user['segment']]
        
        for day in range(days):
            current_date = start_date + timedelta(days=day)
            
            # Determine number of sessions for this day
            num_sessions = random.randint(*user_behavior['daily_sessions'])
            
            for session in range(num_sessions):
                session_id = f"{user['user_id']}_{current_date.strftime('%Y%m%d')}_{session}"
                session_start = current_date + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )
                session_duration = random.randint(*user_behavior['session_duration'])
                
                # Generate page views within session
                num_pages = random.randint(*user_behavior['pages_per_session'])
                
                for page_num in range(num_pages):
                    event_time = session_start + timedelta(
                        minutes=random.randint(0, session_duration)
                    )
                    
                    # Page categories based on user behavior
                    if user['segment'] == 'power_user':
                        page_category = np.random.choice(
                            ['product', 'search', 'account', 'checkout', 'support'],
                            p=[0.3, 0.3, 0.2, 0.15, 0.05]
                        )
                    else:
                        page_category = np.random.choice(
                            ['product', 'search', 'account', 'checkout', 'support'],
                            p=[0.4, 0.3, 0.1, 0.1, 0.1]
                        )
                    
                    event = {
                        'event_id': f"{session_id}_{page_num}",
                        'user_id': user['user_id'],
                        'session_id': session_id,
                        'timestamp': event_time,
                        'event_type': 'page_view',
                        'page_category': page_category,
                        'page_name': f"{page_category}_page_{random.randint(1, 10)}",
                        'time_on_page': random.randint(10, 300),
                        'bounce': page_num == 0 and num_pages == 1,
                        'device_type': user['device_type'],
                        'country': user['country'],
                        'referrer': np.random.choice(
                            ['google', 'facebook', 'direct', 'email', 'other'],
                            p=[0.3, 0.2, 0.3, 0.1, 0.1]
                        ),
                        'user_segment': user['segment']
                    }
                    events.append(event)
                
                # Add conversion events for some sessions
                if random.random() < 0.1:  # 10% conversion rate
                    conversion_event = {
                        'event_id': f"{session_id}_conversion",
                        'user_id': user['user_id'],
                        'session_id': session_id,
                        'timestamp': session_start + timedelta(minutes=session_duration),
                        'event_type': 'conversion',
                        'page_category': 'checkout',
                        'page_name': 'order_complete',
                        'time_on_page': 0,
                        'bounce': False,
                        'device_type': user['device_type'],
                        'country': user['country'],
                        'referrer': events[-1]['referrer'] if events else 'direct',
                        'user_segment': user['segment'],
                        'revenue': round(random.uniform(10, 500), 2)
                    }
                    events.append(conversion_event)
    
    return pd.DataFrame(events)

# Generate the data
print("Generating analytics data...")
df = generate_analytics_data(num_users=1000, days=30)

# Add some calculated fields
df['date'] = df['timestamp'].dt.date
df['hour'] = df['timestamp'].dt.hour
df['day_of_week'] = df['timestamp'].dt.day_name()
df['is_weekend'] = df['timestamp'].dt.dayofweek.isin([5, 6])

# Create aggregated metrics
print("\nGenerating aggregated metrics...")

# Daily user metrics
daily_metrics = df.groupby(['date', 'user_id']).agg({
    'session_id': pd.Series.nunique,
    'event_id': 'count',
    'time_on_page': 'sum'
}).reset_index()
daily_metrics.columns = ['date', 'user_id', 'sessions', 'page_views', 'total_time']

# Session metrics
session_metrics = df.groupby('session_id').agg({
    'user_id': 'first',
    'timestamp': ['min', 'max'],
    'event_id': 'count',
    'bounce': 'any',
    'device_type': 'first',
    'country': 'first',
    'referrer': 'first'
}).reset_index()
session_metrics.columns = ['session_id', 'user_id', 'session_start', 'session_end', 
                          'page_views', 'bounced', 'device_type', 'country', 'referrer']
session_metrics['session_duration'] = (
    session_metrics['session_end'] - session_metrics['session_start']
).dt.total_seconds() / 60

# Save to parquet files
print("\nSaving to parquet files...")

# Event-level data
pq.write_table(pa.Table.from_pandas(df), 'analytics_events.parquet')
print(f"✓ Saved analytics_events.parquet - {len(df):,} events")

# Daily aggregated data
pq.write_table(pa.Table.from_pandas(daily_metrics), 'daily_user_metrics.parquet')
print(f"✓ Saved daily_user_metrics.parquet - {len(daily_metrics):,} daily records")

# Session-level data
pq.write_table(pa.Table.from_pandas(session_metrics), 'session_metrics.parquet')
print(f"✓ Saved session_metrics.parquet - {len(session_metrics):,} sessions")

# Display sample data
print("\n=== Sample Event Data ===")
print(df.head(10))

print("\n=== Data Summary ===")
print(f"Total events: {len(df):,}")
print(f"Unique users: {df['user_id'].nunique():,}")
print(f"Unique sessions: {df['session_id'].nunique():,}")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(f"Conversion events: {len(df[df['event_type'] == 'conversion']):,}")
print(f"Total revenue: ${df[df['event_type'] == 'conversion']['revenue'].sum():,.2f}")

print("\n=== User Segment Distribution ===")
print(df.groupby('user_segment')['user_id'].nunique())

print("\n=== Device Type Distribution ===")
print(df.groupby('device_type')['event_id'].count())

print("\n=== Top Pages ===")
print(df['page_name'].value_counts().head(10))

# Example of reading the parquet file back
print("\n=== Reading parquet file example ===")
df_read = pd.read_parquet('analytics_events.parquet')
print(f"Successfully read {len(df_read)} records from analytics_events.parquet")
print(df_read.info())