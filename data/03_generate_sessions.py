"""
StreamFlow Watch Session Generator
Run this third to create watch history
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Try to import tqdm, fallback if not available
try:
    from tqdm import tqdm
    USE_TQDM = True
except ImportError:
    USE_TQDM = False
    print("Note: Install 'tqdm' for progress bars (pip install tqdm)")

np.random.seed(42)
random.seed(42)

NUM_SESSIONS = 50000  # Start with 50K for testing
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)

print("=" * 50)
print("🎬 StreamFlow Analytics - Session Generation")
print("=" * 50)
print(f"Generating {NUM_SESSIONS:,} watch sessions...")

# Load reference data
try:
    users_df = pd.read_csv('data/users.csv')
    content_df = pd.read_csv('data/content.csv')
    print(f"✅ Loaded {len(users_df):,} users and {len(content_df):,} content items")
except FileNotFoundError as e:
    print(f"❌ Error: {e}")
    print("Make sure you've run 01_generate_users.py and 02_generate_content.py first!")
    exit(1)

# Generate sessions
sessions = []

# Hourly watch probability weights (peaks in evening)
hour_weights = [
    2, 1, 1, 1, 2,  # 0-4 (5 hours)
    3, 5, 7, 6, 5,  # 5-9 (5 hours)
    6, 7, 6, 6,     # 10-13 (4 hours)
    6, 7, 8, 9, 10, 9,  # 14-19 (6 hours)
    8, 6, 4, 3      # 20-23 (4 hours)
]

# Normalize to probabilities that sum to 1
total = sum(hour_weights)
hour_probs = [w / total for w in hour_weights]

# Verify probabilities
assert len(hour_probs) == 24, f"Must have 24 probabilities, got {len(hour_probs)}"
assert abs(sum(hour_probs) - 1.0) < 0.0001, f"Probabilities must sum to 1, got {sum(hour_probs)}"

print(f"✅ Hourly probabilities normalized (sum: {sum(hour_probs):.4f})")

# Create iterator with or without tqdm
iterator = tqdm(range(NUM_SESSIONS), desc="Generating sessions") if USE_TQDM else range(NUM_SESSIONS)

for i in iterator:
    if not USE_TQDM and i % 10000 == 0 and i > 0:
        print(f"  Generated {i:,} sessions...")
    
    user_id = np.random.choice(users_df['user_id'].values)
    content_item = content_df.sample(1).iloc[0]
    
    hour = np.random.choice(range(24), p=hour_probs)
    
    watch_start = START_DATE + timedelta(
        days=random.randint(0, (END_DATE - START_DATE).days),
        hours=int(hour),
        minutes=random.randint(0, 59)
    )
    
    max_duration = content_item['duration_minutes']
    # Most people watch either a little or almost all (bimodal distribution)
    if random.random() < 0.6:
        watch_pct = random.uniform(0.7, 1.0)  # High engagement
    else:
        watch_pct = random.uniform(0.1, 0.4)  # Drop off early
    
    watch_duration = round(max_duration * watch_pct, 1)
    
    sessions.append({
        'session_id': f"SESS_{i:09d}",
        'user_id': user_id,
        'content_id': content_item['content_id'],
        'watch_start_time': watch_start.strftime('%Y-%m-%d %H:%M:%S'),
        'watch_duration_minutes': watch_duration,
        'content_duration_minutes': max_duration,
        'completion_rate': round(watch_pct * 100, 1),
        'completed': watch_pct > 0.85,
        'device_used': np.random.choice(['Mobile', 'Desktop', 'Tablet', 'Smart TV'], p=[0.45, 0.30, 0.15, 0.10]),
        'quality_streamed': np.random.choice(['480p', '720p', '1080p', '4K'], p=[0.15, 0.35, 0.40, 0.10])
    })

sessions_df = pd.DataFrame(sessions)
os.makedirs('data', exist_ok=True)
sessions_df.to_csv('data/watch_sessions.csv', index=False)

print(f"\n✅ Generated {len(sessions_df):,} watch sessions")
print(f"📁 Saved to: data/watch_sessions.csv")
print(f"\nFirst 5 rows:")
print(sessions_df.head())
print(f"\n📊 Data Summary:")
print(f"Date range: {sessions_df['watch_start_time'].min()} to {sessions_df['watch_start_time'].max()}")
print(f"Avg completion rate: {sessions_df['completion_rate'].mean():.1f}%")
print(f"Avg watch duration: {sessions_df['watch_duration_minutes'].mean():.1f} minutes")
print(f"Completed views: {sessions_df['completed'].sum():,} ({sessions_df['completed'].mean()*100:.1f}%)")
print("\n" + "=" * 50)