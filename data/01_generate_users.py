"""
StreamFlow User Generation Script
Run this first to create the users dataset
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Configuration
np.random.seed(42)
random.seed(42)

NUM_USERS = 5000  # Start with 5K for testing, increase to 50K later
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)

print("=" * 50)
print("🎬 StreamFlow Analytics - User Generation")
print("=" * 50)
print(f"Generating {NUM_USERS:,} users...")

# Device types
DEVICES = ['Mobile', 'Desktop', 'Tablet', 'Smart TV', 'Gaming Console']
DEVICE_WEIGHTS = [0.45, 0.25, 0.15, 0.10, 0.05]

# Countries
COUNTRIES = ['USA', 'UK', 'Canada', 'Australia', 'India', 'Germany', 'Brazil', 'Japan']
COUNTRY_WEIGHTS = [0.35, 0.15, 0.10, 0.08, 0.12, 0.07, 0.05, 0.08]

# Subscription plans
PLANS = ['Free', 'Basic', 'Premium', 'Family']
PLAN_WEIGHTS = [0.50, 0.25, 0.20, 0.05]

def generate_signup_date():
    """Generate realistic signup dates with growth trend"""
    days_range = (END_DATE - START_DATE).days
    # Use triangular distribution: left=0, mode=70% of range, right=full range
    # This creates more recent signups (growth trend)
    mode = int(days_range * 0.7)
    day_offset = int(np.random.triangular(0, mode, days_range))
    return START_DATE + timedelta(days=day_offset)

# Generate users
users = []
for user_id in range(1, NUM_USERS + 1):
    if user_id % 1000 == 0:
        print(f"  Generated {user_id:,} users...")
    
    signup_date = generate_signup_date()
    country = np.random.choice(COUNTRIES, p=COUNTRY_WEIGHTS)
    
    # Generate age with normal-like distribution using triangular
    age = int(np.random.triangular(16, 32, 65))
    
    # Device preference based on age
    if age < 25:
        device_weights = [0.55, 0.20, 0.15, 0.05, 0.05]
    elif age > 50:
        device_weights = [0.25, 0.30, 0.20, 0.20, 0.05]
    else:
        device_weights = DEVICE_WEIGHTS
    
    device = np.random.choice(DEVICES, p=device_weights)
    
    # Plan based on country
    if country in ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'Japan']:
        plan_weights = [0.35, 0.30, 0.25, 0.10]
    else:
        plan_weights = [0.65, 0.20, 0.12, 0.03]
    
    plan = np.random.choice(PLANS, p=plan_weights)
    
    users.append({
        'user_id': user_id,
        'username': f"user_{user_id:06d}",
        'signup_date': signup_date.strftime('%Y-%m-%d'),
        'country': country,
        'age_group': f"{(age // 10) * 10}-{((age // 10) * 10) + 9}",
        'primary_device': device,
        'current_plan': plan,
        'acquisition_channel': np.random.choice(
            ['Organic Search', 'Social Media', 'Referral', 'Paid Ads', 'Email'],
            p=[0.35, 0.25, 0.20, 0.15, 0.05]
        )
    })

# Create DataFrame and save
users_df = pd.DataFrame(users)

# Ensure data directory exists
os.makedirs('data', exist_ok=True)
users_df.to_csv('data/users.csv', index=False)

print(f"\n✅ Generated {len(users_df):,} users")
print(f"📁 Saved to: data/users.csv")
print(f"\nFirst 5 rows:")
print(users_df.head())
print("\n📊 Data Summary:")
print(f"Date range: {users_df['signup_date'].min()} to {users_df['signup_date'].max()}")
print(f"Plans distribution:")
print(users_df['current_plan'].value_counts())
print("\n" + "=" * 50)