"""
StreamFlow Subscription History Generator
Run this last to create revenue data
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

np.random.seed(42)
random.seed(42)

print("=" * 50)
print("🎬 StreamFlow Analytics - Subscription Generation")
print("=" * 50)

# Load users
try:
    users_df = pd.read_csv('data/users.csv')
    print(f"✅ Loaded {len(users_df):,} users")
except FileNotFoundError as e:
    print(f"❌ Error: {e}")
    print("Make sure you've run 01_generate_users.py first!")
    exit(1)

PLANS = ['Free', 'Basic', 'Premium', 'Family']
PLAN_PRICES = {'Free': 0, 'Basic': 8.99, 'Premium': 14.99, 'Family': 19.99}

subscriptions = []

print(f"Generating subscription records for {len(users_df):,} users...")

# Create iterator with or without tqdm
iterator = tqdm(users_df.iterrows(), total=len(users_df), desc="Processing users") if USE_TQDM else users_df.iterrows()

for idx, user in iterator:
    if not USE_TQDM and idx % 1000 == 0 and idx > 0:
        print(f"  Processed {idx:,} users...")
    
    user_id = user['user_id']
    signup_date = datetime.strptime(user['signup_date'], '%Y-%m-%d')
    current_plan = user['current_plan']
    
    if current_plan != 'Free':
        billing_date = signup_date
        end_date = datetime.now()
        
        months_count = 0
        while billing_date <= end_date:
            subscriptions.append({
                'subscription_id': f"SUB_{user_id}_{billing_date.strftime('%Y%m')}",
                'user_id': user_id,
                'plan': current_plan,
                'monthly_price': PLAN_PRICES[current_plan],
                'billing_date': billing_date.strftime('%Y-%m-%d'),
                'payment_status': np.random.choice(['Paid', 'Failed'], p=[0.85, 0.15]),
                'payment_method': np.random.choice(['Credit Card', 'PayPal', 'Debit Card', 'Apple Pay'], p=[0.60, 0.25, 0.10, 0.05])
            })
            billing_date = billing_date + timedelta(days=30)
            months_count += 1

subscriptions_df = pd.DataFrame(subscriptions)
os.makedirs('data', exist_ok=True)
subscriptions_df.to_csv('data/subscriptions.csv', index=False)

print(f"\n✅ Generated {len(subscriptions_df):,} subscription records")
print(f"📁 Saved to: data/subscriptions.csv")

if len(subscriptions_df) > 0:
    print(f"\nFirst 5 rows:")
    print(subscriptions_df.head())
    print(f"\n📊 Data Summary:")
    print(f"Total revenue (if all paid): ${subscriptions_df['monthly_price'].sum():,.2f}")
    print(f"Payment status distribution:")
    print(subscriptions_df['payment_status'].value_counts())
    print(f"\nPlan distribution:")
    print(subscriptions_df['plan'].value_counts())
else:
    print("\n⚠️ No paid subscriptions generated (all users on Free plan)")

print("\n" + "=" * 50)