"""
StreamFlow Content Library Generator
Run this second to create content catalog
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

np.random.seed(42)
random.seed(42)

NUM_CONTENT = 2000  # Start with 2K for testing
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)

print("=" * 50)
print("🎬 StreamFlow Analytics - Content Generation")
print("=" * 50)
print(f"Generating {NUM_CONTENT:,} content items...")

CATEGORIES = {
    'Entertainment': 0.20,
    'Education': 0.15,
    'Music': 0.12,
    'Gaming': 0.10,
    'Sports': 0.08,
    'News': 0.07,
    'Technology': 0.08,
    'Lifestyle': 0.10,
    'Documentary': 0.05,
    'Kids': 0.05
}

CONTENT_TYPES = ['Video', 'Series Episode', 'Movie', 'Short', 'Live Stream']
CONTENT_TYPE_WEIGHTS = [0.50, 0.25, 0.10, 0.10, 0.05]

DURATION_RANGES = {
    'Video': (5, 60),
    'Series Episode': (20, 55),
    'Movie': (80, 180),
    'Short': (0.5, 3),
    'Live Stream': (30, 240)
}

TITLE_PREFIXES = ['The', 'Ultimate', 'Amazing', 'Incredible', 'Essential', 'Complete', 'Secret']
TITLE_TOPICS = ['Guide', 'Tutorial', 'Review', 'Compilation', 'Journey', 'Story', 'Analysis']

def generate_title(category):
    """Generate realistic content titles"""
    if random.random() < 0.3:
        prefix = random.choice(TITLE_PREFIXES)
        topic = random.choice(TITLE_TOPICS)
        return f"{prefix} {category} {topic}"
    else:
        return f"{category}: {random.choice(TITLE_TOPICS)} #{random.randint(1, 100)}"

# Generate content
content = []
creators_pool = [f"Creator_{i}" for i in range(1, 101)]  # 100 creators

for content_id in range(1, NUM_CONTENT + 1):
    if content_id % 500 == 0:
        print(f"  Generated {content_id:,} content items...")
    
    publish_date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
    category = np.random.choice(list(CATEGORIES.keys()), p=list(CATEGORIES.values()))
    content_type = np.random.choice(CONTENT_TYPES, p=CONTENT_TYPE_WEIGHTS)
    
    min_dur, max_dur = DURATION_RANGES[content_type]
    duration_minutes = round(random.uniform(min_dur, max_dur), 1)
    
    if random.random() < 0.1:
        creator = f"Top_Creator_{random.randint(1, 10)}"
    else:
        creator = random.choice(creators_pool)
    
    content.append({
        'content_id': f"CONT_{content_id:06d}",
        'title': generate_title(category),
        'category': category,
        'content_type': content_type,
        'duration_minutes': duration_minutes,
        'creator_name': creator,
        'publish_date': publish_date.strftime('%Y-%m-%d'),
        'is_exclusive': random.random() < 0.15,
        'production_quality': np.random.choice(['Standard', 'HD', '4K', 'HDR'], p=[0.1, 0.4, 0.4, 0.1])
    })

content_df = pd.DataFrame(content)
os.makedirs('data', exist_ok=True)
content_df.to_csv('data/content.csv', index=False)

print(f"\n✅ Generated {len(content_df):,} content items")
print(f"📁 Saved to: data/content.csv")
print(f"\nFirst 5 rows:")
print(content_df.head())
print(f"\n📊 Data Summary:")
print(f"Categories distribution:")
print(content_df['category'].value_counts().head())
print(f"\nContent types distribution:")
print(content_df['content_type'].value_counts())
print("\n" + "=" * 50)