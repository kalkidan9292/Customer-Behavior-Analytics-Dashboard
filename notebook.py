import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import psycopg2

# ---------- LOAD ----------
df = pd.read_csv("customer_shopping_behavior.csv")

# ---------- CLEAN COLUMN NAMES ----------
df.columns = (
    df.columns
      .str.strip()
      .str.lower()
      .str.replace(r'[^0-9a-z]+', '_', regex=True)  # spaces, (), %, etc → _
      .str.strip('_')
)
# purchase_amount_(usd) becomes purchase_amount_usd automatically

# ---------- IMPUTE REVIEW RATING BY CATEGORY MEDIAN ----------
df['review_rating'] = pd.to_numeric(df['review_rating'], errors='coerce')
overall_med = df['review_rating'].median()
cat_med = df.groupby('category')['review_rating'].transform('median')
df['review_rating'] = df['review_rating'].fillna(cat_med).fillna(overall_med)

# ---------- FEATURE ENGINEERING ----------
# Age group (quartiles)
labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)

# Text frequency → numeric days
frequency_mapping = {
    'Fortnightly': 14, 'Weekly': 7, 'Monthly': 30, 'Quarterly': 90,
    'Bi-Weekly': 14, 'Annually': 365, 'Every 3 Months': 90
}
df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping).fillna(0).astype(int)

# Drop duplicate signal if it duplicates discount_applied
if 'promo_code_used' in df.columns and (df['discount_applied'] == df['promo_code_used']).all():
    df = df.drop(columns='promo_code_used')

# ---------- INSPECT AFTER CLEANING ----------
print("\n--- COLUMNS ---")
print(list(df.columns))

print("\n--- INFO ---")
print(df.info())

print("\nNulls in review_rating:", df['review_rating'].isna().sum())
print("\nSample rows:")
print(df.head(3))

# ---------- SAVE CLEANED FOR POWER BI ----------
df.to_csv("customer_shopping_behavior_clean.csv", index=False)
print("\n✅ File loaded successfully! Cleaned CSV saved as customer_shopping_behavior_clean.csv")

# ---------- CONNECT TO POSTGRESQL ----------
df['age_group'] = df['age_group'].astype(str)  # ensure string before writing to SQL

url = URL.create(
    drivername="postgresql+psycopg2",
    username="postgres",
    password="Yomiyu@2904",   # raw password okay here
    host="localhost",
    port=5432,
    database="customer_behavior",
)

engine = create_engine(url)

# ---------- EXPORT TO DATABASE ----------
table_name = "customer"
df.to_sql(table_name, engine, if_exists="replace", index=False)
print(f"🎉 Data successfully loaded into table '{table_name}' in database 'customer_behavior'.")
