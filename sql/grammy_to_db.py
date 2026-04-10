from sqlalchemy import create_engine
from pathlib import Path
import pandas as pd
from tabulate import tabulate

# Path
base_path = Path(r"C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\workshop_2")

grammy = base_path / "data" / "the_grammy_awards.csv"

# Read csv file
df_grammy = pd.read_csv(grammy)

# Create connection to the database
engine = create_engine("mysql+pymysql://root:@localhost:3306/workshop2")

# Convert grammy csv to DB table
df_grammy.to_sql("grammy", engine, if_exists="replace", index=False)

print("Tables created successfully!")

# Confirm table
with engine.connect() as conn:
    print(tabulate(
        pd.read_sql("SHOW TABLES", conn),
        headers='keys', tablefmt='psql', showindex=False
    ))

    # ✅ Solo grammy, que es la que existe
    print(f"\n{'='*50}")
    print(f" GRAMMY")
    print(f"{'='*50}")
    print(tabulate(
        pd.read_sql("SELECT * FROM grammy LIMIT 5", conn),
        headers='keys', tablefmt='psql', showindex=False
    ))