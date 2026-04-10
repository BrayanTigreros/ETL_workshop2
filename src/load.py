from sqlalchemy import create_engine, text
import pandas as pd
import os

# Save to csv
def save_dimensions_to_csv(target_file, **dataframes):

    for name, df in dataframes.items():
        file_path = os.path.join(target_file, f"{name}.csv")
        df.to_csv(file_path, index=False)
        print(f"Saved: {file_path}")

def insert_ignore(df, table_name, engine):
    #This function creates a temporary table, inserts the data, and then uses an INSERT IGNORE statement to avoid duplicates
    temp_table = f"tmp_{table_name}"

    # load to temp table
    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    # build the column list for the insert statement
    cols = ", ".join(df.columns)

    # insert ignoring duplicates
    insert_sql = f"""
        INSERT IGNORE INTO {table_name} ({cols})
        SELECT {cols} FROM {temp_table};
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE {temp_table}"))

# Load to de DW
def load_to_dw(dataframes):

    dim_candidate = dataframes["dim_candidate"]
    dim_country = dataframes["dim_country"]
    dim_technology = dataframes["dim_technology"]
    dim_seniority = dataframes["dim_seniority"]
    dim_date = dataframes["dim_date"]
    fact_application = dataframes["fact_application"]

    engine = create_engine(
        "mysql+pymysql://root:@localhost:3306/workshop1"
    )

    insert_ignore(dim_date, "dim_date", engine)
    insert_ignore(dim_candidate, "dim_candidate", engine)
    insert_ignore(dim_country, "dim_country", engine)
    insert_ignore(dim_technology, "dim_technology", engine)
    insert_ignore(dim_seniority, "dim_seniority", engine)

    # to avoid duplicates in the fact table, anti-join with the existinf pk's of the DW
    key_cols = [
        "candidate_key",
        "technology_key",
        "seniority_key",
        "country_key",
        "date_key",
    ]

    # read existing keys from the DW
    existing_keys_sql = f"SELECT {', '.join(key_cols)} FROM fact_application"

    try:
        existing_keys_df = pd.read_sql(existing_keys_sql, engine)
    except Exception:
        existing_keys_df = pd.DataFrame(columns=key_cols)

    # anti-join so only new rows are inserted into the fact table
    if not existing_keys_df.empty:
        merged = fact_application.merge(
            existing_keys_df.drop_duplicates(),
            on=key_cols,
            how="left",
            indicator=True,
        )
        fact_new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        fact_new = fact_application.copy()

    print(
        f"Fact total={len(fact_application)} "
        f"new rows={len(fact_new)} "
        f"duplicates omitted={len(fact_application) - len(fact_new)}"
    )

    # insertar solo nuevas
    if not fact_new.empty:
        insert_ignore(fact_new, "fact_application", engine)
    else:
        print("no new rows for fact_application")

    print("Load to Data Warehouse has been completed successfull")