# ttc_save_data_db.py
"""
body for main data storage in a database implementation
"""

from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text, NUMERIC, TIMESTAMP, Integer

from prefect import task
from prefect.cache_policies import NO_CACHE


# ── Helpers ───────────────────────────────────────────────────────────────────────────────────────────────────────────
def create_engine_to_database(
        tdatabase_url
) -> create_engine:
    """Creating engine for connections"""
    tengine = create_engine(
        tdatabase_url,
        echo=False,  # True for SQL logging
        pool_pre_ping=True  # ensures connections are valid before use
    )
    return tengine


def convert_processed_time_to_dt(
        tprocessed_time
) -> datetime:
    """Converting processed_time (YYYY-MM-DD HH:MM) to proper timestamp(0) for further load"""
    tprocessed_dt = datetime.strptime(tprocessed_time, "%Y-%m-%d %H:%M")
    return tprocessed_dt


def convert_time_elapsed_to_dt(
        ttime_elapsed,
        tprocessed_time,
        tlogger
) -> datetime:
    """Converting time_elapsed ('25 Minute ago') to proper timestamp(0) for further load"""
    if "now" in str(ttime_elapsed).lower():
        return convert_processed_time_to_dt(tprocessed_time=tprocessed_time)
    elif "minute" in str(ttime_elapsed).lower():
        mins = int(str(ttime_elapsed.split(" ")[0]).strip())
        return convert_processed_time_to_dt(tprocessed_time=tprocessed_time) - timedelta(minutes=mins)
    elif "hour" in str(ttime_elapsed).lower():
        hrs = int(str(ttime_elapsed.split(" ")[0]).strip())
        return convert_processed_time_to_dt(tprocessed_time=tprocessed_time) - timedelta(hours=hrs)
    # if "day"... etc
    else:
        tlogger.error("Failed to convert time_elapsed to date.")
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise ValueError(f"Given time_elapsed: '{ttime_elapsed}' is not processed in current logic. Logic needs to be updated first.")


def convert_price(
        tprice_str
) -> float:
    """Converting given prices to proper numeric(10,2) for further load"""
    if isinstance(tprice_str, str):
        return float(tprice_str.replace(' ', ''))
    return tprice_str


def get_item_id_mapping(
        tengine,
        tscd2,
        tschema,
        titem_names
) -> dict:
    """Getting ids for item_names in the dataset for further load"""
    unique_items = list(set(titem_names))

    query = text(f"""
        SELECT item_id, item_name 
        FROM {tschema}.{tscd2}
        WHERE item_name = ANY(:names) AND deleted_flg = 'N'
    """)

    with tengine.connect() as conn:
        result = conn.execute(query, {"names": unique_items})
        return {row[1]: row[0] for row in result.fetchall()}    # dict {1: 'Aetherial Dust', 2 : 'Other Item'}


# ── Tasks ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
@task(cache_policy=NO_CACHE)
def save_processed_data_pqsql(
        tdatabase_url,
        tdb_coded_name,
        tfact_table,
        tfact_item_table_schema,
        tscd2_table,
        tscd2_item_table_schema,
        tprocessed_data,
        tprocessed_time,
        tlogger
) -> int:
    """Saving processed data list into the database"""
    tlogger.info("Creating and preparing dataframe to load...")
    df = pd.DataFrame(tprocessed_data)

    tlogger.info(f"Converting prices...")
    df['unit_price'] = df['unit_price'].apply(convert_price)
    df['total_price'] = df['total_price'].apply(convert_price)
    df['amount'] = df['amount'].astype(int)
    tlogger.info(f"Converting dates and time...")
    df['time_elapsed'] = df['time_elapsed'].apply(
        lambda x: convert_time_elapsed_to_dt(x, tprocessed_time=tprocessed_time, tlogger=tlogger)
    )
    df['created_dt'] = convert_processed_time_to_dt(tprocessed_time=tprocessed_time)
    df['changed_dt'] = convert_processed_time_to_dt(tprocessed_time=tprocessed_time)

    df.rename(columns={'player_id': 'player_teso_id'}, inplace=True)
    df.rename(columns={'location': 'location_name'}, inplace=True)
    df.rename(columns={'time_elapsed': 'listed_dt'}, inplace=True)
    tlogger.info("Dataframe created and prepared.")

    tlogger.info("Creating connection to the database...")
    try:
        engine = create_engine_to_database(tdatabase_url=tdatabase_url)
        tlogger.info("Connection to the database created.")
    except Exception as e:
        tlogger.error(f"Failed to connect to the database: '{tdb_coded_name}' (db_coded_name), {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise

    tlogger.info("Making scd2 id-name mapping for the dataframe...")
    try:
        item_map = get_item_id_mapping(
            tengine=engine,
            tscd2=tscd2_table,
            tschema=tscd2_item_table_schema,
            titem_names=df['item_name'].unique()
        )
        df['item_id'] = df['item_name'].map(item_map)
        tlogger.info("Scd2 id-name mapping for the dataframe made.")
    except Exception as e:
        tlogger.error(f"Failed to make scd2 id-name mapping for the dataframe, {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise

    missing_item_id = df['item_id'].isna()
    if missing_item_id.any():
        missing_items = df.loc[missing_item_id, 'item_name'].unique()
        tlogger.error("Failed to create proper map for item_names: some items didn't get ids.")
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise ValueError(f"Failed to find '{missing_items}' in {tscd2_table}. Values need to be inserted first.")

    tlogger.info("Doing bulk inserts...")
    cols_to_insert = [
        'item_id',
        'player_teso_id',
        'guild_name',
        'location_name',
        'unit_price',
        'amount',
        'total_price',
        'listed_dt',
        'created_dt',
        'changed_dt'
    ]
    df_final = df[cols_to_insert]
    inserted_rows_amt = len(df_final)

    try:
        with engine.begin() as conn:
            df_final.to_sql(
                name=tfact_table,
                con=conn,
                schema=tfact_item_table_schema,
                if_exists='append',
                index=False,    # no pandas index
                method='multi',  # batches for speed
                dtype={
                    'unit_price': NUMERIC(10, 2),
                    'total_price': NUMERIC(10, 2),
                    'listed_dt': TIMESTAMP(timezone=False),  # mandatory for timestamp(0)
                    'created_dt': TIMESTAMP(timezone=False),  # mandatory for timestamp(0)
                    'changed_dt': TIMESTAMP(timezone=False),  # mandatory for timestamp(0)
                    'amount': Integer
                }
            )
        tlogger.info(f"Inserted data: {inserted_rows_amt} rows.")
        return inserted_rows_amt
    except Exception as e:
        tlogger.error(f"Failed to insert data: {inserted_rows_amt} rows, {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise


# ── Main ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pass
