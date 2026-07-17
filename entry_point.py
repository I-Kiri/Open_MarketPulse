# entry_point.py
"""
Market Pulse On Test Data (Tamriel Trade Centre) v1 (by I-Kiri, 20260715)
------------------------------------

A full ETL pipeline built to parse test data from Tamriel Trade Centre and load it into a PostgreSQL database and JSON files (as a backup).

USAGE (run and keep running those commands in your terminal):
    prefect server start
    python entry_point.py

"""

from datetime import datetime

from prefect import flow

from ttc_get_data import get_processed_data
from ttc_save_data_json import save_processed_data_json
from ttc_save_data_db import save_processed_data_pqsql
from ttc_make_graphs import get_data_from_pqsql_create_graphs
from logger import create_logger


# ── Config ────────────────────────────────────────────────────────────────────────────────────────────────────────────
from config import (
        database_url,
        url,
        log_dir,
        ttc_save_json_dir,
        ttc_save_graph_dir,
        db_coded_name,
        scd2_item_table,
        scd2_item_table_schema,
        fact_item_table_schema,
        fact_item_table
)


# ── Functions ─────────────────────────────────────────────────────────────────────────────────────────────────────────
def get_current_system_time_processed() -> str:
    """Getting current system time"""
    now = datetime.now()
    now_formatted = now.strftime('%Y-%m-%d %H:%M')

    return now_formatted


# ── Flow ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
@flow(log_prints=True)
def ttc_scraper_flow() -> None:
    """TTC Scraper flow"""

    # time start and logger
    time_process_st = get_current_system_time_processed()
    main_logger, main_logger_path = create_logger(tlogging_time=time_process_st, tlogging_dir=log_dir)
    main_logger.info(f"Process started at {time_process_st}.")


    # getting processed data from a site
    processed_data, processed_data_len = get_processed_data(
        turl=url,
        tlogger=main_logger
    )
    time_get_processed_data = get_current_system_time_processed()

    if processed_data_len == 0:
        main_logger.error(f"Getting and processing data ended with 0 results at {time_get_processed_data}.")
        main_logger.error("TTC Tasks stopped. Flow is running.")
        return
    main_logger.info(f"Getting and processing data ended successfully at {time_get_processed_data}.")
    main_logger.info(f"Processed data: {processed_data_len} rows.")


    # saving processed data into jsons
    json_file_with_data, saved_json_len = save_processed_data_json(
        tpath=ttc_save_json_dir,
        tprocessed_data=processed_data,
        tprocessed_time=get_current_system_time_processed(),
        tlogger=main_logger
    )
    time_save_processed_data_json = get_current_system_time_processed()

    if saved_json_len == 0:
        main_logger.error(f"Saving processed data into json ended with 0 results at {time_save_processed_data_json}.")
        main_logger.error("TTC Tasks stopped. Flow is running.")
        return
    main_logger.info(f"Process to save processed data into json ended successfully at {time_save_processed_data_json}.")
    main_logger.info(f"Json with processed data created: '{json_file_with_data}'.")

    # saving processed data into database
    inserted_rows_amt = save_processed_data_pqsql(
        tdatabase_url=database_url,
        tdb_coded_name=db_coded_name,
        tfact_table=fact_item_table,
        tfact_item_table_schema=fact_item_table_schema,
        tscd2_table=scd2_item_table,
        tscd2_item_table_schema=scd2_item_table_schema,
        tprocessed_data=processed_data,
        tprocessed_time=get_current_system_time_processed(),
        tlogger=main_logger
    )
    time_save_processed_data_pqsql = get_current_system_time_processed()

    if inserted_rows_amt == 0:
        main_logger.error(f"Saving processed data into database ended with 0 results at {time_save_processed_data_pqsql}.")
        main_logger.error("TTC Tasks stopped. Flow is running.")
        return
    main_logger.info(f"Process to save processed data into database ended successfully at {time_save_processed_data_pqsql}.")
    main_logger.info(f"See results at the table 'dev.{fact_item_table}' of the database '{db_coded_name}'.")

    # creating graphs based on accumulated data in the database
    graphs_paths = get_data_from_pqsql_create_graphs(
        tdatabase_url=database_url,
        tdb_coded_name=db_coded_name,
        tfact_table=fact_item_table,
        tfact_item_table_schema=fact_item_table_schema,
        tsave_dir=ttc_save_graph_dir,
        tlogger=main_logger
    )
    time_get_data_from_pqsql_create_graphs = get_current_system_time_processed()
    if len(graphs_paths) == 0:
        main_logger.error(f"Recreating graphs based on new database info ended with 0 results at {time_get_data_from_pqsql_create_graphs}.")
        main_logger.error("TTC Tasks stopped. Flow is running.")
        return
    main_logger.info(f"Process to recreate graphs based on new database info ended successfully at {time_get_data_from_pqsql_create_graphs}.")
    main_logger.info("Graphs recreated:")
    for graph_path in graphs_paths:
        main_logger.info(f"Graph: {graph_path}")

    # process finish log
    main_logger.info(f"All TTC Tasks executed successfully. See log at {main_logger_path}. Flow is running.")


# ── Main ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ttc_scraper_flow.serve(
        name="ttc-every-day-at-8-pm",
        cron="10 17 * * *"
    )
