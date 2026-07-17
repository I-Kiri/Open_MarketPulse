# ttc_save_data_json.py
"""
body for data JSON storage backup implementation
"""

import os
import json

from prefect import task


# ── Helpers ───────────────────────────────────────────────────────────────────────────────────────────────────────────
def set_output_directory(
        tpath,
        tlogger
) -> None:
    """Setting directory to save file"""
    tlogger.info("Setting directory to save file...")
    if not os.path.exists(tpath):
        os.makedirs(tpath)
        tlogger.info(f"Directory to save file created: {tpath}")
    tlogger.info(f"Directory to save file existed: '{tpath}'.")


def create_empy_json(
        tpath,
        tprocessed_time,
        tlogger
) -> str:
    """Creating empty json to save data"""
    tlogger.info("Creating empty json to save data...")
    filename = f"{tprocessed_time.replace(' ', '_').replace(':', '-')}.json"
    filepath = os.path.join(tpath, filename)
    if os.path.exists(filepath):
        tlogger.info(f"Json to save data '{filepath}' already exists and thus will be overwritten to save data anew.")
    tlogger.info(f"Empty json to save data created: '{filepath}'.")
    return filepath


# ── Tasks ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
@task
def save_processed_data_json(
        tpath,
        tprocessed_data,
        tprocessed_time,
        tlogger
) -> (str, int):
    """Saving processed data list into jsons"""
    tlogger.info("Validating processed data to save...")
    if not isinstance(tprocessed_data, list) or len(tprocessed_data) == 0:
        tlogger.error(f"Failed to get processed data to save.")
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise ValueError(f"Expected list with the length > 0, got {type(tprocessed_data).__name__}, {len(tprocessed_data)}")
    tlogger.info("Validating processed time to save...")
    if not isinstance(tprocessed_time, str) or len(tprocessed_time) == 0:
        tlogger.error(f"Failed to get processed time to save.")
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise ValueError(f"Expected string with the length > 0, got {type(tprocessed_time).__name__}, {len(tprocessed_time)}")
    tlogger.info("Processed data and time to save validated.")

    try:
        set_output_directory(tpath=tpath, tlogger=tlogger)
        filepath = create_empy_json(tpath=tpath, tprocessed_time=tprocessed_time, tlogger=tlogger)

        tlogger.info(f"Saving data to file: '{filepath}'...")
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(tprocessed_data, f, ensure_ascii=False, indent=2, sort_keys=False)
                saved_json_len = len(tprocessed_data)
            tlogger.info(f"Saved data to file: '{filepath}'.")
            return filepath, saved_json_len

        except Exception as e:
            tlogger.error(f"Failed to save data to file '{filepath}': {e}", exc_info=True)
            tlogger.error("TTC Tasks stopped. Flow is running.")
            raise

    except Exception as e:
        tlogger.error(f"Failed to save data to json: {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise


# ── Main ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pass
