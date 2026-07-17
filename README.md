# Market Pulse On Test Data (Tamriel Trade Centre)

A full ETL pipeline built using Prefect and Selenium that starts at a scheduled time and fetches certain item listings from Tamriel Trade Centre (TTC), then processes them and stores them in a PostgreSQL database and JSON files (simply as a backup).

---

## Features

1. Automated scraping using Selenium (headless Chrome) with retry logic. 
2. Data processing - extracts and cleans raw HTML data.
3. JSON storage - saves each run’s data with a timestamp in its JSON filename.
4. PostgreSQL loading - stores data in a fact table, handling all conversions, timestamps, and mappings (scd2 table).
5. Prefect orchestration - the main flow is scheduled to run at a scheduled time, with task retries, logging, and flow‑level error handling.
6. Logging - rotating file logs and console output for each run.

---

## Installation

1. Clone or download the repository
2. Navigate to the project directory
3. Ensure you have Python 3 installed
4. Install all the required packages from requirements.txt:
```bash
pip install -r requirements.txt
```

**The code uses webdriver-manager to automatically download and manage ChromeDriver. No manual setup required.**


## Usage

### Start the Prefect server

Prefect requires a server to run flows with scheduling. Start it via the command:

```bash
prefect server start
```

The Prefect server and UI will start on http://127.0.0.1:4200 by default. Stop it at any time with Ctrl+C.

**If you are starting the server for the first time: after the server is running, set the Prefect API URL via:**

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

### Start the flow

Run on a schedule (execute once while the server is running):

```bash
python entry_point.py
```

### Viewing the Prefect UI

Open http://127.0.0.1:4200 (by default) in your browser. You can see runs, logs, and manage deployments.

### Viewing the results
You can see results at the directory 'output/' inside the project:
- graphs_ttc/ -> TTC records graphs recreated with the current date;
- json_data_ttc/ -> backup data in JSON files;
- logs/ -> process logs.

---

### Project Structure

```plain
├── entry_point.py                          # Main Prefect flow with schedule
├── ttc_get_data.py                         # Selenium scraping and data processing
├── ttc_save_data_json.py                   # Saving data to JSON files
├── ttc_save_data_db.py                     # Saving data to PostgreSQL
├── config.py                               # Variables storage
├── process_time.py                         # Utility to get formatted current time
├── logger.py                               # Custom logger with rotating file and console
├── output/                                 # Created automatically; contains logs and JSON data
│   ├── logs/                               # Rotating log files
│   ├── json_data_ttc/                      # JSON backups
│   └── graphs_ttc/                         # TTC Graphs, artifacts of the process
│       ├── data_amt_trend.html             # Graph: Accumulated Data Trends
│       ├── top_5_cts_by_wrs_amt.html       # Graph: Top 5 Cities By Wares Amount
│       ├── top_5_plrs_by_wrs_amt.html      # Graph: Top 5 Players By Wares Amount
│       ├── top_5_plrs_by_wrs_sum.html      # Graph: Top 5 Players By Wares Sum
│       └── top_5_rgns_by_wrs_amt.html      # Graph: Top 5 Regions By Wares Amount
├── documentation/                          # Other project documentation
│   └── marketpulsedbs.txt  
└── README.md
```
