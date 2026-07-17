# ttc_get_data.py
"""
body for parsing data from TTC page implementation
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from webdriver_manager.chrome import ChromeDriverManager
from pydantic import BaseModel, field_validator, ValidationError

from prefect import task
from prefect.cache_policies import NO_CACHE


# ── Classes ───────────────────────────────────────────────────────────────────────────────────────────────────────────
class TTCItemRowSchema(BaseModel):
    """Pydantic class to validate TTC data"""
    item_name: str
    player_id: str
    guild_name: str
    location: str
    unit_price: str
    amount: str
    total_price: str
    time_elapsed: str

    @field_validator("*")
    @classmethod
    def check_not_empty(cls, v, info):
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"{info.field_name} must not be empty")
        return v.strip()


# ── Helpers ───────────────────────────────────────────────────────────────────────────────────────────────────────────
def setup_driver_options(
        tlogger
) -> webdriver.ChromeOptions:
    """Setup driver options"""
    tlogger.info("Setting driver options up...")
    options = webdriver.ChromeOptions()

    # headless spec
    options.add_argument('--headless=new')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--hide-scrollbars')
    options.add_argument('--disable-gpu')

    # browser options spec
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-notifications')
    options.add_argument('--blink-settings=imagesEnabled=false')

    # stealth spec
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    #options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...')
    #options.add_argument('--proxy-server="http://proxy:port"')

    tlogger.info("Driver options set up.")
    return options


def setup_driver(
        tlogger
) -> webdriver.Chrome:
    """Setup and return driver"""
    tlogger.info("Setting driver...")
    try:
        options = setup_driver_options(tlogger=tlogger)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        tlogger.info("Driver set.")
        return driver
    except Exception as e:
        tlogger.error(f"Failed to set driver: {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise


# ── Tasks ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
@task(retries=3, retry_delay_seconds=3, cache_policy=NO_CACHE)
def open_page(
        tdriver,
        turl,
        tlogger
) -> None:
    """Open page"""
    tlogger.info("Opening page...")
    try:
        tdriver.get(turl)
        tlogger.info(f"Page opened: {turl}")
    except Exception as e:
        tlogger.error(f"Failed to open page {turl}: {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise


@task(cache_policy=NO_CACHE)
def get_processed_data(
        turl,
        tlogger
) -> (list, int):
    """Getting and processing data from the page"""
    driver = setup_driver(tlogger)
    wait = WebDriverWait(driver, 10)
    open_page(driver, turl, tlogger)

    tlogger.info("Getting table body to parse...")
    try:
        tbody = wait.until(
            expected_conditions.presence_of_element_located((By.CSS_SELECTOR, "tbody[data-bind*='TradeDetails']"))
        )
        tlogger.info("Got body to parse.")

        tlogger.info("Defining data to process...")
        raw_data = tbody.find_elements(By.CSS_SELECTOR, "tr.cursor-pointer")
        tlogger.info(f"Data to process defined: {len(raw_data)} rows found.")

        tlogger.info("Processing data...")
        processed_data = process_data(tdata=raw_data, tlogger=tlogger)
        tlogger.info("Data processed.")

        return processed_data, len(processed_data)

    except Exception as e:
        tlogger.error(f"Failed to get body to parse: {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise

    finally:
        tlogger.info("Quiting browser...")
        driver.quit()
        tlogger.info("Browser quit.")


@task(cache_policy=NO_CACHE)
def process_data(
        tdata,
        tlogger
) -> list:
    """Processing raw item data from TTC"""
    tlogger.info("Processing data row by row...")
    processed_data_list = list()
    for i, row in enumerate(tdata):
        tlogger.info(f"Processing row №{i}...")

        try:
            # item name
            item_name = str(row.find_element(By.CSS_SELECTOR, "td div[data-bind*='text: Name']").text)

            # player ID
            player_id = str(row.find_element(By.CSS_SELECTOR, "td.hidden-xs div[data-bind*='text: PlayerID']").text)

            # guild name
            guild_name = str(row.find_element(By.CSS_SELECTOR, "td:nth-child(3) div[data-bind*='text: GuildName']").text)

            # location
            try:
                location = str(row.find_element(By.CSS_SELECTOR, "td:nth-child(3) a.trade-list-clickable").text)
            except:
                location = ""

            # price info
            gold_cell = row.find_element(By.CSS_SELECTOR, "td.gold-amount")
            unit_price = str(gold_cell.find_element(By.CSS_SELECTOR, "span[data-bind*='UnitPrice']").text)
            amount = str(gold_cell.find_element(By.CSS_SELECTOR, "span[data-bind*='Amount']").text)
            total_price = str(gold_cell.find_element(By.CSS_SELECTOR, "span[data-bind*='TotalPrice']").text)

            # time
            time_elapsed = str(row.find_element(By.CSS_SELECTOR, "td.bold.hidden-xs[data-bind*='minutesElapsed']").text)

            # creating dict_to_append
            dict_to_append = {
                "item_name": item_name,
                "player_id": player_id,
                "guild_name": guild_name,
                "location": location,
                "unit_price": unit_price,
                "amount": amount,
                "total_price": total_price,
                "time_elapsed": time_elapsed
            }

            # validating rows
            try:
                dict_to_append_checked = TTCItemRowSchema(**dict_to_append)

                # dict appended
                processed_data_list.append(dict_to_append_checked.model_dump())
                tlogger.info("Row processed.")
            except ValidationError as e:
                print("Row have errors:")
                for err in e.errors():
                    print(f"    {err['loc'][0]}: {err['msg']}")
                print("Row skipped.")
                continue

        except Exception as e:
            tlogger.error("Couldn't process row:", e)

    tlogger.info("All given data processed.")
    return processed_data_list


# ── Main ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pass
