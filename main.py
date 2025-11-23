import asyncio
import logging
import os
import csv
import pandas as pd
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
from modules.captcha_solver import CaptchaSolver
from modules.data_extracter import DataExtracter
from typing import Set, Optional

# --- Configuration ---
# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MahaReraScraper")

# --- File Configuration ---
OUTPUT_FILENAME = "maharera_complete_data.csv"
FAILED_PROJECTS_FILENAME = "maharera_failed_projects.csv"

# --- Scraping Configuration ---
START_ID = 401
END_ID = 800
BASE_URL = "https://maharerait.maharashtra.gov.in/public/project/view/"

# --- Worker counts (12 total = 8 normal + 4 retry) ---
TOTAL_WORKERS = 16
RETRY_WORKERS = 5
NORMAL_WORKERS = TOTAL_WORKERS - RETRY_WORKERS


# --- Column order for the final CSV ---
DESIRED_ORDER = [
    "project_id", "registration_number", "date_of_registration", "project_name",
    "project_type", "project_location", "proposed_completion_date",
    "extension_date", "project_status", "planning_authority",
    "full_name_of_planning_authority", "final_plot_bearing",
    "total_land_area", "land_area_applied", "permissible_builtup",
    "sanctioned_builtup", "aggregate_open_space", "CC/NA Order Issued to",
    "CC/NA Order in the name of", "project_address_state_ut",
    "project_address_district", "project_address_taluka",
    "project_address_village", "project_address_pin_code", "promoter_details",
    "promoter_official_communication_address_state_ut",
    "promoter_official_communication_address_district",
    "promoter_official_communication_address_taluka",
    "promoter_official_communication_address_village",
    "promoter_official_communication_address_pin_code", "partner_name",
    "partner_designation", "promoter_past_project_names",
    "promoter_past_project_statuses", "promoter_past_litigation_statuses",
    "authorised_signatory_names", "authorised_signatory_designations","spa_name","spa_designation",
    "architect_names", "engineer_names", "chartered_accountant_names","other_professional_names",
    "sro_name", "sro_document_name", "latest_form1_date", "latest_form2_date","latest_form5_date","has_occupancy_certificate",
    "promoter_is_landowner", "has_other_landowners", "landowner_names",
    "landowner_types", "landowner_share_types", "building_identification_plan",
    "wing_identification_plan", "sanctioned_floors",
    "sanctioned_habitable_floors", "sanctioned_apartments",
    "cc_issued_floors", "view_document_available",
    "summary_identification_building_wing", "summary_identification_wing_plan",
    "summary_floor_type", "summary_total_no_of_residential_apartments",
    "summary_total_no_of_non_residential_apartments",
    "summary_total_no_of_apartments_nr_r", "summary_total_no_of_sold_units",
    "summary_total_no_of_unsold_units", "summary_total_no_of_booked",
    "summary_total_no_of_rehab_units", "summary_total_no_of_mortgage",
    "summary_total_no_of_reservation",
    "summary_total_no_of_land_owner_investor_share_sale",
    "summary_total_no_of_land_owner_investor_share_not_for_sale",
    "total_no_of_apartments", "are_there_investors_other_than_promoter",
    "litigation_against_project_count", "open_space_parking_total",
    "closed_space_parking_total", "bank_name", "ifsc_code", "bank_address",
    "complaint_count", "complaint_numbers", "real_estate_agent_names",
    "maharera_certificate_nos"
]

# --- Thread-safe file writing locks ---
csv_lock = asyncio.Lock()
failed_csv_lock = asyncio.Lock()

async def save_record(data: dict):
    """Appends a single successful record to the main CSV file in a thread-safe manner."""
    async with csv_lock:
        try:
            df = pd.json_normalize([data])
            df = df.reindex(columns=DESIRED_ORDER)
            file_exists = os.path.exists(OUTPUT_FILENAME)
            df.to_csv(OUTPUT_FILENAME, mode='a', index=False, header=not file_exists)
        except Exception as e:
            logger.error(f"Failed to save record for {data.get('project_id')}: {e}")

async def log_failed_project(project_id: int, url: str):
    """Appends a single failed project to the failure CSV file in a thread-safe manner."""
    async with failed_csv_lock:
        try:
            file_exists = os.path.exists(FAILED_PROJECTS_FILENAME)
            with open(FAILED_PROJECTS_FILENAME, 'a', newline='', encoding='utf-8') as f:
                if not file_exists:
                    f.write("project_id,url\n")
                f.write(f"{project_id},{url}\n")
        except Exception as e:
            logger.error(f"Failed to log failed project {project_id}: {e}")  

def get_processed_ids() -> Set[int]:
    """Reads both success and failure CSVs to get a set of all IDs that have been attempted."""
    processed_ids = set()
    for filename in [OUTPUT_FILENAME, FAILED_PROJECTS_FILENAME]:
        if os.path.exists(filename):
            try:
                df = pd.read_csv(filename, usecols=['project_id'], on_bad_lines='skip')
                processed_ids.update(df['project_id'].dropna().astype(int).tolist())
            except (ValueError, KeyError, FileNotFoundError) as e:
                logger.warning(f"Could not read project_id column from {filename}. It might be empty or malformed. Error: {e}")
    return processed_ids

# ---------------- FIXED function with boolean return for reliability ----------------
async def process_single_project(page: Page, captcha_solver: CaptchaSolver, data_extracter: DataExtracter, project_id: int, url: str) -> bool:
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)

        success = await captcha_solver.solve_and_fill(
            page=page,
            captcha_selector="canvas#captcahCanvas",
            input_selector="input[name='captcha']",
            submit_selector="button.btn.btn-primary.next",
            reg_no=str(project_id)
        )

        if not success:
            logger.warning(f"CAPTCHA FAILED for project {project_id}.")
            return False

        await page.wait_for_load_state('networkidle', timeout=30000)
       
        
        await page.wait_for_timeout(2500)

        data = await data_extracter.extract_project_details(page, str(project_id))

        if data:
            data["project_id"] = project_id
            await save_record(data)
            logger.info(f"✅ SUCCESS: Saved data for project {project_id}")
            return True # Return True on successful save
        else:
            logger.warning(f"Data extraction returned None for project {project_id}.")
            return False # Return False if no data is extracted

    except Exception as e:
        logger.error(f"FATAL ERROR processing project {project_id}: {e}", exc_info=False)
        return False # Return False on any exception

# ---------------- Helper functions for retry system ----------------
async def remove_from_failed(project_id: int):
    """Remove a project ID from failed CSV after a successful retry."""
    async with failed_csv_lock:
        if not os.path.exists(FAILED_PROJECTS_FILENAME):
            return
        try:
            rows = []
            fieldnames = ['project_id', 'url']
            with open(FAILED_PROJECTS_FILENAME, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or fieldnames
                for row in reader:
                    if str(row.get('project_id')) != str(project_id):
                        rows.append(row)
            with open(FAILED_PROJECTS_FILENAME, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        except Exception as e:
            logger.error(f"Error removing project {project_id} from failed list: {e}")

async def log_failed_and_enqueue(project_id: int, url: str, retry_queue: asyncio.Queue):
    """Append to failed CSV and push ID to retry queue."""
    await log_failed_project(project_id, url)
    await retry_queue.put(project_id)


async def normal_worker(playwright, project_queue, retry_queue, captcha_solver, data_extracter):
    browser, context, page = await create_chromium_context(playwright)

    try:
        while True:
            project_id = await project_queue.get()
            url = f"{BASE_URL}{project_id}"
            logger.info(f"[NORMAL] Processing {project_id}")

            ok = await process_single_project(page, captcha_solver, data_extracter, project_id, url)

            if not ok:
                await log_failed_project(project_id, url)
                await retry_queue.put(project_id)

            project_queue.task_done()

    except asyncio.CancelledError:
        pass
    finally:
        await browser.close()
    

async def retry_worker(playwright, retry_queue, captcha_solver, data_extracter):
    browser, context, page = await create_chromium_context(playwright)

    try:
        while True:
            project_id = await retry_queue.get()
            url = f"{BASE_URL}{project_id}"
            logger.info(f"[RETRY] Retrying {project_id}")

            ok = await process_single_project(page, captcha_solver, data_extracter, project_id, url)

            if not ok:
                logger.warning(f"[RETRY] Failed again: {project_id}")
                await asyncio.sleep(1)
                await retry_queue.put(project_id)

            retry_queue.task_done()

    except asyncio.CancelledError:
        pass
    finally:
        await browser.close()

async def main():
    """Main function to set up the scraping environment and run workers in parallel."""
    logger.info("--- Starting MahaRERA Scraper (Chromium Version) ---")

    processed_ids = get_processed_ids()
    logger.info(f"Found {len(processed_ids)} previously attempted projects. They will be skipped.")

    project_queue = asyncio.Queue()
    retry_queue = asyncio.Queue()

    # Queue fresh IDs
    for i in range(START_ID, END_ID + 1):
        if i not in processed_ids:
            await project_queue.put(i)

    # Load previous failed attempts
    if os.path.exists(FAILED_PROJECTS_FILENAME):
        try:
            df_failed = pd.read_csv(FAILED_PROJECTS_FILENAME, usecols=['project_id'], on_bad_lines='skip')
            for pid in df_failed['project_id'].dropna().astype(int).tolist():
                await retry_queue.put(pid)
            logger.info(f"Loaded {retry_queue.qsize()} IDs from failed list into retry queue.")
        except Exception as e:
            logger.warning(f"Failed to preload failed IDs: {e}")

    total_to_process = project_queue.qsize()
    logger.info(f"Queued {total_to_process} new projects for scraping.")

    if total_to_process == 0 and retry_queue.qsize() == 0:
        logger.info("No new or failed projects to process. Exiting.")
        return

    captcha_solver = CaptchaSolver()
    data_extracter = DataExtracter()

    async with async_playwright() as p:
        tasks = []

        # Start NORMAL workers (Chromium version)
        for _ in range(NORMAL_WORKERS):
            tasks.append(asyncio.create_task(
                normal_worker(
                    p,
                    project_queue,
                    retry_queue,
                    captcha_solver,
                    data_extracter
                )
            ))

        # Start RETRY workers (Chromium version)
        for _ in range(RETRY_WORKERS):
            tasks.append(asyncio.create_task(
                retry_worker(
                    p,
                    retry_queue,
                    captcha_solver,
                    data_extracter
                )
            ))

        # Wait for NORMAL queue to finish
        await project_queue.join()

        # Now process RETRY queue (but do not wait forever)
        try:
            await asyncio.wait_for(retry_queue.join(), timeout=300.0)
        except asyncio.TimeoutError:
            logger.warning("Retry queue processing timed out. Some projects might remain failed.")

        # Cancel all workers gracefully
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("--- SCRAPING RUN COMPLETE ---")
    logger.info(f"Successful data saved to: {OUTPUT_FILENAME}")
    logger.info(f"Failed/Skipped projects logged in: {FAILED_PROJECTS_FILENAME}")

if __name__ == "__main__":
    asyncio.run(main())
