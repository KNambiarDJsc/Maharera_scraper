import argparse
import asyncio
import logging
import os
import pandas as pd

from playwright.async_api import async_playwright, Page
from playwright_stealth import stealth

from modules.captcha_solver import CaptchaSolver
from modules.data_extractor import DataExtracter


# ---------------------------
# LOGGING CONFIG
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("MahaReraScraper")


# ---------------------------
# CONSTANTS
# ---------------------------
BASE_URL = "https://maharerait.maharashtra.gov.in/public/project/view/"
OUTPUT_FILENAME = "single_project_output.csv"

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
    "authorised_signatory_names", "authorised_signatory_designations", "spa_name", "spa_designation",
    "architect_names", "engineer_names", "chartered_accountant_names", "other_professional_names",
    "sro_name", "sro_document_name", "latest_form1_date", "latest_form2_date", "latest_form5_date",
    "has_occupancy_certificate", "promoter_is_landowner", "has_other_landowners", "landowner_names",
    "landowner_types", "landowner_share_types", "building_identification_plan",
    "wing_identification_plan", "sanctioned_floors", "sanctioned_habitable_floors",
    "sanctioned_apartments", "cc_issued_floors", "view_document_available",
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

async def save_record(data: dict):
    """Saves a single project record to a CSV."""
    df = pd.json_normalize([data])
    df = df.reindex(columns=DESIRED_ORDER)
    file_exists = os.path.exists(OUTPUT_FILENAME)
    df.to_csv(OUTPUT_FILENAME, mode='a', index=False, header=not file_exists)

async def process_single_project(page: Page, captcha_solver: CaptchaSolver,
                                 data_extractor: DataExtracter, project_id: int, url: str) -> bool:
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
            logger.warning(f"CAPTCHA failed for {project_id}")
            return False

        await page.wait_for_load_state("networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        data = await data_extractor.extract_project_details(page, str(project_id))

        if data:
            data["project_id"] = project_id
            await save_record(data)
            return True

        return False

    except Exception as e:
        logger.error(f"Error scraping {project_id}: {e}")
        return False

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

async def create_chromium_context(playwright):
    """Creates Chromium browser, context and page with stealth mode."""
    browser = await playwright.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage"
        ]
    )

    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    )

    page = await context.new_page()
    await stealth(page)

    # Block images, fonts, videos for speed
    await page.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in ["image", "stylesheet", "font", "media"]
        else route.continue_()
    )

    return browser, context, page


async def normal_worker(playwright, project_queue, retry_queue, captcha_solver, data_extracter):
    # Create a fresh Chromium context + page for this worker
    browser, context, page = await create_chromium_context(playwright)

    try:
        while True:
            project_id = await project_queue.get()
            url = f"{BASE_URL}{project_id}"
            logger.info(f"[NORMAL] Processing {project_id}")

            try:
                ok = await process_single_project(page, captcha_solver, data_extracter, project_id, url)

                if not ok:
                    # Log failure + queue for retry
                    await log_failed_project(project_id, url)
                    await retry_queue.put(project_id)

            except Exception as e:
                logger.error(f"[NORMAL] Unexpected error for {project_id}: {e}")
                await log_failed_project(project_id, url)
                await retry_queue.put(project_id)

            finally:
                project_queue.task_done()

    except asyncio.CancelledError:
        logger.info("[NORMAL] Worker cancelled.")
    finally:
        await browser.close()
    

async def retry_worker(playwright, retry_queue, captcha_solver, data_extracter):
    browser, context, page = await create_chromium_context(playwright)

    try:
        while True:
            project_id = await retry_queue.get()
            url = f"{BASE_URL}{project_id}"
            logger.info(f"[RETRY] Retrying {project_id}")

            try:
                ok = await process_single_project(page, captcha_solver, data_extracter, project_id, url)

                if ok:
                    logger.info(f"[RETRY] Success on retry: {project_id}")
                    # Remove from failed.csv since retry succeeded
                    await remove_from_failed(project_id)
                else:
                    logger.warning(f"[RETRY] Failed again: {project_id}")
                    await asyncio.sleep(1)
                    await retry_queue.put(project_id)

            except Exception as e:
                logger.error(f"[RETRY] Unexpected crash for {project_id}: {e}")
                await asyncio.sleep(1)
                await retry_queue.put(project_id)

            finally:
                retry_queue.task_done()

    except asyncio.CancelledError:
        logger.info("[RETRY] Worker cancelled.")
    finally:
        await browser.close()


async def main():
    logger.info("--- Starting Single-ID MahaRERA Scraper (Chromium Version) ---")

    # Ask user for RERA ID
    project_id = input("Enter the RERA Project ID to scrape: ").strip()

    if not project_id.isdigit():
        logger.error("Invalid project ID. Must be a number.")
        return

    project_id = int(project_id)
    url = f"{BASE_URL}{project_id}"

    captcha_solver = CaptchaSolver()
    data_extracter = DataExtracter()

    async with async_playwright() as p:
        # Create chromium browser
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage"
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0 Safari/537.36"
        )
        page = await context.new_page()
        await stealth(page)

        # Process single ID
        success = await process_single_project(page, captcha_solver, data_extracter, project_id, url)

        if success:
            logger.info(f"SUCCESS: Scraped project ID {project_id}")
        else:
            logger.error(f"FAILED to scrape project ID {project_id}")

        await browser.close()

    logger.info("--- SINGLE SCRAPE COMPLETE ---")


if __name__ == "__main__":
    asyncio.run(main())
