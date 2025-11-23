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