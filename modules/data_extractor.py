import asyncio
import re
from typing import Dict, List, Optional, Any
import logging
from playwright.async_api import Page, expect

logger = logging.getLogger(__name__)

class DataExtracter:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def extract_project_details(self, page: Page, reg_no: str) -> Optional[Dict[str, Any]]:
        """Extract comprehensive project details from the MahaRERA project page."""
        try:
            await page.wait_for_selector("div.form-card", timeout=10000)
            data = {'reg_no': reg_no}

            tasks = [
                self._extract_registration_block(page),
                self._extract_project_details_block(page),
                self._extract_planning_authority_block(page),
                self._extract_planning_land_block(page),
                self._extract_commencement_certificate(page),
                self._extract_project_address(page),
                self._extract_promoter_details(page),
                self._extract_promoter_address(page),
                self._extract_all_tab_data(page),
                self._extract_latest_form_dates(page),
                self.extract_promoter_landowner_details(page),
                self._extract_investor_flag(page),
                self._extract_litigation_details(page),
                self._extract_building_details(page),
                self._extract_apartment_summary(page),
                self._extract_parking_details(page),
                self._extract_bank_details(page),
                self._extract_complaint_details(page),
                self._extract_real_estate_agents(page)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.warning(f"A data block extraction failed for {reg_no}: {result}")
                elif result:
                    data.update(result)

            return data

        except Exception as e:
            self.logger.error(f"Fatal error extracting data for {reg_no}: {e}")
            return None

    async def _extract_registration_block(self, page: Page) -> Dict[str, str]:
        try:
            reg_number = await page.locator("label[for='yourUsername']:has-text('Registration Number')").locator("xpath=following-sibling::label[1]").inner_text(timeout=5000)
            reg_date = await page.locator("label[for='yourUsername']:has-text('Date of Registration')").locator("xpath=following-sibling::label[1]").inner_text(timeout=5000)

            result = {
                'registration_number': reg_number.strip(),
                'date_of_registration': reg_date.strip()
            }

            self.logger.info(f"Extracted Registration Block: {result}")
            return result
        except Exception as e:
            self.logger.warning(f"Could not extract Registration Block: {e}")
            return {}

    async def _extract_project_details_block(self, page: Page) -> Dict[str, str]:
        data = {}

        fields = {
            'project_name': "Project Name",
            'project_type': "Project Type",
            'project_location': "Project Location",
            'proposed_completion_date': "Proposed Completion Date (Original)"
        }

        try:
            for key, label in fields.items():
                
                await page.wait_for_selector("div:has-text('Project Name')", timeout=10000)
                locator = page.locator(f"div:text-is('{label}')").nth(0)
                value_locator = locator.locator("xpath=following-sibling::div[1]")
                value = await value_locator.inner_text(timeout=5000)
                data[key] = value.strip()

            try:
                ext_label = "Proposed Completion Date (Revised)"
                ext_locator = page.locator(f"div:text-is('{ext_label}')").nth(0)
                ext_value_locator = ext_locator.locator("xpath=following-sibling::div[1]")
                ext_value = await ext_value_locator.inner_text(timeout=3000)
                data['extension_date'] = ext_value.strip()
            except Exception:
                data['extension_date'] = None

            try:
                status_label = page.locator("span:text-is('Project Status')").first
                status_value = await status_label.locator("xpath=../../following-sibling::div[1]//span").inner_text(timeout=3000)
                data['project_status'] = status_value.strip()
            except Exception:
                data['project_status'] = None

            self.logger.info(f"Extracted Project Details: {data}")
            return data

        except Exception as e:
            self.logger.warning(f"Could not extract Project Details Block: {e}")
            return {}

    async def _extract_planning_authority_block(self, page: Page) -> Dict[str, Optional[str]]:
        data = {
            "planning_authority": None,
            "full_name_of_planning_authority": None
        }
        try:
            container = page.locator('div.row:has-text("Planning Authority")').first
            await container.wait_for(timeout=5000)
            try:
                label_pa = container.locator('span:has-text("Planning Authority")')
                value_pa_locator = label_pa.locator("xpath=./ancestor::div[contains(@class, 'col-12 text-font')]/following-sibling::div[1]/p").first
                value_pa = await value_pa_locator.inner_text()
                data["planning_authority"] = value_pa.strip() if value_pa else None
            except Exception as e:
                self.logger.warning(f"Could not extract 'Planning Authority' value: {e}")
            try:
                label_fn = container.locator('span:has-text("Full Name of the Planning Authority")')
                value_fn_locator = label_fn.locator("xpath=./ancestor::div[contains(@class, 'col-12 text-font')]/following-sibling::div[1]/p").first
                value_fn = await value_fn_locator.inner_text()
                data["full_name_of_planning_authority"] = value_fn.strip() if value_fn else None
            except Exception as e:
                self.logger.warning(f"Could not extract 'Full Name of the Planning Authority' value: {e}")
            return data
        except Exception as e:
            self.logger.error(f"Could not find or process the Planning Authority block: {e}")
            return data


    async def _extract_planning_land_block(self, page: Page) -> Dict[str, Optional[str]]:
        data = {}
        try:
            field_map = {
                'final_plot_bearing': "Final Plot bearing No/CTS Number/Survey Number",
                'total_land_area': "Total Land Area of Approved Layout (Sq. Mts.)",
                'land_area_applied': "Land Area for Project Applied for this Registration (Sq. Mts)",
                'permissible_builtup': "Permissible Built-up Area",
                'sanctioned_builtup': "Sanctioned Built-up Area of the Project applied for Registration",
                'aggregate_open_space': "Aggregate area(in sq. mts) of recreational open space as per Layout / DP Remarks"
            }
            section_card = page.locator("div.card-header:has-text('Land Area & Address Details')").first
            form_card = section_card.locator("xpath=ancestor::div[contains(@class, 'form-card')]").first
            await form_card.wait_for(timeout=5000)
            white_boxes = form_card.locator("div.white-box")
            count = await white_boxes.count()
            for key, expected_label in field_map.items():
                found = False
                for i in range(count):
                    box = white_boxes.nth(i)
                    try:
                        label = await box.locator("label").inner_text()
                        if expected_label.strip() in label.strip():
                            value_div = box.locator("div.text-font.f-w-700")
                            await value_div.wait_for(timeout=2000)
                            value = await value_div.inner_text()
                            data[key] = value.strip()
                            found = True
                            break
                    except Exception:
                        continue
                if not found:
                    data[key] = None
                    self.logger.warning(f"Label '{expected_label}' not found in Planning/Land block.")
            return data
        except Exception as e:
            self.logger.warning(f"Could not extract Planning/Land Block at all: {e}")
            return {}

    async def _extract_commencement_certificate(self, page: Page) -> Dict[str, str]:
        data = { "CC/NA Order Issued to": "", "CC/NA Order in the name of": "" }
        try:
            section = page.locator("div:has(h5.card-title.mb-0:has-text('Commencement Certificate / NA Order Documents Details'))")
            divOfTable=section.locator("xpath=following-sibling::div[1]");
            table = divOfTable.locator("table:has-text('CC/NA Order Issued to')")
            await table.wait_for(timeout=5000)
            rows = table.locator("tbody tr")
            count = await rows.count()
            if count == 0 or "No-Data-Found" in (await rows.first.inner_text()):
                self.logger.info("No Commencement Certificate data found in the table.")
                return data
            col2_values, col3_values = [], []
            for i in range(count):
                row = rows.nth(i)
                try:
                    col2 = await row.locator("td:nth-child(2)").inner_text()
                    col3 = await row.locator("td:nth-child(3)").inner_text()
                    col2_values.append(col2.strip())
                    col3_values.append(col3.strip())
                except Exception as e:
                    self.logger.warning(f"Could not process a row in Commencement Certificate table: {e}")
                    continue
            data["CC/NA Order Issued to"] = ", ".join(col2_values)
            data["CC/NA Order in the name of"] = ", ".join(col3_values)
            return data
        except Exception as e:
            self.logger.warning(f"Could not extract Commencement Certificate details: {e}")
            return data

    async def _extract_project_address(self, page: Page) -> Dict[str, str]:
        target_labels = ["State/UT", "District", "Taluka", "Village", "Pin Code"]
        results = {}

        try:
            header = page.locator("h5.card-title:has-text('Project Address Details')")
            await header.wait_for(timeout=10000)
            section = header.locator("xpath=ancestor::div[contains(@class, 'white-box')]")

            for label in target_labels:
                key_name = f"project_address_{label.lower().replace('/', '_').replace(' ', '_')}"
                try:
                    label_locator = section.locator(f"label.form-label:has-text('{label}')")
                    if not await label_locator.count():
                        results[key_name] = None
                        continue

                    value_locator = label_locator.locator("xpath=following-sibling::*[1]")
                    child_div_locator = value_locator.locator("div")

                    if await child_div_locator.count():
                        await child_div_locator.first.wait_for(timeout=3000)
                        value_text = (await child_div_locator.first.text_content() or "").strip()
                    else:
                        value_text = None

                    results[key_name] = value_text if value_text else None
                except Exception:
                    results[key_name] = None

        except Exception as e:
            self.logger.warning(f"Could not extract some location fields: {e}")
            # Ensure all keys are present in case of complete failure
            for label in target_labels:
                key_name = f"project_address_{label.lower().replace('/', '_').replace(' ', '_')}"
                results.setdefault(key_name, None)

        return results

    async def _extract_promoter_details(self, page: Page) -> Dict[str, str]:
        try:
            header = page.locator("h5.card-title:has-text('Promoter Details')").first
            await header.wait_for(timeout=10000)
            section = header.locator("xpath=ancestor::fieldset[1]")
            await section.wait_for(timeout=5000)
            outer_row = section.locator("xpath=.//div[contains(@class,'row')][.//label]").first
            cols = outer_row.locator("xpath=.//div[contains(@class,'col')][.//label]")
            total_cols = await cols.count()
            details = []
            for i in range(total_cols):
                col = cols.nth(i)
                label_loc = col.locator("label")
                if await label_loc.count() == 0:
                    continue
                label_text = (await label_loc.first.text_content() or "").strip().rstrip(":")
                value_loc = col.locator("xpath=.//*[self::div or self::span][normalize-space(string(.))!=''][1]")
                raw_value_text = (await value_loc.first.text_content() or "").strip() if await value_loc.count() > 0 else ""
                value_text = raw_value_text.replace(label_text, "").strip()
                if label_text and value_text:
                    details.append(f"{label_text} - {value_text}")
            promoter_details_str = ", ".join(details) if details else None
            return {"promoter_details": promoter_details_str}
        except Exception as e:
            self.logger.warning(f"Could not extract Promoter Details: {e}")
            return {"promoter_details": None}

    async def _extract_promoter_address(self, page: Page) -> Dict[str, str]:
        address_details = {}
        try:
            header = page.locator("h5:has-text('Promoter Official Communication Address')")
            section = header.locator("xpath=ancestor::fieldset[1]")
            await section.wait_for(timeout=5000)
            fields_to_extract = ['State/UT', 'District', 'Taluka', 'Village', 'Pin Code']
            for field in fields_to_extract:
                label_locator = section.locator(f"label:has-text('{field}')")
                value_text = None
                if await label_locator.count() > 0:
                    value_locator = label_locator.locator("xpath=./following-sibling::div/div")
                    if await value_locator.count() > 0:
                        value_text = (await value_locator.first.text_content() or "").strip()
                key_suffix = re.sub(r'[^a-z0-9_]', '', field.lower().replace(' ', '_').replace('/', '_'))
                dict_key = f"promoter_official_communication_address_{key_suffix}"
                address_details[dict_key] = value_text
            return address_details
        except Exception as e:
            self.logger.warning(f"Could not extract Promoter Address details: {e}")
            return { f"promoter_official_communication_address_{re.sub(r'[^a-z0-9_]', '', field.lower().replace(' ', '_').replace('/', '_'))}": None for field in fields_to_extract }



        