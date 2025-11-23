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

    async def _extract_all_tab_data(self, page: Page) -> Dict[str, Any]:
        self.logger.info("--- Starting Robust Sequential Tab Extraction ---")
        all_tab_data: Dict[str, Any] = {}
        TAB_SELECTOR_MAP = {
            "Partner Details": "partner_details",
            "Director Details": "partner_details",  # <-- same logic
            "Promoter Past Experience": "promoter_past_experience",
            "Authorised Signatory": "authorised_signatory",
            "Single Point of Contact":"single_point_of_contact",
            "Project Professionals": "project_professionals",
            "SRO Details": "sro_details",
        }
        SKIP_TABS = [ "Allottee Grievance"]
        try:
            tab_buttons = await page.locator(".tabs button").all()
            self.logger.info(f"Found {len(tab_buttons)} tab buttons.")

            for idx, btn in enumerate(tab_buttons, start=1):
                try:
                    raw_name = (await btn.text_content()) or ""
                except Exception as e:
                    self.logger.warning(f"Could not get text for tab button #{idx}: {e}")
                    continue

                tab_name = raw_name.strip()
                if not tab_name:
                    continue

                # Skip if it's in SKIP_TABS
                if any(skip in tab_name for skip in SKIP_TABS):
                    continue

                matched_key = next((k for k in TAB_SELECTOR_MAP if k.lower() in tab_name.lower()), None)
                if not matched_key:
                    continue

                # Safe click only if visible & enabled
                try:
                    if not await btn.is_visible():
                        self.logger.info(f"Skipping hidden tab '{tab_name}'")
                        continue
                    await btn.scroll_into_view_if_needed()
                    await btn.click(force=True)
                except Exception as e:
                    self.logger.warning(f"Could not click tab '{tab_name}': {e}")
                    continue

                try:
                    # Agar Promoter Past Experience hai to extra wait de
                    extra_timeout = 12000 if matched_key == "Promoter Past Experience" else 5000

                    tabs_container = btn.locator("xpath=ancestor::div[contains(@class,'tabs')]")
                    sibling_candidates = [
                        tabs_container.locator("xpath=following-sibling::div[1]"),
                        tabs_container.locator("xpath=following-sibling::div[2]")
                    ]
                    table_locator = None
                    for sib in sibling_candidates:
                        candidate_table = sib.locator("xpath=.//table").first
                        try:
                            await candidate_table.wait_for(state="visible", timeout=extra_timeout)
                            table_locator = candidate_table
                            break
                        except:
                            continue
                    if table_locator is None:
                        self.logger.warning(f"No visible table found for tab '{tab_name}'.")
                        continue

                    rows = await table_locator.locator("tbody tr").all() if await table_locator.locator("tbody tr").count() > 0 else await table_locator.locator("tr").all()

                    if matched_key in ["Partner Details", "Director Details"]:
                        names, desigs = [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                names.append((await cells[1].text_content() or "").strip())
                                desigs.append((await cells[2].text_content() or "").strip())
                        all_tab_data["partner_name"] = (all_tab_data.get("partner_name", "") + 
                                                        (", " if all_tab_data.get("partner_name") else "") +
                                                        ", ".join(filter(None, names)))
                        all_tab_data["partner_designation"] = (all_tab_data.get("partner_designation", "") +
                                                            (", " if all_tab_data.get("partner_designation") else "") +
                                                            ", ".join(filter(None, desigs)))

                    elif matched_key == "Promoter Past Experience":
                        names, statuses, litigations = [], [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 5:
                                names.append((await cells[1].text_content() or "").strip())
                                statuses.append((await cells[4].text_content() or "").strip())
                                litigations.append((await cells[5].text_content() or "").strip())
                        all_tab_data["promoter_past_project_names"] = ", ".join(filter(None, names))
                        all_tab_data["promoter_past_project_statuses"] = ", ".join(filter(None, statuses))
                        all_tab_data["promoter_past_litigation_statuses"] = ", ".join(filter(None, litigations))

                    elif matched_key == "Authorised Signatory":
                        names, desigs = [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                names.append((await cells[1].text_content() or "").strip())
                                desigs.append((await cells[2].text_content() or "").strip())
                        all_tab_data["authorised_signatory_names"] = ", ".join(filter(None, names))
                        all_tab_data["authorised_signatory_designations"] = ", ".join(filter(None, desigs))


                    elif matched_key =="Single Point of Contact":
                        spa_names, spa_desigs = [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                spa_names.append((await cells[1].text_content() or "").strip())
                                spa_desigs.append((await cells[2].text_content() or "").strip())
                        all_tab_data["spa_name"] = ", ".join(filter(None, spa_names))
                        all_tab_data["spa_designation"] = ", ".join(filter(None, spa_desigs))

                    elif matched_key == "Project Professionals":
                        architects, engineers, chartered_accountants, others = [], [], [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                prof_type = (await cells[1].text_content() or "").strip().lower()
                                prof_name = (await cells[2].text_content() or "").strip()

                                if "architect" in prof_type:
                                    architects.append(prof_name)
                                elif "engineer" in prof_type:
                                    engineers.append(prof_name)
                                elif "chartered accountant" in prof_type:
                                    chartered_accountants.append(prof_name)
                                else:
                                    others.append(prof_name)

                        all_tab_data["architect_names"] = ", ".join(filter(None, architects))
                        all_tab_data["engineer_names"] = ", ".join(filter(None, engineers))
                        all_tab_data["chartered_accountant_names"] = ", ".join(filter(None, chartered_accountants))
                        all_tab_data["other_professional_names"] = ", ".join(filter(None, others))


                    elif matched_key == "SRO Details":
                        sro_names, doc_names = [], []
                        for row in rows:
                            cells = await row.locator("td").all()
                            if len(cells) > 2:
                                sro_names.append((await cells[1].text_content() or "").strip())
                                doc_names.append((await cells[2].text_content() or "").strip())
                        all_tab_data["sro_name"] = ", ".join(filter(None, sro_names))
                        all_tab_data["sro_document_name"] = ", ".join(filter(None, doc_names))

                except Exception as e:
                    self.logger.warning(f"Could not process data in tab '{tab_name}': {e}")
            return all_tab_data
        except Exception as e:
            self.logger.error(f"Fatal error during tab extraction: {e}")
            return {}

    async def _extract_latest_form_dates(self, page: Page) -> Dict[str, Optional[str]]:
        latest_dates = {
            "latest_form1_date": None,
            "latest_form2_date": None,
            "latest_form5_date": None,
            "has_occupancy_certificate": False  # <-- new boolean field
        }
        try:
            from datetime import datetime

            button = page.locator('h2#headingOne >> button[aria-controls="documentLibrary"]')
            await button.wait_for(state="visible", timeout=7000)

            table = page.locator('div#documentLibrary table')
            if not await table.is_visible():
                await button.scroll_into_view_if_needed()
                await button.click()
                await table.wait_for(state="visible", timeout=5000)

            rows = await table.locator('tbody tr').all()

            # Track latest dates for Form 1, Form 2, Form 5
            parsed_dates = { "Form 1": None, "Form 2": None, "Form 5": None }

            for row in rows:
                cells = await row.locator('td').all()
                if len(cells) >= 4:
                    document_type = (await cells[1].text_content() or "").strip()
                    created_date_str = (await cells[3].text_content() or "").strip()

                    # Check for Occupancy Certificate
                    if "occupancy certificate" in document_type.lower():
                        latest_dates["has_occupancy_certificate"] = True

                    try:
                        current_date = datetime.strptime(created_date_str, '%d/%m/%Y, %I:%M %p')
                        for form_name in parsed_dates.keys():
                            if form_name in document_type:
                                if parsed_dates[form_name] is None or current_date > parsed_dates[form_name]:
                                    parsed_dates[form_name] = current_date
                    except ValueError:
                        continue

            if parsed_dates["Form 1"]:
                latest_dates["latest_form1_date"] = parsed_dates["Form 1"].strftime('%d/%m/%Y, %I:%M %p')
            if parsed_dates["Form 2"]:
                latest_dates["latest_form2_date"] = parsed_dates["Form 2"].strftime('%d/%m/%Y, %I:%M %p')
            if parsed_dates["Form 5"]:
                latest_dates["latest_form5_date"] = parsed_dates["Form 5"].strftime('%d/%m/%Y, %I:%M %p')

            return latest_dates

        except Exception as e:
            self.logger.error(f"Could not extract form dates: {e}")
            return latest_dates

    async def extract_promoter_landowner_details(self, page: Page) -> Dict[str, Any]:
        landowner_data = { "promoter_is_landowner": False, "has_other_landowners": False, "landowner_names": None, "landowner_types": None, "landowner_share_types": None }
        try:
            container = page.locator('div.white-box:has-text("Promoter Landowner")')
            await container.wait_for(state="visible", timeout=7000)
            promoter_checkbox = container.locator('div.form-check1:has(label:text-is("Promoter")) input[type="checkbox"]')
            other_landowners_checkbox = container.locator('div.form-check1:has(label:text-is("Promoter Landowner(s)")) input[type="checkbox"]')
            landowner_data["promoter_is_landowner"] = await promoter_checkbox.is_checked()
            landowner_data["has_other_landowners"] = await other_landowners_checkbox.is_checked()
            if landowner_data["has_other_landowners"]:
                table = container.locator("div.table-responsive > table")
                await table.wait_for(state="visible", timeout=5000)
                rows = table.locator("tbody tr")
                row_count = await rows.count()
                if row_count == 0 or "no record found" in (await rows.first.inner_text()).lower():
                    return landowner_data
                names, types, shares = [], [], []
                for i in range(row_count):
                    row = rows.nth(i)
                    cells = row.locator("td")
                    if await cells.count() >= 4:
                        name = (await cells.nth(1).text_content() or "").strip()
                        owner_type = (await cells.nth(2).text_content() or "").strip()
                        share_type = (await cells.nth(3).text_content() or "").strip()
                        names.append(name)
                        types.append(owner_type)
                        shares.append(share_type)
                if names:
                    landowner_data["landowner_names"] = ", ".join(filter(None, names))
                    landowner_data["landowner_types"] = ", ".join(filter(None, types))
                    landowner_data["landowner_share_types"] = ", ".join(filter(None, shares))
            return landowner_data
        except Exception as e:
            self.logger.warning(f"Could not extract promoter landowner details: {e}")
            return landowner_data

    async def _extract_investor_flag(self, page: Page) -> Dict[str, Any]:
        result_key = "are_there_investors_other_than_promoter"
        try:
            container = page.locator("div.col-sm-12:has(label:has-text('Are there any Investor other than the Promoter'))")
            await container.wait_for(timeout=7000)
            answer_label = container.locator("label.form-label-preview-text > b")
            answer = (await answer_label.inner_text()).strip()
            return {result_key: answer}
        except Exception as e:
            self.logger.warning(f"Could not extract investor info: {e}")
            return {result_key: None}

    async def _extract_litigation_details(self, page: Page) -> Dict[str, Any]:
        result_key = "litigation_against_project_count"
        try:
            litigation_container = page.locator("div.white-box:has(b:has-text('Litigation Details'))")
            await litigation_container.wait_for(timeout=7000)
            question_container = litigation_container.locator("div:has-text('Is there any litigation against this proposed project :  ')")
            answer_label = question_container.locator("label.form-label-preview-text  ")
            answer_text = (await answer_label.inner_text()).strip().lower()
            if answer_text == "no":
                return {result_key: 0}
            table = litigation_container.locator("div.table-responsive > table")
            await table.wait_for(timeout=5000)
            rows = table.locator("tbody > tr")
            row_count = await rows.count()
            if row_count == 1 and ("no data" in (await rows.first.text_content() or "").lower() or "no record" in (await rows.first.text_content() or "").lower()):
                return {result_key: 0}
            return {result_key: row_count}
        except Exception as e:
            self.logger.warning(f"Could not extract litigation info: {e}")
            return {result_key: None}

    async def _extract_building_details(self, page: Page) -> Dict[str, Any]:
        def normalize(text: str) -> str:
            return " ".join(text.split()).strip().lower()
        header_key_map = {
            "Identification of Building/ Wing as per Sanctioned Plan": "building_identification_plan",
            "Identification of Wing as per Sanctioned Plan": "wing_identification_plan",
            "Number of Sanctioned Floors (Including Basement+ Stilt+ Podium+ Service+ Habitable excluding terrace)": "sanctioned_floors",
            "Total No. of Building Sanctioned Habitable Floor": "sanctioned_habitable_floors",
            "Sanctioned Apartments / Unit (NR+R)": "sanctioned_apartments",
            "CC Issued up-to (No. of Floors)": "cc_issued_floors",
            "View": "view_document_available"
        }
        normalized_header_map = {normalize(k): v for k, v in header_key_map.items()}
        building_data = {key: [] for key in header_key_map.values()}
        try:
            container = page.locator("div.white-box:has(b:has-text('Building Details'))")
            await container.wait_for(timeout=7000)
            table = container.locator("table")
            await table.wait_for(timeout=5000)
            header_elements = await table.locator("thead th").all()
            actual_headers = [(await h.text_content() or "").strip() for h in header_elements]
            actual_headers = [h for h in actual_headers if h != '#']
            rows = await table.locator("tbody tr").all()
            for row in rows:
                if "Total" in (await row.text_content() or ""):
                    continue
                cells = await row.locator("td").all()
                row_cells = cells[1:]
                for i, header_text in enumerate(actual_headers):
                    if i < len(row_cells):
                        cell = row_cells[i]
                        dict_key = normalized_header_map.get(normalize(header_text))
                        if not dict_key:
                            continue
                        if normalize(header_text) == normalize("View"):
                            is_visible = await cell.locator("i.bi-eye-fill").count() > 0
                            building_data[dict_key].append(str(is_visible))
                        else:
                            cell_text = (await cell.text_content() or "").strip()
                            building_data[dict_key].append(cell_text)
            final_data = {key: ", ".join(value) for key, value in building_data.items() if value}
            return final_data
        except Exception as e:
            self.logger.error(f"Could not extract building details: {e}")
            return {key: None for key in header_key_map.values()}

    async def _extract_apartment_summary(self, page: Page) -> Dict[str, Any]:
        all_keys = {
            "summary_identification_building_wing": None, "summary_identification_wing_plan": None,
            "summary_floor_type": None, "summary_total_no_of_residential_apartments": None,
            "summary_total_no_of_non_residential_apartments": None, "summary_total_no_of_apartments_nr_r": None,
            "summary_total_no_of_sold_units": None, "summary_total_no_of_unsold_units": None,
            "summary_total_no_of_booked": None, "summary_total_no_of_rehab_units": None,
            "summary_total_no_of_mortgage": None, "summary_total_no_of_reservation": None,
            "summary_total_no_of_land_owner_investor_share_sale": None,
            "summary_total_no_of_land_owner_investor_share_not_for_sale": None, "total_no_of_apartments": None,
        }
        try:
            container = page.locator("div.white-box:has(b:has-text('Summary of Apartments/Units'))")
            await container.wait_for(timeout=7000)
            table = container.locator("table")
            await table.wait_for(timeout=5000)
            header_elements = await table.locator("thead th").all()
            header_count = len(header_elements)
            if header_count > 10:
                header_map = {
                    "Identification of Building/ Wing as per Sanctioned Plan": "summary_identification_building_wing",
                    "Identification of Wing as per Sanctioned Plan": "summary_identification_wing_plan",
                    "Floor Type": "summary_floor_type",
                    "Total No. Of Residential Apartments/ Units": "summary_total_no_of_residential_apartments",
                    "Total No. Of Non-Residential Apartments/ Units": "summary_total_no_of_non_residential_apartments",
                    "Total Apartments / Unit (NR+R)": "summary_total_no_of_apartments_nr_r",
                    "Total No. of Sold Units": "summary_total_no_of_sold_units",
                    "Total No. of Unsold Units": "summary_total_no_of_unsold_units",
                    "Total No. of Booked": "summary_total_no_of_booked",
                    "Total No. of Rehab Units": "summary_total_no_of_rehab_units",
                    "Total No. of Mortgage": "summary_total_no_of_mortgage",
                    "Total No. of Reservation": "summary_total_no_of_reservation",
                    "Total No. of Land Owner/ Investor Share (For Sale)": "summary_total_no_of_land_owner_investor_share_sale",
                    "Total No. of Land Owner/ Investor Share (Not For Sale)": "summary_total_no_of_land_owner_investor_share_not_for_sale",
                }
                temp_data = {key: [] for key in header_map.values()}
                actual_headers = [(await h.text_content() or "").strip() for h in header_elements if (await h.text_content() or "").strip() != '#']
                rows = await table.locator("tbody tr").all()
                for row in rows:
                    if "Total" in (await row.text_content() or ""): continue
                    cells = await row.locator("td").all()
                    row_cells = cells[1:]
                    for j, header_text in enumerate(actual_headers):
                        if j < len(row_cells):
                            cell = row_cells[j]
                            dict_key = header_map.get(header_text)
                            if dict_key:
                                temp_data[dict_key].append((await cell.text_content() or "").strip())
                for key, values in temp_data.items():
                    all_keys[key] = ", ".join(values)
            elif header_count == 5:
                total_apartments = 0
                rows = await table.locator("tbody tr").all()
                for row in rows:
                    cells = await row.locator("td").all()
                    if len(cells) == 5:
                        try:
                            num_text = (await cells[4].text_content() or "0").strip()
                            if num_text: total_apartments += int(num_text)
                        except (ValueError, IndexError): continue
                all_keys["total_no_of_apartments"] = str(total_apartments)
            return all_keys
        except Exception as e:
            self.logger.error(f"Could not extract apartment summary: {e}")
            return all_keys

    async def _extract_parking_details(self, page: Page) -> Dict[str, Any]:
        results = { "open_space_parking_total": None, "closed_space_parking_total": None }
        try:
            button = page.locator("button:has-text('Parking Details')")
            await button.wait_for(timeout=7000)
            parking_section = page.locator("div#parkingDetails")
            if not "show" in (await parking_section.get_attribute("class") or ""):
                await button.click()
                # FIX: Replaced flaky expect with a more reliable wait for the table inside.
                await parking_section.locator("table").first.wait_for(state="visible", timeout=5000)
            
            tables = parking_section.locator("div.table-responsive > table")
            table_count = await tables.count()
            if table_count == 0:
                return results
            open_counts, closed_counts = [], []
            for i in range(table_count):
                table = tables.nth(i)
                rows = await table.locator("tbody tr").all()
                open_sum_for_table, closed_sum_for_table = 0, 0
                for row in rows:
                    cells = await row.locator("td").all()
                    if len(cells) < 8: continue
                    try:
                        parking_type = (await cells[1].inner_text()).strip().lower()
                        count_text = (await cells[6].inner_text()).strip()
                        count = int(count_text) if count_text.isdigit() else 0
                        if "open" in parking_type: open_sum_for_table += count
                        elif "closed" in parking_type or "covered" in parking_type: closed_sum_for_table += count
                    except (ValueError, IndexError):
                        continue
                open_counts.append(str(open_sum_for_table))
                closed_counts.append(str(closed_sum_for_table))
            results["open_space_parking_total"] = ", ".join(open_counts)
            results["closed_space_parking_total"] = ", ".join(closed_counts)
            return results
        except Exception as e:
            self.logger.warning(f"Could not extract parking details: {e}")
            return results

    async def _extract_bank_details(self, page: Page) -> Dict[str, Optional[str]]:
        result = { "bank_name": None, "ifsc_code": None, "bank_address": None }
        try:
            container = page.locator("project-bank-details-preview fieldset").nth(0)
            await container.wait_for(timeout=7000)
            fields_to_extract = { "Bank Name": "bank_name", "IFSC Code": "ifsc_code", "Bank Address": "bank_address" }
            for label_text, dict_key in fields_to_extract.items():
                try:
                    label_locator = container.locator(f"label.form-label:has-text('{label_text}')")
                    value_locator = label_locator.locator("xpath=following-sibling::div[1]")
                    value = (await value_locator.inner_text()).strip()
                    result[dict_key] = value
                except Exception as inner_e:
                    self.logger.warning(f"Could not find bank field '{label_text}': {inner_e}")
                    continue
            return result
        except Exception as e:
            self.logger.error(f"Failed to extract bank details section: {e}")
            return result


