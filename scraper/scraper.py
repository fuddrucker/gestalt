import os
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth  # <-- Import the new Stealth class
from urllib.parse import urljoin, urlparse
import requests
#import the time module to add delays between requests
import json
import time
import os
import boto3
import re
import random

BASE_URL = 'https://www.justice.gov/epstein/doj-disclosures'
DOWNLOAD_DIR = "/tmp"
STATE_FILE = "scraper_state.json"
BAD_PAGE_FILE = "bad_pages.json"
LOG_FILE = "playwright_scraper_log.txt"
cur_dataset_index = 0
cur_dataset_page = 0
cur_doc_index = 0

#should start on dataset 10, page 20, EFTA00000990.pdf
start_dataset_index = 10
start_dataset_page = 2208
start_doc_index = 0
started = False

# Set these to integers to force the scraper to start at a specific location, ignoring the saved state file. 
# Set them to None to rely on the saved state.
FORCE_DATASET_INDEX = None  # e.g., 10
FORCE_DATASET_PAGE = None   # e.g., 1257

# 1. Grab the bucket name injected by our CDK stack
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')

# 2. Environment-Aware Setup
if STAGING_BUCKET:
    print(f"Running in CLOUD MODE. Destination: S3 Bucket '{STAGING_BUCKET}'")
    s3 = boto3.client('s3')
    DOWNLOAD_DIR = "/tmp"  # Ephemeral storage for Fargate
else:
    print("Running in LOCAL MODE. Destination: Local Disk")
    s3 = None
    DOWNLOAD_DIR = "D:/development/datasets/Epstein_DOJ_Files"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- STATE MANAGEMENT ---
def load_state():
    global start_dataset_index, start_dataset_page, start_doc_index
    state = {"dataset_index": 0, "dataset_page": 0, "doc_index": 0}

    try:
        # Try to pull the latest state from S3 if in cloud mode
        if STAGING_BUCKET:
            try:
                s3.download_file(STAGING_BUCKET, STATE_FILE, os.path.join(DOWNLOAD_DIR, STATE_FILE))
                print(f"Downloaded state file from S3: {STATE_FILE}")
            except Exception as e:
                print(f"Failed to download state file from S3: {STATE_FILE}")
                print(f"Exception: {e}")
                pass # Normal if it's the very first run

        if os.path.exists(os.path.join(DOWNLOAD_DIR, STATE_FILE)):
            with open(os.path.join(DOWNLOAD_DIR, STATE_FILE), 'r') as f:
                state = json.load(f)
            print(f"Loaded saved state: {state}")
        else:
            print("No saved state found. Starting fresh.")

    except Exception as e:
        print(f"Error loading state: {e}. Starting fresh.")

    # Apply Overrides
    start_dataset_index = FORCE_DATASET_INDEX if FORCE_DATASET_INDEX is not None else state.get("dataset_index", 0)
    start_dataset_page = FORCE_DATASET_PAGE if FORCE_DATASET_PAGE is not None else state.get("dataset_page", 0)
    start_doc_index = state.get("doc_index", 0)

    if FORCE_DATASET_INDEX is not None or FORCE_DATASET_PAGE is not None:
        print(f"OVERRIDE ENGAGED: Forcing start at Dataset {start_dataset_index}, Page {start_dataset_page}")

def save_state(d_idx, p_idx, doc_idx):
    state = {
        "dataset_index": d_idx,
        "dataset_page": p_idx,
        "doc_index": doc_idx
    }
    try:
        with open(os.path.join(DOWNLOAD_DIR, STATE_FILE), 'w') as f:
            json.dump(state, f)

        if STAGING_BUCKET:
            s3.upload_file(os.path.join(DOWNLOAD_DIR, STATE_FILE), STAGING_BUCKET, STATE_FILE)
        print(f"--> State Saved: Dataset {d_idx}, Page {p_idx}")
    except Exception as e:
        print(f"Failed to save state: {e}")

def save_bad_page(d_idx, p_idx, doc_ids, dataset_url):
    bad_page_info = {
        "dataset_index": d_idx,
        "dataset_page": p_idx,
        "doc_ids": doc_ids,
        "dataset_url": dataset_url
    }
    try:
        bad_file_path = os.path.join(DOWNLOAD_DIR, BAD_PAGE_FILE)
        # open this as append so we keep a running log of all bad pages instead of overwriting
        with open(bad_file_path, 'a') as f:
            f.write(json.dumps(bad_page_info) + "\n")
        print(f"--> Logged bad page: Dataset {d_idx}, Page {p_idx}, URL: {dataset_url}")
    except Exception as e:
        print(f"Failed to log bad page: {e}")

def check_for_robot_check(page):
    try:
        robot_button = page.get_by_role('button', name='I am not a robot').or_(page.get_by_text('I am not a robot'))
        if robot_button.count() > 0 and robot_button.first.is_visible():
            print("Clicking the 'I am not a robot' button...")
            robot_button.first.click()
            page.wait_for_load_state('networkidle')  # Wait for the page to load after
        else:
            print("Could not find the 'I am not a robot' button. Please check the page structure.")
    except Exception as e:
        print(f"Error waiting for page to load: {e}")

def check_for_age_gate(page):
    try:
        age_block = page.locator('#age-verify-block')
        if age_block.count() > 0 and age_block.first.is_visible():
            print("Age gate detected. Attempting to bypass...")
            yes_button = age_block.first.get_by_role('button', name='Yes').or_(age_block.first.get_by_text('Yes'))
            if yes_button.count() > 0:
                # Click the "I am over 18" button within the age gate block
                yes_button.first.click()
            page.wait_for_load_state('networkidle')  # Wait for the page to load after bypassing age gate
        else:
            print("No age gate detected.")
    except Exception as e:
        print(f"Error checking for age gate: {e}")

def select_dropdown(page):
    drop_down = page.get_by_text('Epstein Files Transparency Act (H.R.4405)')
    if drop_down.count() > 0:
        print("Clicking the dataset dropdown...")
        drop_down.first.click()
        page.wait_for_load_state('networkidle')  # Wait for the page to load after clicking the dropdown
    else:
        print("Could not find the dataset dropdown. Please check the page structure.")

def navigate_to_datasets(page):
    check_for_robot_check(page)  # Check for robot check on the initial page load 
    check_for_age_gate(page)  # Check for age gate on the initial page load
    # Now you can add your logic to navigate through the pages and download PDFs
    # For example, you can look for PDF links and click them to trigger downloads
    select_dropdown(page)  # Click the dataset dropdown to select a specific dataset

def list_dataset_links(page):
    links = page.locator('a[href*="data-set-"][href*="-files"]').all()
    dataset_hrefs = [link.get_attribute('href') for link in links]
    return dataset_hrefs

def navigate_to_next_page(page):
    # check for next button and loop through pages if it exists
    next_is_present = False
    next_button = page.get_by_role('button', name='Next').or_(page.get_by_text('Next'))
    for button in next_button.all():
        print("checking next button")
        button_visible = button.is_visible()
        button_text = button.inner_text().strip().lower()
        print(f"Next button visible: {button_visible}")
        print(f"Next button text: {button_text}")
        if button_visible and button_text == 'next':
            print("assigning next button")
            next_button = button
            next_is_present = True
            break
        else:
            print("button was there, but not clicking")

    print(f"next_is_present: {next_is_present}") 
    if next_is_present:
        print("Clicking the 'Next' button to go to the next page of the dataset...")
        # 1. Smoothly scroll the button into the browser viewport
        next_button.first.scroll_into_view_if_needed()
        time.sleep(random.uniform(0.5, 1.0))

        # 2. Move the virtual mouse to hover over the button
        next_button.first.hover()
        time.sleep(random.uniform(0.2, 0.6)) # Brief pause while "looking" at it

        # 3. Click with a physical delay (milliseconds) between mouse-down and mouse-up
        # A human finger usually takes between 50ms and 150ms to tap a mouse button
        next_button.first.click(delay=random.randint(50, 150))

        # 4. Wait for the DOJ's JavaScript to fetch the new PDF links
        # Because the URL doesn't change, we wait for the network to stop making requests
        print("Click successful. Waiting for new PDFs to load...")
        page.wait_for_load_state("networkidle")

        # (Optional but recommended) Add a hard buffer just in case their server is slow
        # time.sleep(random.uniform(2.0, 4.0))

        return True
    else:
        print("No more pages found in this dataset.")
        return False

def download_pdf(page, pdf_url):
    global cur_doc_index
    cookies = page.context.cookies()
    session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
    user_agent = page.evaluate("navigator.userAgent")
    headers = {'User-Agent': user_agent}

    parsed_url = urlparse(pdf_url)
    filename = os.path.basename(parsed_url.path)
    if not filename:
        filename = f'unknown_document_{cur_doc_index}.pdf'

    file_path = os.path.join(DOWNLOAD_DIR, filename)
    try:
        # 1. Download the file locally
        with requests.get(pdf_url, headers=headers, cookies=session_cookies, stream=True, timeout=30) as response:
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded PDF to: {file_path}")

        # 2. Environment-Aware Storage Handling
        if STAGING_BUCKET:
            try:
                print(f"  -> Pushing {filename} to S3 Staging Bucket...")   
                s3.upload_file(file_path, STAGING_BUCKET, filename)
                print(f"  -> SUCCESS: {filename} secured in S3.")
            except Exception as e:
                print(f"  -> ERROR: Failed to upload {filename} to S3: {str(e)}")
            finally:
                # CRITICAL for Fargate: Clean up ephemeral disk space
                if os.path.exists(file_path):
                    os.remove(file_path)
        else:
            # Local mode: leave the file exactly where it is
            print(f"  -> LOCAL MODE: File retained on disk.")

    except requests.exceptions.RequestException as e:
        print(f"Failed to download PDF: {pdf_url} with error {e}")

def fast_forward_to_page(page, base_dataset_url, target_page_index):
    print(f"Fast-forwarding to page {target_page_index} using the hybrid click-then-jump method...")

    # If the target page is very close, it's faster/safer to just click there
    if target_page_index <= 3:
        for _ in range(target_page_index):
            navigate_to_next_page(page)
        return

    # Step 1: Establish human trust by clicking Next 3 times
    print("Performing 3 humanized clicks to build Akamai trust score...")
    for i in range(3):
        print(f" -> Trust-building click {i+1}/3...")
        navigate_to_next_page(page)
        # Add a slightly longer delay between these specific clicks to ensure telemetry is sent
        time.sleep(random.uniform(1.5, 2.5))

    # Step 2: Jump directly to the target URL
    # Handle whether the base URL already has query parameters
    target_url = base_dataset_url
    if "?" in target_url:
        target_url += f"&page={target_page_index}"
    else:
        target_url += f"?page={target_page_index}"

    print(f"Trust established. Jumping directly to: {target_url}")

    # Add a human-like pause before manually modifying the URL bar
    time.sleep(random.uniform(2.0, 3.5))

    page.goto(target_url)
    page.wait_for_load_state('networkidle')
    print("Successfully jumped to target page!")


def process_dataset_page(page, dataset_url, is_resume_dataset=False):
    global started, start_dataset_page, start_doc_index
    global cur_dataset_page, cur_doc_index

    print(f"Navigating to dataset page: {dataset_url}")
    cur_dataset_page = 0
    page.goto(dataset_url)
    page.wait_for_load_state('networkidle')

    # --- THE NEW FAST-FORWARD LOGIC ---
    if is_resume_dataset and start_dataset_page - 1 > 0:
        fast_forward_to_page(page, dataset_url, start_dataset_page - 1)
        # CRITICAL: Update our tracker so the script knows we jumped!
        cur_dataset_page = start_dataset_page 

    # Get the list of PDF links on the dataset page
    while True:
        check_for_robot_check(page)
        check_for_age_gate(page)
        pdf_links = page.locator('a[href$=".pdf"]').all()
        pdf_hrefs = [link.get_attribute('href') for link in pdf_links]
        
        pdf_hrefs_to_process = pdf_hrefs

        # If we are resuming, slice off the documents we already downloaded on this specific page
        if is_resume_dataset and not started and start_doc_index > 0:
            pdf_hrefs_to_process = pdf_hrefs[start_doc_index:]

        # The moment we process our first batch of links, we are officially fully caught up.
        started = True

        if len(pdf_hrefs) == 0:
            screenshot_path = os.path.join(DOWNLOAD_DIR, f"debug_state_{cur_dataset_index}_page_{cur_dataset_page}.png")
            page.screenshot(path=screenshot_path, full_page=True)
            print("No PDFs found on this page. Taking debug screenshot...")
            print("No PDFs found on this page. Saving debug info locally...")
            save_bad_page(cur_dataset_index, cur_dataset_page, [], dataset_url)
            if STAGING_BUCKET:
                s3.upload_file(screenshot_path, STAGING_BUCKET, f"debug_state_{cur_dataset_index}_page_{cur_dataset_page}.png")

        for i, pdf_href in enumerate(pdf_hrefs_to_process):
            full_pdf_url = urljoin(dataset_url, pdf_href)

            # Keep global track of where we are on the current page for logging/errors
            # If we sliced the array, we need to add the offset back to get the real index
            cur_doc_index = i if start_doc_index == 0 else (i + start_doc_index)

            print(f"Processing PDF {cur_doc_index}: {full_pdf_url}")
            download_pdf(page, full_pdf_url)

        # Reset doc index to 0 for the next page
        start_doc_index = 0 

        if not navigate_to_next_page(page):
            save_state(cur_dataset_index + 2, 0, 0)
            break
        else:
            cur_dataset_page += 1
            save_state(cur_dataset_index + 1, cur_dataset_page, 0)


def loop_through_datasets(page, dataset_links):
    global started, start_dataset_index, cur_dataset_index

    # Use enumerate to keep the real index even if we skip items
    for i, link in enumerate(dataset_links):
        cur_dataset_index = i

        if not started and i < start_dataset_index - 1:
            print(f"Skipping dataset {i}: {link}")
            continue

        print(f"Processing dataset {i}: {link}")

        # Pass a flag so the page processor knows if it needs to click the 'Next' button to catch up
        is_resume_target = (not started and i == start_dataset_index - 1)
        process_dataset_page(page, urljoin(BASE_URL, link), is_resume_dataset=is_resume_target)

def run():
    load_state()
    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=True)  # Set headless=True to run without opening a browser window
        context = browser.new_context( 
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},  # <-- Comma added here
            accept_downloads=True
        )
        page = context.new_page()
        page.goto(BASE_URL)
        page.wait_for_load_state('networkidle')  # Wait for the page to load completely
        page.wait_for_timeout(2000)  # Wait for 2 seconds to ensure the page is fully loaded

        navigate_to_datasets(page)
        dataset_links = list_dataset_links(page)
        print("Found PDF links:", dataset_links)
        loop_through_datasets(page, dataset_links)
        browser.close()

if __name__ == "__main__":
    if not STAGING_BUCKET:
        print("WARNING: STAGING_BUCKET environment variable missing. Defaulting to LOCAL storage.")
    run()