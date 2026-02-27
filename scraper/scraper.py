import os
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth  # <-- Import the new Stealth class
from urllib.parse import urljoin, urlparse
import requests
#import the time module to add delays between requests
import time
import os
import boto3
import re
import random

BASE_URL = 'https://www.justice.gov/epstein/doj-disclosures'
DOWNLOAD_DIR = "/tmp"
LOG_FILE = "playwright_scraper_log.txt"
cur_dataset_index = 0
cur_dataset_page = 0
cur_doc_index = 0

#should start on dataset 1, page 20, EFTA00000990.pdf
start_dataset_index = 10
start_dataset_page = 1257
start_doc_index = 0
started = False

# 1. Initialize AWS S3 Client
# (Fargate automatically injects the IAM credentials, so no keys needed here)
s3 = boto3.client('s3')

# 2. Grab the bucket name injected by our CDK stack
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')

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
    cookies = page.context.cookies()
    session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
    user_agent = page.evaluate("navigator.userAgent")
    headers = {'User-Agent': user_agent}

    try:
        with requests.get(pdf_url, headers=headers, cookies=session_cookies, stream=True, timeout=30) as response:
            response.raise_for_status()
            parsed_url = urlparse(pdf_url)
            filename = os.path.basename(parsed_url.path)
            if not filename:
                filename = 'unknown_document.pdf'
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded locally PDF: {file_path}")

            # move to S3 bucket
            try:
                print(f"  -> Pushing {filename} to S3 Staging Bucket...")   
                s3.upload_file(file_path, STAGING_BUCKET, filename)
                print(f"  -> SUCCESS: {filename} secured in S3.")
            except Exception as e:
                print(f"  -> ERROR: Failed to upload {filename} to S3: {str(e)}")
            finally:
                # CRITICAL: Always clean up the container's disk space
                if os.path.exists(file_path):
                    os.remove(file_path)
    except requests.exceptions.RequestException as e:
        print(f"Failed to download PDF: {pdf_url} with error {e}")
        

def select_page(page, page_index):
    check_for_robot_check(page)  # Check for robot check on the initial page load 
    check_for_age_gate(page)  # Check for age gate on the initial page load
    for _ in range(page_index):  # Try up to 10 times to find the page button
        navigate_to_next_page(page)

def process_dataset_page(page, dataset_url):
    global started
    global start_dataset_page
    global start_doc_index
    print(f"Navigating to dataset page: {dataset_url}")
    cur_dataset_page = 0
    page.goto(dataset_url)
    page.wait_for_load_state('networkidle')  # Wait for the page to load

    if start_dataset_page > 0 and not started:
        print(f"Starting from page {start_dataset_page} of the dataset...")
        select_page(page, start_dataset_page)

    #get the list of PDF links on the dataset page
    while True:
        check_for_robot_check(page)  # Check for robot check on the initial page load 
        check_for_age_gate(page)  # Check for age gate on the initial page load
        pdf_links = page.locator('a[href$=".pdf"]').all()
        pdf_hrefs = [link.get_attribute('href') for link in pdf_links]
        # TODO if not started, check the index of the current document and only process the ones that are after the current document index
        pdf_hrefs_to_process = pdf_hrefs
        if start_doc_index > 0 and not started:
            pdf_hrefs_to_process = pdf_hrefs[start_doc_index:]
        started = True
        print(f"pdf_hrefs is {pdf_hrefs_to_process}")
        print(f"pdf_links is {pdf_links}")
        if len(pdf_hrefs) == 0:
            page.screenshot(path="/tmp/debug_state.png")
            s3.upload_file("/tmp/debug_state.png", STAGING_BUCKET, "debug_state.png")
        for pdf_href in pdf_hrefs_to_process:
            full_pdf_url = urljoin(dataset_url, pdf_href)
            try:
                cur_doc_index = pdf_hrefs.index(pdf_href)
                download_pdf(page, full_pdf_url)
            except Exception as e:
                print(f"Error downloading PDF: {full_pdf_url}, Error: {e}")
            print(f"Found PDF link: {full_pdf_url}")
        if not navigate_to_next_page(page):
            break
        else:
            cur_dataset_page += 1
    return

def loop_through_datasets(page, dataset_links):
    global started
    global start_dataset_index
    # TODO if not started, check the index of the current dataset and only process the ones that are after the current dataset index
    if start_dataset_index > 0 and not started:
        dataset_links = dataset_links[start_dataset_index:]
    for link in dataset_links:
        print(f"Processing dataset link: {link}")
        cur_dataset_index = dataset_links.index(link)
        process_dataset_page(page, urljoin(BASE_URL, link))
        # Here you can add logic to download PDFs or extract information from the dataset page

def run():
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
        print("CRITICAL ERROR: STAGING_BUCKET environment variable missing.")
        exit(1)
    run()