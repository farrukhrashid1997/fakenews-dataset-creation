import trafilatura
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from tqdm import tqdm
import sys
import time
import random
import requests

def process_url(record):
    """
    Processes a single URL record to extract required features.

    Parameters:
        record (dict): A dictionary containing 'URL' a`nd 'Date'.

    Returns:
        dict: A dictionary with extracted features.
    """
    url = record.get('URL')
    date = record.get('Date')
    result = {
        'claim': None, 
        'claim_factcheck_url': url,
        "claim_author": None,
        "claim_source": None,
        "claim_date": None,
        "factcheck_date": date,
        "justification": None,
        "fact_check_sources": None,
        "issue": None,
        'label': None,
        'tags': None,
        "text": None
    }
    try:
        # Pause between requests
        time.sleep(random.uniform(1, 5))
        
        # Fetch the page content
        page = trafilatura.fetch_url(url)
        if not page:
            result['claim'] = "Failed to fetch page"
            return result
        
        # Check for response status codes (e.g., 429 or 403)
        response = requests.head(url)
        if response.status_code in [429, 403]:
            print("Rate limit reached or forbidden access. Pausing for 2 minutes.")
            time.sleep(120)
            return result

        # Extract JSON data using trafilatura
        page_json = trafilatura.extract(page, output_format='json', with_metadata=True)
        if not page_json:
            result['label'] = "Failed to extract page JSON"
            return result
        json_data = json.loads(page_json)

        # Parse the page with BeautifulSoup
        soup = BeautifulSoup(page, 'html.parser')

        # Extract Claim Rating
        rating_tag = soup.find("div", class_='rating_title_wrap')
        if rating_tag and rating_tag.contents:
            claim_rating = rating_tag.contents[0].strip()
        else:
            claim_rating = "Rating not found"
        result['label'] = claim_rating

        # Extract Claim Text
        claim_cont_tag = soup.find("div", class_="claim_cont")
        if claim_cont_tag:
            claim_text = claim_cont_tag.get_text(strip=True)
        else:
            claim_text = "Claim text not found"
        result['claim'] = claim_text
    
        # Extract text
        text = json_data.get("text")
        result['text'] = text
        
        # Extract Tags
        tags = json_data.get("tags", "")
        if isinstance(tags, str):
            # Split the tags string by ";" to convert it into a list
            result['tags'] = [tag.strip() for tag in tags.split(";") if tag.strip()]
        else:
            result['tags'] = tags if tags else []

        # Extract Categories
        categories = json_data.get("categories", [])
        result['issue'] = categories if categories else "No categories found"

        # Extract Source Links
        
        sources = soup.find("div", id="sources_rows")
        extracted_urls = []
        url_pattern = re.compile(
                r'(https?://(?:www\.|(?!www))[^\s<>"\'()]+|www\.[^\s<>"\'()]+)'
            )
        if sources:
            sources = soup.find("div", id="sources_rows")
            p_tags = sources.find_all('p')
            for p in p_tags:
                text = p.get_text()
                urls = url_pattern.findall(text)
                cleaned_urls = [url.rstrip('.,;') for url in urls]
                extracted_urls.extend(cleaned_urls)

        if len(extracted_urls) == 0:
            csl_entries = sources.find("div", class_="csl-bib-body").find_all("div", class_="csl-entry")
            for entry in csl_entries:
                text = entry.get_text()
                urls = url_pattern.findall(text)
                cleaned_urls = [url.rstrip('.,;') for url in urls]
                extracted_urls.extend(cleaned_urls)
        # Store extracted URLs as a list
        result['fact_check_sources'] = extracted_urls if extracted_urls else []

    except Exception as e:
        # Capture any unexpected errors
        result['claim_rating'] = f"Error: {str(e)}"
    return result

def initialize_output_csv(output_csv, columns):
    """
    Initializes the output CSV file with headers.

    Parameters:
        output_csv (str): The path to the output CSV file.
        columns (list): A list of column names.
    """
    try:
        # Create an empty DataFrame with the specified columns and save it
        pd.DataFrame(columns=columns).to_csv(output_csv, index=False)
    except Exception as e:
        print(f"Failed to initialize output CSV '{output_csv}': {e}")
        sys.exit(1)

def save_sources_not_found(records, output_csv):
    """
    Saves URLs with no fact_check_sources to a new CSV file.

    Parameters:
        records (list): List of records where each record is a dict.
        output_csv (str): The path to the output CSV file for sources not found.
    """
    no_sources = [record['claim_factcheck_url'] for record in records if len(record.get('fact_check_sources', [])) == 0]
    if no_sources:
        pd.DataFrame(no_sources, columns=['URL']).to_csv(output_csv, index=False)

def main():
    input_csv = "snopes_urls.csv"
    output_csv = "snopes_results.csv"
    no_sources_csv = "no_fact_check_sources.csv"

    # Read the input CSV
    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        print(f"Failed to read input CSV '{input_csv}': {e}")
        sys.exit(1)

    # Verify required columns
    if not {'URL', 'Date'}.issubset(df.columns):
        print("Input CSV must contain 'URL' and 'Date' columns.")
        sys.exit(1)

    # Define output columns
    columns = ['claim', 
               'claim_factcheck_url', 
               "claim_author", 
               "claim_source", 
               "claim_date", 
               "factcheck_date", 
               "justification", 
               "fact_check_sources", 
               "issue", 
               'label', 
               'tags', 
               "text"]
    initialize_output_csv(output_csv, columns)

    # Prepare data for processing
    records = df.to_dict('records')
    results = []
    batch_size = 10

    # Set up multiprocessing pool with a maximum of 4 processes
    pool_size = min(2, multiprocessing.cpu_count())  # Maximum of 4 processes
    pool = Pool(processes=pool_size)

    try:
        # Process URLs with a progress bar
        for result in tqdm(pool.imap_unordered(process_url, records), total=len(records), desc="Processing URLs"):
            results.append(result)
            if len(results) >= batch_size:
                # Append results to the CSV in batches
                pd.DataFrame(results).to_csv(output_csv, mode='a', header=False, index=False)
                results = []
        # Write any remaining results
        if results:
            pd.DataFrame(results, columns=columns).to_csv(output_csv, mode='a', header=False, index=False, quoting=1)
        
     
        # Save URLs with no fact_check_sources to a separate CSV
        save_sources_not_found(results, no_sources_csv)

    except KeyboardInterrupt:
        print("\nProcessing interrupted by user.")
    except Exception as e:
        print(f"An error occurred during processing: {e}")
    finally:
        pool.close()
        pool.join()

if __name__ == "__main__":
    main()
