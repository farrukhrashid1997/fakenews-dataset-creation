import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import os
from multiprocessing import Pool, cpu_count

date_limit = datetime(2022, 1, 1).date()
output_file = 'snopes_urls.csv'

if not os.path.isfile(output_file):
    pd.DataFrame(columns=['URL', 'Date']).to_csv(output_file, index=False)

def parse_date(date_str):
    if "(Updated:" in date_str:
        date_str = date_str.split(" (Updated:")[0].strip()
    date_str = date_str.replace("Sept.", "Sep.")
    try:
        parsed_date = datetime.strptime(date_str, '%b. %d, %Y')
        return parsed_date.date()
    except ValueError:
        pass
    try:
        parsed_date = datetime.strptime(date_str, '%B %d, %Y')
        return parsed_date.date()
    except ValueError:
        pass

    return None



def fetch_page_links(page_num):
    url = f'https://www.snopes.com/fact-check/?pagenum={page_num}'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    page_data = []
    articles = soup.find_all('div', class_='article_wrapper')

    for article in articles:
        link_tag = article.find('a', class_='outer_article_link_wrapper', href=True)
        if not link_tag:
            continue
        link = link_tag['href']
        
        article_text = link_tag.find('div', class_='article_text')
        date_tag = article_text.find('span', class_='article_date') if article_text else None
        if date_tag:
            claim_date = parse_date(date_tag.get_text(strip=True))
            if claim_date is None:
                print(date_tag.get_text(strip=True))
            page_data.append({
                'URL': link,
                'Date': claim_date
            })
    
    return page_data

def scrape_snopes_links():
    page_num = 1
    max_pages = 350
    cpu_count_used = cpu_count() - 1
    pool = Pool(processes=cpu_count_used)

    while page_num <= max_pages:
        print(f"Scraping pages starting from page {page_num}...")
        pages_to_scrape = min(cpu_count_used, max_pages - page_num + 1)
        page_results = pool.map(fetch_page_links, [page_num + i for i in range(pages_to_scrape)])
        page_results = [item for sublist in page_results if sublist for item in sublist]
        if not page_results:
            break
            
        df = pd.DataFrame(page_results)
        # Filter out dates before 2022
        df = df[df['Date'] >= date_limit]
        if not df.empty:
            df.to_csv(output_file, mode='a', header=False, index=False)

        page_num += pages_to_scrape

    pool.close()
    pool.join()

# Run the scraper
if __name__ == '__main__':
    scrape_snopes_links()