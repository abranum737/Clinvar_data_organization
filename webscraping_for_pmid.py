import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import logging


logging.basicConfig(filename='snp_fetch.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


def fetch_snp_publications(snp_id, session, cache):
    # Check cache first
    if snp_id in cache:
        return cache[snp_id]

    url = f"https://www.ncbi.nlm.nih.gov/snp/{snp_id}#publications"
    try:
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            return parse_publications(response.text, snp_id, cache)
        else:
            logging.warning(f"Failed to fetch data for {snp_id}, Status Code: {response.status_code}")
            cache[snp_id] = [(snp_id, 'Failed to fetch page', None, None, None, None, None)]
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {snp_id}, Error: {e}")
        cache[snp_id] = [(snp_id, 'Request Exception', None, None, None, None, None)]
    return cache[snp_id]

def parse_publications(html, snp_id, cache):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', id='publication_datatable')
    publications = []
    if table:
        rows = table.find_all('tr')[1:]  # Skip the header row
        for row in rows:
            cols = row.find_all('td')
            if cols:
                pmid_link = cols[0].find('a')
                publications.append([
                    snp_id,
                    pmid_link.text.strip() if pmid_link else 'No PMID',
                    cols[1].text.strip(),
                    cols[2].text.strip(),
                    cols[3].text.strip(),
                    cols[4].text.strip(),
                    f"https://www.ncbi.nlm.nih.gov{pmid_link['href']}" if pmid_link else None
                ])
    if not publications:
        publications = [(snp_id, 'No publications found', None, None, None, None, None)]
    cache[snp_id] = publications
    return publications

def save_cache(cache, filepath):
    """ Save the cache to a CSV file """
    cache_list = [item for sublist in cache.values() for item in sublist]
    cache_df = pd.DataFrame(cache_list, columns=['dbsnp_id', 'PMID', 'Title', 'Author', 'Year', 'Journal', 'Link'])
    cache_df.to_csv(filepath, index=False)


def load_cache(filepath):
    """ Load cache from a CSV if it exists, otherwise return an empty dictionary """
    try:
        cache_df = pd.read_csv(filepath, low_memory=False)
        cache = {}
        for _, row in cache_df.iterrows():
            snp_id = row['dbsnp_id']
            if snp_id not in cache:
                cache[snp_id] = []
            cache[snp_id].append(row.tolist())
        return cache
    except FileNotFoundError:
        return {}

def process_snps(snps, session, cache):
    for snp in snps:
        publications = fetch_snp_publications(snp, session, cache)
        logging.info(f"SNP {snp} searched.")
    return publications

# Load existing data or initialize cache
cache = load_cache('publication_cache.csv')

# Load SNP IDs from CSV
df = pd.read_csv('/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/reorganized_genes.csv', low_memory=False)
df = df.drop_duplicates(subset='dbsnp_id')

# Filter SNPs not in cache
new_snps = df[~df['dbsnp_id'].isin(cache.keys())]['dbsnp_id'].tolist()

# Setup ThreadPool and Session
with requests.Session() as session:
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda snp: process_snps([snp], session, cache), new_snps)

# Save updated cache
save_cache(cache, 'publication_cache.csv')

# Prepare final DataFrame
publications_df = pd.DataFrame([item for sublist in cache.values() for item in sublist],
                               columns=['dbsnp_id', 'PMID', 'Title', 'Author', 'Year', 'Journal', 'Link'])
final_df = pd.merge(df, publications_df, on='dbsnp_id', how='left')


# Save the updated DataFrame back to a new CSV in Processed csvs folder
final_df.to_csv(
    '/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/final_reorganized_genes.csv',
    index=False)
