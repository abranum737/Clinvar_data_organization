#
# import requests
# import pandas as pd
# import aiohttp
# import asyncio
# from aiohttp import ClientSession, TCPConnector
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# import logging
#
# logging.basicConfig(level=logging.INFO)
#
#
# def create_session_with_retries():
#     session = requests.Session()
#     retry = Retry(
#         total=5,  # Total number of retries
#         backoff_factor=0.3,  # Backoff factor for sleep between retries
#         status_forcelist=[500, 502, 503, 504],  # HTTP status codes to retry
#         allowed_methods=["HEAD", "GET", "OPTIONS"]  # Methods to retry
#     )
#     adapter = HTTPAdapter(max_retries=retry)
#     session.mount("https://", adapter)
#     session.mount("http://", adapter)
#     return session
#
#
# # Create a session with retries
# session_with_retries = create_session_with_retries()
#
#
# def find_by_gene(gene_name, page=0, size=10, sort=None, projection=None):
#     base_url = "https://www.ebi.ac.uk/gwas/rest/api/singleNucleotidePolymorphisms/search/findByGene"
#
#     params = {
#         "geneName": gene_name,
#         "page": page,
#         "size": size
#     }
#
#     if sort:
#         params["sort"] = sort
#     if projection:
#         params["projection"] = projection
#
#     response = session_with_retries.get(base_url, params=params)
#
#     if response.status_code == 200:
#         return response.json()
#     else:
#         response.raise_for_status()
#
#
# def fetch_associations(snp_data):
#     associations = []
#
#     for snp in snp_data['_embedded']['singleNucleotidePolymorphisms']:
#         if 'associations' in snp['_links']:
#             associations_url = snp['_links']['associations']['href']
#             response = session_with_retries.get(associations_url)
#             if response.status_code == 200:
#                 association_data = response.json()
#                 if '_embedded' in association_data and 'associations' in association_data['_embedded']:
#                     associations.extend(association_data['_embedded']['associations'])
#
#     return associations
#
#
# def convert_to_dataframe(associations):
#     df = pd.DataFrame(associations)
#     return df
#
#
# def extract_nested_info(df, column_name):
#     if column_name not in df.columns:
#         logging.warning(f"Column '{column_name}' does not exist in the DataFrame")
#         return df
#
#     extracted_data = []
#     for idx, row in df.iterrows():
#         nested_info = row[column_name]
#         if isinstance(nested_info, dict):
#             extracted_data.append(nested_info)
#         else:
#             extracted_data.append({})
#
#     nested_df = pd.DataFrame(extracted_data).add_prefix(f"{column_name}.")
#     df = df.drop(columns=[column_name])
#     return pd.concat([df, nested_df], axis=1)
#
#
# async def fetch_json(session, url):
#     async with session.get(url) as response:
#         return await response.json()
#
# #
# # async def fetch_snps_info(session, url):
# #     data = await fetch_json(session, url)
# #     rsids = []
# #     if '_embedded' in data and 'singleNucleotidePolymorphisms' in data['_embedded']:
# #         for snp in data['_embedded']['singleNucleotidePolymorphisms']:
# #             rsids.append(snp.get('rsId', ''))
# #     return rsids
# #
# # async def fetch_study_info(session, url):
# #     data = await fetch_json(session, url)
# #     study_info = {
# #         "publicationDate": data.get("publicationInfo", {}).get("publicationDate", ""),
# #         "pubmedId": data.get("publicationInfo", {}).get("pubmedId", ""),
# #         "publication": data.get("publicationInfo", {}).get("publication", ""),
# #         "title": data.get("publicationInfo", {}).get("title", ""),
# #         "author": data.get("publicationInfo", {}).get("author", {}).get("fullname", ""),
# #         "trait": data.get("diseaseTrait", {}).get("trait", ""),
# #         "platforms": ", ".join([platform.get("manufacturer", "") for platform in data.get("platforms", [])]),
# #         "cohort": data.get("cohort", ""),
# #         "studyDesignComment": data.get("studyDesignComment", ""),
# #         "initialSampleSize": data.get("initialSampleSize", "")
# #     }
# #     return study_info
#
# # rate limiting for snps info and study info, increases overall time
# async def fetch_snps_info(session, url, retries=5, delay=1):
#     for attempt in range(retries):
#         try:
#             data = await fetch_json(session, url)
#             rsids = []
#             if '_embedded' in data and 'singleNucleotidePolymorphisms' in data['_embedded']:
#                 for snp in data['_embedded']['singleNucleotidePolymorphisms']:
#                     rsids.append(snp.get('rsId', ''))
#             return rsids
#         except aiohttp.ClientError as e:
#             logging.warning(f"Error fetching SNPs info: {e}, retrying in {delay} seconds...")
#             await asyncio.sleep(delay)
#             delay *= 2
#     logging.error(f"Failed to fetch SNPs info after {retries} attempts")
#     return []
#
# async def fetch_study_info(session, url, retries=5, delay=1):
#     for attempt in range(retries):
#         try:
#             data = await fetch_json(session, url)
#             study_info = {
#                 "publicationDate": data.get("publicationInfo", {}).get("publicationDate", ""),
#                 "pubmedId": data.get("publicationInfo", {}).get("pubmedId", ""),
#                 "publication": data.get("publicationInfo", {}).get("publication", ""),
#                 "title": data.get("publicationInfo", {}).get("title", ""),
#                 "author": data.get("publicationInfo", {}).get("author", {}).get("fullname", ""),
#                 "trait": data.get("diseaseTrait", {}).get("trait", ""),
#                 "platforms": ", ".join([platform.get("manufacturer", "") for platform in data.get("platforms", [])]),
#                 "cohort": data.get("cohort", ""),
#                 "studyDesignComment": data.get("studyDesignComment", ""),
#                 "initialSampleSize": data.get("initialSampleSize", "")
#             }
#             return study_info
#         except aiohttp.ClientError as e:
#             logging.warning(f"Error fetching study info: {e}, retrying in {delay} seconds...")
#             await asyncio.sleep(delay)
#             delay *= 2
#     logging.error(f"Failed to fetch study info after {retries} attempts")
#     return {}
#
#
# async def fetch_data(extracted_df, max_concurrent_requests=100):
#     snps_data = []
#     study_data = []
#
#     connector = TCPConnector(limit_per_host=max_concurrent_requests)
#     async with ClientSession(connector=connector) as session:
#         tasks = []
#         for index, row in extracted_df.iterrows():
#             snp_url = row['_links.snps.href']
#             study_url = row['_links.study.href']
#             tasks.append(fetch_snps_info(session, snp_url))
#             tasks.append(fetch_study_info(session, study_url))
#
#         results = await asyncio.gather(*tasks)
#
#     # Separate SNP and study data from results
#     for i in range(0, len(results), 2):
#         snps_data.append(results[i])
#         study_data.append(results[i + 1])
#
#     return snps_data, study_data
#
#
# def process_gene(gene_name):
#     try:
#         gene_data = find_by_gene(gene_name)
#         associations = fetch_associations(gene_data)
#         associations_df = convert_to_dataframe(associations)
#
#         if associations_df.empty:
#             logging.warning(f"No associations found for gene: {gene_name}")
#             return pd.DataFrame()  # Return empty DataFrame if no associations found
#
#         extracted_df = extract_nested_info(associations_df, '_links')
#         extracted_df = extract_nested_info(extracted_df, '_links.snps')
#         extracted_df = extract_nested_info(extracted_df, '_links.study')
#
#         snps_data, study_data = asyncio.run(fetch_data(extracted_df, max_concurrent_requests=100))
#
#         snps_df = pd.DataFrame(snps_data)
#         study_df = pd.DataFrame(study_data)
#
#         extracted_df = pd.concat([extracted_df, snps_df.add_prefix('snp_'), study_df.add_prefix('study_')], axis=1)
#
#         extracted_relevant_columns = ['snp_0', 'pvalue', 'riskFrequency', 'standardError', 'range', 'betaNum',
#                                       'betaUnit', 'betaDirection', 'study_publicationDate', 'study_pubmedId',
#                                       'study_publication', 'study_title', 'study_author', 'study_trait',
#                                       'study_platforms', 'study_cohort', 'study_studyDesignComment',
#                                       'study_initialSampleSize']
#
#         # Filter only existing relevant columns
#         extracted_relevant_columns = [col for col in extracted_relevant_columns if col in extracted_df.columns]
#
#
#         # Add the PubMed link column
#         extracted_df['pubmed_link'] = extracted_df['study_pubmedId'].apply(lambda x: f"https://pubmed.ncbi.nlm.nih.gov/{x}/" if pd.notna(x) else "")
#
#         # Add the gene name as the first column
#         extracted_df.insert(0, 'gene_name', gene_name)
#
#         return extracted_df[['gene_name'] + extracted_relevant_columns + ['pubmed_link']]
#     except Exception as e:
#         logging.error(f"{gene_name} generated an exception: {e}")
#         return pd.DataFrame()  # Return empty DataFrame in case of exception
#
#
# def main():
#     # Load the CSV file with gene names
#     gene_names_file = 'unique_gene_names.csv'
#     gene_df = pd.read_csv(gene_names_file)
#     gene_names = gene_df['gene(s)'].tolist()
#
#     # Initialize an empty DataFrame to store all results
#     combined_df = pd.DataFrame()
#
#     # Process each gene name concurrently using ThreadPoolExecutor
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         future_to_gene = {executor.submit(process_gene, gene_name): gene_name for gene_name in gene_names}
#         for future in as_completed(future_to_gene):
#             gene_name = future_to_gene[future]
#             try:
#                 gene_df = future.result()
#                 if not gene_df.empty:
#                     combined_df = pd.concat([combined_df, gene_df], ignore_index=True)
#                 logging.info(f"Processed {gene_name}")
#             except Exception as exc:
#                 logging.error(f"{gene_name} generated an exception: {exc}")
#
#     # Remove duplicates from the combined DataFrame
#     combined_df = combined_df.drop_duplicates()
#
#
#     # Save the combined DataFrame to a single CSV file
#     output_file = 'combined_gene_associations.csv'
#     combined_df.to_csv(output_file, index=False)
#     logging.info(f"All gene associations saved to {output_file}")
#
#
# if __name__ == "__main__":
#     main()

import requests
import pandas as pd
import aiohttp
import asyncio
from aiohttp import ClientSession, TCPConnector
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logging.basicConfig(level=logging.INFO)


def create_session_with_retries():
    session = requests.Session()
    retry = Retry(
        total=5,  # Total number of retries
        backoff_factor=0.3,  # Backoff factor for sleep between retries
        status_forcelist=[500, 502, 503, 504],  # HTTP status codes to retry
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]  # Methods to retry
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# Create a session with retries
session_with_retries = create_session_with_retries()


def find_by_gene(gene_name, page=0, size=10, sort=None, projection=None):
    base_url = "https://www.ebi.ac.uk/gwas/rest/api/singleNucleotidePolymorphisms/search/findByGene"

    params = {
        "geneName": gene_name,
        "page": page,
        "size": size
    }

    if sort:
        params["sort"] = sort
    if projection:
        params["projection"] = projection

    response = session_with_retries.get(base_url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


def fetch_associations(snp_data):
    associations = []

    for snp in snp_data['_embedded']['singleNucleotidePolymorphisms']:
        if 'associations' in snp['_links']:
            associations_url = snp['_links']['associations']['href']
            response = session_with_retries.get(associations_url)
            if response.status_code == 200:
                association_data = response.json()
                if '_embedded' in association_data and 'associations' in association_data['_embedded']:
                    associations.extend(association_data['_embedded']['associations'])

    return associations


def convert_to_dataframe(associations):
    df = pd.DataFrame(associations)
    return df


def extract_nested_info(df, column_name):
    if column_name not in df.columns:
        logging.warning(f"Column '{column_name}' does not exist in the DataFrame")
        return df

    extracted_data = []
    for idx, row in df.iterrows():
        nested_info = row[column_name]
        if isinstance(nested_info, dict):
            extracted_data.append(nested_info)
        else:
            extracted_data.append({})

    nested_df = pd.DataFrame(extracted_data).add_prefix(f"{column_name}.")
    df = df.drop(columns=[column_name])
    return pd.concat([df, nested_df], axis=1)


async def fetch_json(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.json()


async def fetch_snps_info(session, url, retries=5, delay=1):
    for attempt in range(retries):
        try:
            data = await fetch_json(session, url)
            rsids = []
            if '_embedded' in data and 'singleNucleotidePolymorphisms' in data['_embedded']:
                for snp in data['_embedded']['singleNucleotidePolymorphisms']:
                    rsids.append(snp.get('rsId', ''))
            return rsids
        except aiohttp.ClientError as e:
            logging.warning(f"Error fetching SNPs info: {e}, retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay *= 2
    logging.error(f"Failed to fetch SNPs info after {retries} attempts")
    return []


async def fetch_study_info(session, url, retries=5, delay=1):
    for attempt in range(retries):
        try:
            data = await fetch_json(session, url)
            study_info = {
                "publicationDate": data.get("publicationInfo", {}).get("publicationDate", ""),
                "pubmedId": data.get("publicationInfo", {}).get("pubmedId", ""),
                "publication": data.get("publicationInfo", {}).get("publication", ""),
                "title": data.get("publicationInfo", {}).get("title", ""),
                "author": data.get("publicationInfo", {}).get("author", {}).get("fullname", ""),
                "trait": data.get("diseaseTrait", {}).get("trait", ""),
                "platforms": ", ".join([platform.get("manufacturer", "") for platform in data.get("platforms", [])]),
                "cohort": data.get("cohort", ""),
                "studyDesignComment": data.get("studyDesignComment", ""),
                "initialSampleSize": data.get("initialSampleSize", "")
            }
            return study_info
        except aiohttp.ClientError as e:
            logging.warning(f"Error fetching study info: {e}, retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay *= 2
    logging.error(f"Failed to fetch study info after {retries} attempts")
    return {}


async def fetch_data(extracted_df, max_concurrent_requests=100):
    snps_data = []
    study_data = []

    connector = TCPConnector(limit_per_host=max_concurrent_requests)
    async with ClientSession(connector=connector) as session:
        tasks = []
        for index, row in extracted_df.iterrows():
            snp_url = row['_links.snps.href']
            study_url = row['_links.study.href']
            tasks.append(fetch_snps_info(session, snp_url))
            tasks.append(fetch_study_info(session, study_url))

        results = await asyncio.gather(*tasks)

    # Separate SNP and study data from results
    for i in range(0, len(results), 2):
        snps_data.append(results[i])
        study_data.append(results[i + 1])

    return snps_data, study_data


def process_gene(gene_name):
    try:
        gene_data = find_by_gene(gene_name)
        associations = fetch_associations(gene_data)
        associations_df = convert_to_dataframe(associations)

        if associations_df.empty:
            logging.warning(f"No associations found for gene: {gene_name}")
            return pd.DataFrame()  # Return empty DataFrame if no associations found

        extracted_df = extract_nested_info(associations_df, '_links')
        extracted_df = extract_nested_info(extracted_df, '_links.snps')
        extracted_df = extract_nested_info(extracted_df, '_links.study')

        snps_data, study_data = asyncio.run(fetch_data(extracted_df, max_concurrent_requests=100))

        snps_df = pd.DataFrame(snps_data)
        study_df = pd.DataFrame(study_data)

        extracted_df = pd.concat([extracted_df, snps_df.add_prefix('snp_'), study_df.add_prefix('study_')], axis=1)

        extracted_relevant_columns = ['snp_0', 'pvalue', 'riskFrequency', 'standardError', 'range', 'betaNum',
                                      'betaUnit', 'betaDirection', 'study_publicationDate', 'study_pubmedId',
                                      'study_publication', 'study_title', 'study_author', 'study_trait',
                                      'study_platforms', 'study_cohort', 'study_studyDesignComment',
                                      'study_initialSampleSize']

        # Filter only existing relevant columns
        extracted_relevant_columns = [col for col in extracted_relevant_columns if col in extracted_df.columns]


        # Add the PubMed link column
        extracted_df['pubmed_link'] = extracted_df['study_pubmedId'].apply(lambda x: f"https://pubmed.ncbi.nlm.nih.gov/{x}/" if pd.notna(x) else "")

        # Add the gene name as the first column
        extracted_df.insert(0, 'gene_name', gene_name)

        return extracted_df[['gene_name'] + extracted_relevant_columns + ['pubmed_link']]
    except Exception as e:
        logging.error(f"{gene_name} generated an exception: {e}")
        return pd.DataFrame()  # Return empty DataFrame in case of exception


def main():
    # Load the CSV file with gene names
    gene_names_file = 'unique_gene_names.csv'
    gene_df = pd.read_csv(gene_names_file)
    gene_names = gene_df['gene(s)'].tolist()

    # Initialize an empty DataFrame to store all results
    combined_df = pd.DataFrame()

    # Process each gene name concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_gene = {executor.submit(process_gene, gene_name): gene_name for gene_name in gene_names}
        for future in as_completed(future_to_gene):
            gene_name = future_to_gene[future]
            try:
                gene_df = future.result()
                if not gene_df.empty:
                    combined_df = pd.concat([combined_df, gene_df], ignore_index=True)
                logging.info(f"Processed {gene_name}")
            except Exception as exc:
                logging.error(f"{gene_name} generated an exception: {exc}")

    # Remove duplicates from the combined DataFrame
    combined_df = combined_df.drop_duplicates()


    # Save the combined DataFrame to a single CSV file
    output_file = 'combined_gene_associations.csv'
    combined_df.to_csv(output_file, index=False)
    logging.info(f"All gene associations saved to {output_file}")


if __name__ == "__main__":
    main()
