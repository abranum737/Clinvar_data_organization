#many entries have 'unavailable due to server error', but contain PMIDs, here is where we collect data to fill it in
# import requests
# import xml.etree.ElementTree as ET
# import pandas as pd
# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry
#
# #
# # def fetch_pubmed_details(pmid):
# #     url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
# #     response = requests.get(url)
# #     if response.status_code == 200:
# #         print(response.text)  # Debugging line to see the XML
# #         return parse_pubmed_response(response.text)
# #     else:
# #         print(f"Failed to fetch data for PMID: {pmid}")
# #         return None
# #
# # def parse_pubmed_response(xml_data):
# #     root = ET.fromstring(xml_data)
# #     article_info = {
# #         "title": None,
# #         "authors": [],
# #         "year": None,
# #         "journal": None
# #     }
# #
# #     article = root.find('.//Article')
# #     if article:
# #         article_info['title'] = article.findtext('.//ArticleTitle')
# #         journal = article.find('.//Journal')
# #         if journal:
# #             article_info['journal'] = journal.findtext('.//Title')
# #             article_info['year'] = journal.find('.//PubDate/Year').text if journal.find('.//PubDate/Year') is not None else "Year not found"
# #
# #         authors = root.findall('.//AuthorList/Author')
# #         for author in authors:
# #             lastname = author.findtext('LastName')
# #             forename = author.findtext('ForeName')
# #             if lastname and forename:
# #                 article_info['authors'].append(f"{forename} {lastname}")
# #
# #     return article_info
# #
# # def format_authors(authors_list):
# #     if not authors_list:
# #         return "Unknown"
# #     if len(authors_list) > 1:
# #         return f"{authors_list[0]} et al."
# #     return authors_list[0]
# #
# # # Load the dataset
# # df = pd.read_csv('/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/final_reorganized_genes.csv', low_memory=False)
# # missing_titles = df[(df['Title_y'] == 'Unavailable due to server error') & (df['PMID_y'].notna())]
# #
# # for index, row in missing_titles.iterrows():
# #     details = fetch_pubmed_details(row['PMID_y'])
# #     if details:
# #         df.at[index, 'Title'] = details['title']
# #         if 'authors' in details:
# #             authors_formatted = format_authors(details['authors'])
# #             df.at[index, 'Authors'] = authors_formatted
# #         df.at[index, 'Year'] = details['year']
# #         df.at[index, 'Journal'] = details['journal']
# #
# #
# # df.to_csv('/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/final_reorganized_genes_updated.csv', index=False)
#
#
# def fetch_pubmed_details(pmid, retries=3):
#     url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
#     headers = {'User-Agent': 'Mozilla/5.0'}
#
#     session = requests.Session()
#     retry = Retry(
#         total=retries,
#         backoff_factor=1,
#         status_forcelist=[429, 500, 502, 503, 504],
#         allowed_methods=["HEAD", "GET", "OPTIONS"]
#     )
#     adapter = HTTPAdapter(max_retries=retry)
#     session.mount("https://", adapter)
#     session.mount("http://", adapter)
#
#     try:
#         response = session.get(url, headers=headers)
#         response.raise_for_status()
#         print(response.text)  # Debugging line to see the XML
#         return parse_pubmed_response(response.text)
#     except requests.exceptions.SSLError as ssl_err:
#         print(f"SSL error occurred: {ssl_err}")
#         return None
#     except requests.exceptions.RequestException as req_err:
#         print(f"Request error occurred: {req_err}")
#         return None
#
#
# def parse_pubmed_response(xml_data):
#     root = ET.fromstring(xml_data)
#     article_info = {
#         "title": None,
#         "authors": [],
#         "year": None,
#         "journal": None
#     }
#
#     article = root.find('.//Article')
#     if article:
#         article_info['title'] = article.findtext('.//ArticleTitle')
#         journal = article.find('.//Journal')
#         if journal:
#             article_info['journal'] = journal.findtext('.//Title')
#             article_info['year'] = journal.find('.//PubDate/Year').text if journal.find(
#                 './/PubDate/Year') is not None else "Year not found"
#
#         authors = root.findall('.//AuthorList/Author')
#         for author in authors:
#             lastname = author.findtext('LastName')
#             forename = author.findtext('ForeName')
#             if lastname and forename:
#                 article_info['authors'].append(f"{forename} {lastname}")
#
#     return article_info
#
#
# def format_authors(authors_list):
#     if not authors_list:
#         return "Unknown"
#     if len(authors_list) > 1:
#         return f"{authors_list[0]} et al."
#     return authors_list[0]
#
#
# # Load the dataset
# df = pd.read_csv(
#     '/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/final_reorganized_genes.csv',
#     low_memory=False)
# missing_titles = df[(df['Title_y'] == 'Unavailable due to server error') & (df['PMID_y'].notna())]
#
# for index, row in missing_titles.iterrows():
#     details = fetch_pubmed_details(row['PMID_y'])
#     if details:
#         df.at[index, 'Title_y'] = details['title']
#         if 'authors' in details:
#             authors_formatted = format_authors(details['authors'])
#             df.at[index, 'Author_y'] = authors_formatted
#         df.at[index, 'Year_y'] = details['year']
#         df.at[index, 'Journal_y'] = details['journal']
#
#
# df.to_csv(
#     '/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/final_reorganized_genes_updated.csv',
#     index=False)

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def fetch_pubmed_details(pmid, retries=3):
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
    headers = {'User-Agent': 'Mozilla/5.0'}

    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        print(response.text)  # Debugging line to see the XML
        return parse_pubmed_response(response.text)
    except requests.exceptions.SSLError as ssl_err:
        print(f"SSL error occurred: {ssl_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        return None


def parse_pubmed_response(xml_data):
    root = ET.fromstring(xml_data)
    article_info = {
        "title": None,
        "authors": [],
        "year": None,
        "journal": None
    }

    article = root.find('.//Article')
    if article:
        article_info['title'] = article.findtext('.//ArticleTitle')
        journal = article.find('.//Journal')
        if journal:
            article_info['journal'] = journal.findtext('.//Title')
            article_info['year'] = journal.find('.//PubDate/Year').text if journal.find(
                './/PubDate/Year') is not None else "Year not found"

        authors = root.findall('.//AuthorList/Author')
        for author in authors:
            lastname = author.findtext('LastName')
            forename = author.findtext('ForeName')
            if lastname and forename:
                article_info['authors'].append(f"{forename} {lastname}")

    return article_info


def format_authors(authors_list):
    if not authors_list:
        return "Unknown"
    if len(authors_list) > 1:
        return f"{authors_list[0]} et al."
    return authors_list[0]


# Load the dataset
df = pd.read_csv(
    '/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/final_reorganized_genes.csv',
    low_memory=False)
missing_titles = df[(df['Title_y'] == 'Unavailable due to server error') & (df['PMID_y'].notna())]

# Counter to keep track of requests
request_counter = 0
save_interval = 10  # Number of requests after which to save the DataFrame

for index, row in missing_titles.iterrows():
    details = fetch_pubmed_details(row['PMID_y'])
    if details:
        df.at[index, 'Title_y'] = details['title']
        if 'authors' in details:
            authors_formatted = format_authors(details['authors'])
            df.at[index, 'Author_y'] = authors_formatted
        df.at[index, 'Year_y'] = details['year']
        df.at[index, 'Journal_y'] = details['journal']

    request_counter += 1
    if request_counter % save_interval == 0:
        df.to_csv(
            '/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/final_reorganized_genes_updated.csv',
            index=False)
        print(f"Data saved after {request_counter} requests")

# Final save after the loop
df.to_csv(
    '/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/final_reorganized_genes_updated.csv',
    index=False)
print("Final data save complete")