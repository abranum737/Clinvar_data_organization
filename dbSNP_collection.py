import pandas as pd
import requests
from pandas import json_normalize
import os


# Uncomment these lines when using the real dataset
# file_path = '/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/reorganized_genes.csv'
# df = pd.read_csv(file_path)
# df['numeric_rs_id'] = df['dbsnp_id'].str.extract('rs(\d+)').dropna().astype(int)
# df = df.drop_duplicates(subset='numeric_rs_id')

# For testing purposes
data = {'numeric_rs_id': [200673370, 1554704214, 989584849, 987724889]}
df = pd.DataFrame(data)


# Define the base URL for dbSNP's REST API SPDI endpoint
base_url = "https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/"

def fetch_frequency_data(rsid):
    url = f"{base_url}{rsid}/frequency"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # Returns the JSON response
    else:
        print(f"Failed to fetch data for RSID {rsid}: HTTP {response.status_code}")
        return None

import pandas as pd

# Create a DataFrame from the provided mapping table
mapping_data = {
    'sub_id': ['1', 'AFR', 'ASN', '5', '6', '8', '9'],
    'sub_name': ['European', 'African', 'Asian', 'Latin American 1', 'Latin American 2', 'South Asian', 'Other'],
    'sub_group': ['Sub']*7,
    'sub_description': [
        'European',
        'All Africans, AFO and AFA Individuals',
        'Asian Individuals excluding South Asian',
        'Latin American individuals with Afro-Caribbean ancestry',
        'Latin American individuals with mostly European and Native American Ancestry',
        'South Asian',
        'The self-reported population is inconsistent with the GRAF-assigned population'
    ],
    'sub_biosample_id': ['SAMN10492695', 'SAMN10492703', 'SAMN10492704', 'SAMN10492699', 'SAMN10492700', 'SAMN10492702', 'SAMN11605645']
}
mapping_df = pd.DataFrame(mapping_data)


# Collect frequency data
frequency_data = []
for rsid in df['numeric_rs_id']:
    if pd.notna(rsid):
        response_data = fetch_frequency_data(rsid)
        if response_data:
            frequency_data.append(response_data)

if frequency_data:
    try:
        df_frequency = json_normalize(frequency_data)
        # Create a dictionary for the column header replacement
        header_mapping = dict(zip(mapping_df['sub_biosample_id'], mapping_df['sub_name']))
        # Rename the columns in df_frequency
        df_frequency.rename(columns=header_mapping, inplace=True)
        print(df_frequency.head())

        df_frequency.to_csv('/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/snp_frequency.csv', index=False)
    except Exception as e:
        print(f"Error normalizing frequency data: {e}")
else:
    print("No frequency data fetched.")

# Example JSON data provided
data_json = [
    {
        "id": "dbGaP_PopFreq.1",
        "bioproject_id": "PRJNA507278",
        "short_name": "dbGaP_PopFreq",
        "url": "https://www.ncbi.nlm.nih.gov/snp/docs/gsr/alfa/",
        "comment": "",
        "name": "Allele Frequency Aggregator",
        "description": "A new source of dbGaP aggregated frequency data (>1 Million Subjects) provided by dbSNP.",
        "release_date": "2023-07-18T14:59:21.324545+00:00",
        "submit_date": "2023-07-18T14:59:21.324545+00:00",
        "study_def_file": "",
        "study_data_file": [""],
        "populations": [
            {
                "description": "Total (~global) across all populations",
                "group": "Global",
                "id": "TOT",
                "biosample_id": "SAMN10492705",
                "name": "Total",
                "subs": [
                    {
                        "id": "1",
                        "name": "European",
                        "group": "Sub",
                        "description": "European",
                        "biosample_id": "SAMN10492695"
                    },
                    {
                        "id": "AFR",
                        "name": "African",
                        "group": "Sub",
                        "description": "All Africans, AFO and AFA Individuals",
                        "biosample_id": "SAMN10492703",
                        "subs": [
                            {
                                "id": "2",
                                "name": "African Others",
                                "group": "Sub",
                                "description": "Individuals with African ancestry",
                                "biosample_id": "SAMN10492696"
                            },
                            {
                                "id": "4",
                                "name": "African American",
                                "group": "Sub",
                                "description": "African American",
                                "biosample_id": "SAMN10492698"
                            }
                        ]
                    },
                    {
                        "id": "ASN",
                        "name": "Asian",
                        "group": "Sub",
                        "description": "Asian Individuals excluding South Asian",
                        "biosample_id": "SAMN10492704",
                        "subs": [
                            {
                                "id": "3",
                                "name": "East Asian",
                                "group": "Sub",
                                "description": "East Asian (95%)",
                                "biosample_id": "SAMN10492697"
                            },
                            {
                                "id": "7",
                                "name": "Other Asian",
                                "group": "Sub",
                                "description": "Asian Individuals excluding South or East Asian",
                                "biosample_id": "SAMN10492701"
                            }
                        ]
                    },
                    {
                        "id": "5",
                        "name": "Latin American 1",
                        "group": "Sub",
                        "description": "Latin American individuals with Afro-Caribbean ancestry",
                        "biosample_id": "SAMN10492699"
                    },
                    {
                        "id": "6",
                        "name": "Latin American 2",
                        "group": "Sub",
                        "description": "Latin American individuals with mostly European and Native American Ancestry",
                        "biosample_id": "SAMN10492700"
                    },
                    {
                        "id": "8",
                        "name": "South Asian",
                        "group": "Sub",
                        "description": "South Asian",
                        "biosample_id": "SAMN10492702"
                    },
                    {
                        "id": "9",
                        "name": "Other",
                        "group": "Sub",
                        "description": "The self-reported population is inconsistent with the GRAF-assigned population",
                        "biosample_id": "SAMN11605645"
                    }
                ]
            }
        ],
        "build_id": "20230706150541"
    }
]


# Assuming data_json is defined as provided earlier
try:
    df_populations = json_normalize(
        data=data_json,
        record_path=["populations", "subs"],
        meta=[
            'id',  # Top-level 'id' of the main document
            'bioproject_id',
            'short_name',
            'name',  # Top-level name
            'description',  # Top-level description
            'release_date',
            'submit_date',
            'url',
            ['populations.id', 'population_id'],
            ['populations.name', 'population_name'],
            ['populations.description', 'population_description'],
            ['populations.group', 'population_group'],
            ['populations.biosample_id', 'population_biosample_id']
        ],
        errors='ignore',
        record_prefix='sub_'
    )
    print(df_populations.head())
    # Directory path
    dir_path = '/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs'
    # Now save the CSV into the directory
    df_populations.to_csv(os.path.join(dir_path, 'normalized_data.csv'), index=False)

except Exception as e:
    print(f"Error normalizing population data: {e}")


