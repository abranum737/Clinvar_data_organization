import pandas as pd
import os

# Ensure that path names are correct. The file name should be in the format: 'clinvar_result_',   'search term', '.txt'
# Update file path to txt files as needed

def txt_to_csv(data_path, path_lenth):
    # Step 1: Load the data from a tab-delimited text file
    df = pd.read_csv(data_path, delimiter='\t', encoding='utf-8', low_memory=False) # low memory false for unexpected data types

    #file name information
    path_to_save = data_path[path_lenth:-4]
    search_term = path_to_save[15:]

    # Step 2: Normalize the data
    df.columns = [col.strip().replace(' ', '_').lower() for col in df.columns]

    # Step 3: Check for missing data and handle it
    # Fill missing values with 'unknown' or appropriate defaults
    df.fillna(value={'grch37chromosome': 'unknown', 'grch37location': 'unknown'}, inplace=True)

    # Step 4: Remove duplicates
    df.drop_duplicates(inplace=True)
    #remove unneccessary columns
    df = df.drop(columns=['somatic_clinical_impact', 'somatic_clinical_impact_date_last_evaluated', 'somatic_clinical_impact_review_status', 'oncogenicity_classification', 'oncogenicity_date_last_evaluated', 'oncogenicity_review_status', 'unnamed:_24'])


    # Step 5: Create categories (e.g., pathogenicity based on germline classification)
    # Check if 'germline_classification' column exists and create 'pathogenicity'
    if 'germline_classification' in df.columns:
        # Mapping germline classification to a simpler pathogenicity category
        def classify_pathogenicity(classification):
            if pd.isna(classification):
                return None  # or return some default value if appropriate
            try:
                classification = classification.lower()
                if 'conflicting classifications of pathogenicity' in classification:
                    return 'Conflicting classifications of pathogenicity'
                elif 'likely pathogenic' in classification:
                    return 'Likely Pathogenic'
                elif 'likely benign' in classification:
                    return 'Likely Benign'
                elif 'likely risk allele' in classification:
                    return 'Likely risk allele'
                elif 'uncertain significance' in classification:
                    return 'Uncertain Significance'
                elif 'drug response' in classification:
                    return 'Drug response'
                elif 'risk factor' in classification:
                    return 'Risk factor'
                elif 'conflicting classifications' in classification:
                    return 'Conflicting Classifications'
                elif 'benign' in classification:
                    return 'Benign'
                elif 'pathogenic' in classification:
                    return 'Pathogenic'
                else:
                    return 'Other'

            except AttributeError:
                return 'Error'  # Handle unexpected attribute errors

        df['pathogenicity'] = df['germline_classification'].apply(classify_pathogenicity)
    else:
        print("Error: 'germline_classification' column not found.")
        df['pathogenicity'] = 'Unknown'

    # Step 6: Indexing for quick access
    df.set_index('variationid', inplace=True)

    # Step 7: Data cleaning
    df['search_term'] = search_term
    df['germline_date_last_evaluated'] = pd.to_datetime(df['germline_date_last_evaluated'], errors='coerce')

    # Step 8: Enhance accessibility
    df['simple_description'] = df['canonical_spdi'].apply(
        lambda x: f"Mutation at {x.split(':')[2]} to {x.split(':')[3]}" if pd.notna(x) else 'Unknown')

    # Step 9: Integrate with external data (simulated example)
    df['additional_info'] = 'External data'  # Normally, merge with another DataFrame

    # Step 10: Prepare for analysis and reporting
    summary = df.groupby('pathogenicity').size()

    # Save the cleaned and processed dataframe to a new CSV file with unique name
    path_to_save = data_path[45:-4]
    df.to_csv(path_to_save + '.csv')

    # Print summary and the first few rows to check the output
    print(summary)
    print(df.head())

# Combines CSV files generated from txt to csv method
def combine_csvs(folder_path):
    # first check whether file already exists or not
    # calling remove method to delete the csv file if it exists
    file = 'ClinVar_combined.csv'
    if (os.path.exists(file) and os.path.isfile(file)):
        os.remove(file)
        print('Old combo csv deleted')
    else:
        pass

    all_files = os.listdir(folder_path)

    # Filter out non-CSV files
    csv_files = [f for f in all_files if f.endswith('.csv')]

    # Create a list to hold the dataframes
    df_list = []

    for csv in csv_files:
        file_path = os.path.join(folder_path, csv)
        try:
            # Try reading the file using default UTF-8 encoding
            df = pd.read_csv(file_path)
            df_list.append(df)
        except UnicodeDecodeError:
            try:
                # If UTF-8 fails, try reading the file using UTF-16 encoding with tab separator
                df = pd.read_csv(file_path, sep='\t', encoding='utf-16')
                df_list.append(df)
            except Exception as e:
                print(f"Could not read file {csv} because of error: {e}")
        except Exception as e:
            print(f"Could not read file {csv} because of error: {e}")

    # Concatenate all data into one DataFrame
    big_df = pd.concat(df_list, ignore_index=True)
    big_df.drop_duplicates(inplace=True)


    # Save the final result to a new CSV file
    big_df.to_csv(os.path.join(folder_path, 'ClinVar_combined.csv'))

