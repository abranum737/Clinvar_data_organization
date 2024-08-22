import pandas as pd

def extract_unique_gene_names(input_file, output_file):
    # Load the CSV file
    df = pd.read_csv(input_file)

    # Extract the first column
    gene_names = df.iloc[:, 0]

    # Split the gene names and remove LOC information
    gene_names = gene_names.apply(lambda x: x.split('|')[0] if isinstance(x, str) else x)

    # Remove duplicates
    unique_gene_names = gene_names.drop_duplicates().reset_index(drop=True)

    # Remove rows containing 'LOC'
    unique_gene_names = unique_gene_names[~unique_gene_names.str.contains('LOC', na=False)]

    # Convert to DataFrame
    unique_gene_names_df = pd.DataFrame(unique_gene_names, columns=['gene(s)'])

    # Save to a new CSV file
    unique_gene_names_df.to_csv(output_file, index=False)
    print(f"Unique gene names saved to {output_file}")

# Define file paths
input_file = 'reorganized_genes.csv'
output_file = 'unique_gene_names.csv'

# Extract unique gene names
extract_unique_gene_names(input_file, output_file)
