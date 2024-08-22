import pandas as pd

# Load data from ClinVar_csvs folder
df = pd.read_csv("/Users/annebranum/PycharmProjects/ClinVar_data_organization/ClinVar_csvs/ClinVar_combined.csv")

# Modify DataFrame slices using .loc
df_filtered = df[df['dbsnp_id'].notna()].copy()  # Create a copy to avoid modifying the original DataFrame
df_filtered['dbsnp_id'] = df_filtered['dbsnp_id'].apply(lambda x: x.split('|')) # Split entries of dbsnp_id with multiples
df_exploded = df_filtered.explode('dbsnp_id') # Explode into separate lines

# Group data by gene and snp id, considering the future deprecation
grouped_df = df_exploded.groupby('gene(s)', group_keys=False).apply(
    lambda x: x.sort_values(['dbsnp_id'], ascending=False)[['gene(s)', 'dbsnp_id'] + list(x.columns.difference(['gene(s)', 'dbsnp_id']))]
)

# Save to a new CSV file to Processed csvs folder
grouped_df.to_csv('/Users/annebranum/PycharmProjects/ClinVar_data_organization/Processed_csvs/reorganized_genes.csv', index=False)

# Print the first few rows to verify
print(grouped_df.head())