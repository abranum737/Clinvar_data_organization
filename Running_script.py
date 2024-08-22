import ClinVar_txt_to_csv



# Running ClinVar_txt_to_csv txt_to_csv method to clean txt files downloaded from ClinVar and put them into csv format.
# Then calls combine_csvs to combine all csvs into one

# txt files downloaded from https://www.ncbi.nlm.nih.gov/clinvar with search terms
filename = ['clinvar_result_covid.txt', 'clinvar_result_aging.txt', 'clinvar_result_senescence.txt', 'clinvar_result_death.txt', 'clinvar_result_longevity.txt', 'clinvar_result_diabetes.txt', 'clinvar_result_cell-age.txt', 'clinvar_result_ctrls.txt']

# local path to txt files
path = '/Users/annebranum/Documents/Genomics_project/'
pathlength = len(path)

# Calls the method on each txt file to generate cleaned csv files
for file in filename:
    filepath = (path + file)
    ClinVar_txt_to_csv.txt_to_csv(filepath, pathlength)

# Calls the method on each csv file to generate combined file
folder_path = r'/Users/annebranum/PycharmProjects/ClinVar_data_organization/ClinVar_csvs'
ClinVar_txt_to_csv.combine_csvs(folder_path)