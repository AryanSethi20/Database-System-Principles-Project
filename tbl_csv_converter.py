import csv
import os

# Set the folder where your .tbl files are located
tbl_folder = '/Users/chaitanya/Documents/NTU/Y3S1/SC3020-Database/sc3020-project-2/tpc-tbl-files'

# Create a folder for the CSV files
csv_folder = '/Users/chaitanya/Documents/NTU/Y3S1/SC3020-Database/sc3020-project-2/tpc-csv-files'
os.makedirs(csv_folder, exist_ok=True)

for filename in os.listdir(tbl_folder):
    if filename.endswith(".tbl"):
        tbl_file = os.path.join(tbl_folder, filename)
        csv_file = os.path.join(csv_folder, os.path.splitext(filename)[0] + '.csv')

        with open(tbl_file, 'r') as tbl, open(csv_file, 'w', newline='') as csv:
            # Read the data from the .tbl file and write it to the .csv file
            for line in tbl:
                row = line.strip().split('|')
                csv.write(','.join(row) + '\n')

        print(f'Conversion of {filename} complete. Data has been written to {csv_file}')