import pandas as pd
import os

# List of file names of your CSV files
csv_files = ['processed_manual_data12.csv', 'processed_manual_data11.csv', 
             'processed_manual_data10.csv', 'processed_manual_data9.csv', 
             'processed_manual_data8.csv', 'processed_manual_data7.csv', 
             'processed_manual_data6.csv', 'processed_manual_data5.csv', 
             'processed_manual_data4.csv', 'processed_manual_data3.csv', 
             'processed_manual_data2.csv', 'processed_manual_data.csv']

# Assuming all files are in the same directory as your script
# You can change the directory path as needed
directory_path = 'data/'

# Initialize an empty list to store DataFrames
dataframes = []

for file_name in csv_files:
    file_path = os.path.join(directory_path, file_name)
    # Read each CSV file and append to the list
    df = pd.read_csv(file_path)
    dataframes.append(df)

# Concatenate all DataFrames in the list into a single DataFrame
final_dataframe = pd.concat(dataframes, ignore_index=True)

# Export the concatenated DataFrame to a new CSV file
final_dataframe.to_csv('data/merged_dataset.csv', index=False)

print("All files have been merged into merged_dataset.csv")
