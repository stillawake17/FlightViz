import pandas as pd

# Load the dataset
df = pd.read_csv('data/merged_dataset.csv')

# Remove duplicates
df_deduplicated = df.drop_duplicates()

# Save the deduplicated DataFrame to a new CSV file
df_deduplicated.to_csv('data/merged_dataset_deduplicated.csv', index=False)