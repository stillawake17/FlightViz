import pandas as pd

# Load the data
file_path = 'Total daily flights.csv'  # Update this path as needed
data = pd.read_csv(file_path)

# Calculate the mean for each numerical column
mean_values = data[['Total', 'night', 'shoulder', 'regular']].mean()

# Impute missing values with the mean
data_imputed = data.fillna(mean_values)

# round the data to no decimal places
data_imputed = data_imputed.round(0)

# Export the imputed data to a new CSV file
output_file_path = 'Imputed_Total_daily_flights.csv'  # Update this path as needed
data_imputed.to_csv(output_file_path, index=False)

print(f"Imputed data saved to {output_file_path}")
