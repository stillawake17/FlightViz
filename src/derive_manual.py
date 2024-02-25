import pandas as pd

# Load the merged dataset
#df = pd.read_csv('data/merged_dataset_deduplicated.csv')

df = pd.read_csv('data/cleaned_dataset.csv')

# Assuming df is your DataFrame after loading the merged dataset
# Ensure the 'Date' column is in datetime format to extract year-month
df['Date'] = pd.to_datetime(df['Date'])

# Extract year-month into a new column for more precise analysis
df['YearMonth'] = df['Date'].dt.to_period('M')

import pandas as pd

def get_time_category(atd):
    try:
        # Updated to handle time format with seconds
        atd_time = pd.to_datetime(atd, format='%H:%M:%S').time()
    except:
        return "Invalid time format"
    hour = atd_time.hour
    minute = atd_time.minute
    
    if hour == 23 and minute < 30:
        return "Shoulder hour flights"
    if (hour == 23 and minute >= 30) or hour < 6:
        return "Night hour arrivals"
    if hour == 6:
        return "Shoulder hour flights"
    return "Regular arrivals"

# Example usage with a pandas DataFrame
# Ensure you have a DataFrame 'df' with the correct 'ATD' data
# df['TimeCategory'] = df['ATD'].apply(lambda x: get_time_category(x))


# Apply the function to determine the time category for each flight
df['TimeCategory'] = df['ATD'].apply(lambda x: get_time_category(x))



# Assuming 'df' is your DataFrame and it has a column named 'DateTime' in datetime format
# If 'DateTime' is not already in datetime format, convert it first
df['DateTime'] = pd.to_datetime(df['DateTime'])

# Extract the year from the 'DateTime' column
df['Year'] = df['DateTime'].dt.year

# Group by 'Year' and 'TimeCategory', then count the occurrences
time_category_by_year = df.groupby(['Year', 'TimeCategory']).size().reset_index(name='Count')

# Display the result
print(time_category_by_year)


# Get counts of flights arriving by day (considering the whole date for uniqueness)
flights_by_day = df.groupby(df['Date'].dt.date).size()

# Get counts of flights arriving by year-month
flights_by_year_month = df.groupby('YearMonth').size()

# Get counts of flights by time category
flights_by_time_category = df.groupby('TimeCategory').size()


# Export flights by day to CSV
flights_by_day.to_csv('flights_by_day.csv', header=['Count'])

# Export flights by year-month to CSV
flights_by_year_month.to_csv('flights_by_year_month.csv', header=['Count'])

# Export flights by time category to CSV
flights_by_time_category.to_csv('flights_by_time_category.csv', header=['Count'])


# Display the results
print("Flights by Day:\n", flights_by_day)
print("\nFlights by Year-Month:\n", flights_by_year_month)
print("\nFlights by Time Category:\n", flights_by_time_category)

# Group by the date (YYYY-MM-DD) and TimeCategory, then count the occurrences
time_category_by_day = df.groupby(df['DateTime'].dt.date)['TimeCategory'].value_counts().reset_index(name='Count')

# Export to CSV
time_category_by_day.to_csv('time_category_by_day.csv', index=False)

# Extract year and month from 'DateTime'
df['YearMonth'] = df['DateTime'].dt.to_period('M')

# Group by 'YearMonth' and 'TimeCategory', then count the occurrences
time_category_by_month_year = df.groupby(['YearMonth', 'TimeCategory']).size().reset_index(name='Count')

# Export to CSV
time_category_by_month_year.to_csv('time_category_by_month_year.csv', index=False)
