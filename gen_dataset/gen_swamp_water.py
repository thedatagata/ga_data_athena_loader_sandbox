import duckdb

# Define your file path and read the data
path_to_csv = '{path_to_train_v2.csv}'
data_swamp = duckdb.query(f"""
    SELECT *,
           DATE_TRUNC('month', TO_DATE(date, '%Y%m%d')) AS month_start
    FROM read_csv_auto('{path_to_csv}', max_line_size=5000000)
""").to_df()

# Get unique months and iterate over each to save separate CSV files
for month in data_swamp['month_start'].unique():
    # Filter data for the specific month
    monthly_data = data_swamp[data_swamp['month_start'] == month]
    # Save to CSV with the start date of the month in the file name
    month_str = month.strftime('%Y-%m-%d')
    monthly_data.to_csv(f'monthly_data_{month_str}.csv', index=False)


