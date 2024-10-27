import pandas as pd
import duckdb
from datetime import datetime, timedelta 

db = duckdb.connect(database=':memory:', read_only=False)
data_swamp = duckdb.query("SELECT * FROM read_csv_auto('{path_to_train_v2.csv}', max_line_size=5000000)")

# 638 is the number of days between the earliest date and the latest date in the dataset + 1 because range stops 1 before the second number
for i in range(0, 638):
    # 2730 is the number of days between the earliest date in the dataset and today
    x = 2730 - i 
    df = duckdb.query(f"SELECT * FROM data_swamp ds WHERE DATEDIFF('d', strptime(ds.date, '%Y%m%d'), CURRENT_DATE) = {x}").df() 
    fn_dt = pd.to_datetime(df['date'], format='%Y%m%d')[0].strftime('%d-%m-%Y')
    print(fn_dt)
    df.to_csv(f"../data_swamp/swamp_water-{fn_dt}.csv", index=False)