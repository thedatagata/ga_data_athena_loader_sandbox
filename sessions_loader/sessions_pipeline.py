import os 
import dlt
from dlt.sources.filesystem import filesystem
from dlt.destinations.adapters import athena_partition, athena_adapter
import pandas as pd
import enlighten 
from datetime import datetime

def execute_sessions_pipeline():
    # Define the pipeline for Athena with Iceberg table configuration
    athena_sessions_pipeline = dlt.pipeline(
        pipeline_name="sessions_pipeline",
        destination="athena",
        dataset_name="raw_data",
        staging="filesystem"
    )

    def gen_session_chunks(df):
        df['session_start_time'] = df['session_start_time'].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))
        df['session_start_time'] = pd.to_datetime(df['session_start_time']) 
        yield df.to_dict(orient='records')

    # Resource to read data from CSV files and yield lists of dictionaries
    @dlt.resource(table_format="iceberg", write_disposition="merge", primary_key="session_id" , parallelized=True)
    def session_reader(file_data):
        # Read the CSV file in chunks and yield each chunk as a list of dictionaries
        for df in pd.read_csv(file_data['file_url'], dtype={"user_id": str, "session_id": str}, chunksize=20000):
            df.drop_duplicates(subset=['session_id'], keep='first', inplace=True)
            yield from gen_session_chunks(df)

    athena_adapter(
        session_reader,
        partition=[
            athena_partition.year("session_start_time"),
            athena_partition.month("session_start_time"),
            athena_partition.day("session_start_time"),
        ]
    )

    total_files = len(os.listdir(dlt.config["sources.filesystem.file_directory"]))
    manager = enlighten.get_manager()
    pbar = manager.counter(total=total_files, desc='Processing CSV', unit='files')
    for f in filesystem(bucket_url=dlt.config["sources.filesystem.file_directory"], file_glob='*.csv'):
        run_info = athena_sessions_pipeline.run(session_reader(f), table_name="sessions")
        pbar.update(1)
    pbar.close() 

if __name__ == "__main__":
    execute_sessions_pipeline()
