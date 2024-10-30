import os 
import dlt
from dlt.sources.filesystem import filesystem
from dlt.destinations.adapters import athena_partition, athena_adapter
import pandas as pd
import enlighten
from datetime import datetime

def execute_pageviews_pipeline():
    # Define the pipeline for Athena with Iceberg table configuration
    athena_pageviews_pipeline = dlt.pipeline(
        pipeline_name="pageviews_pipeline",
        destination="athena",
        dataset_name="raw_data",
        staging="filesystem"
    )

    def gen_session_chunks(df):
        df.drop_duplicates(subset=['pageview_id'], inplace=True)
        df['session_start_time'] = df['session_start_time'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
        df['pageview_timestamp'] = df['pageview_timestamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
        yield df.to_dict(orient='records')

    # Resource to read data from CSV files and yield lists of dictionaries
    @dlt.resource(table_format="iceberg", write_disposition="merge", primary_key="pageview_id", parallelized=True)
    def pageview_reader(file_data):
        # Read the CSV file in chunks and yield each chunk as a list of dictionaries
        for df in pd.read_csv(file_data['file_url'], dtype={"user_id": str, "session_id": str, "pageview_id": str}, chunksize=20000):
            yield from gen_session_chunks(df)

    athena_adapter(
        pageview_reader,
        partition=[
            athena_partition.year("pageview_timestamp"),
            athena_partition.month("pageview_timestamp"),
            athena_partition.day("pageview_timestamp"),
        ]
    )

    total_files = len(os.listdir(dlt.config["sources.filesystem.file_directory"]))
    manager = enlighten.get_manager()
    pbar = manager.counter(total=total_files, desc='Processing CSV', unit='files')
    for f in filesystem(bucket_url=dlt.config["sources.filesystem.file_directory"], file_glob='*.csv'):
        run_info = athena_pageviews_pipeline.run(pageview_reader(f), table_name="pageviews")
        pbar.update(1)
    pbar.close() 

if __name__ == "__main__":
    execute_pageviews_pipeline()