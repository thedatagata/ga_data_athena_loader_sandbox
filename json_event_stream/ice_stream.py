import dlt
from dlt.sources.filesystem import filesystem
from dlt.destinations.adapters import athena_partition, athena_adapter
from fastapi import FastAPI, HTTPException
from datetime import datetime
from schemas import PageviewsSchema, SessionsSchema

app = FastAPI()

@app.post("/pageview_data")
async def stream_pageview_data(data: PageviewsSchema):
    pv_obj = data.dict()
    try:
        pageview_json_stream = dlt.pipeline(
            pipeline_name="pageviews_stream",
            destination="athena",
            dataset_name="raw_data",
            staging="filesystem"
        )
        @dlt.resource(table_format="iceberg", write_disposition="merge")
        def pageview_resource(pv_obj):
            pv_obj["pageview_timestamp"] = datetime.strptime(pv_obj["pageview_timestamp"], "%Y-%m-%d %H:%M:%S")
            yield pv_obj

        athena_adapter(
            pageview_resource,
            partition=[
                athena_partition.day("pageview_timestamp"),
                athena_partition.month("pageview_timestamp"),
                athena_partition.year("pageview_timestamp"),
            ]
        )

        pageview_json_stream.run(pageview_resource(pv_obj), table_name="pageviews")
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/session_data")
async def stream_session_data(data: SessionsSchema):
    s_obj = data.dict()
    try:
        session_json_stream = dlt.pipeline(
            pipeline_name="sessions_stream",
            destination="athena",
            dataset_name="raw_data",
            staging="filesystem"
        )
        @dlt.resource(table_format="iceberg", write_disposition="merge", primary_key="session_id")
        def session_resource(s_obj):
            s_obj["session_start_time"] = datetime.fromtimestamp(s_obj["session_start_time"])
            yield s_obj

        athena_adapter(
            session_resource,
            partition=[
                athena_partition.year("session_start_time"),
                athena_partition.month("session_start_time"),
                athena_partition.day("session_start_time"),
            ]
        )

        session_json_stream.run(session_resource(s_obj), table_name="sessions")
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))