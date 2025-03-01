import json
import datetime
import time
from typing import Any, Generator
from uuid import uuid4
from google.cloud import storage
from apache_beam import Map, Pipeline, PCollection, Filter
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

from gymshark.schemas import SCHEMAS


PROJECT_ID = "gymshark"
BUCKET_NAME = "gymshark_gcs"
PUBSUB_TOPIC_NAME = "backend-events-topic"
PUBSUB_TOPIC_NAME_SUBSCRIPTION = "backend-events-topic-sub"
DATASET = "data_us"


def generate_source_file_name(event_type: str) -> str:
    """ Function generates name of file base on current timestamp """
    miliseconds = str(int(time.time() * 1000))[-4:]
    return f"{event_type}_{datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")}{miliseconds}"


def generate_batch_id() -> str:
    """ Function generates UUID4 """
    return uuid4()


def transform_event_data(element) -> Generator[Any, None, None]:
    """ Function yielding specific stream of data with adding meta data: 
    source_file_name to save to GCS, batch_id, whichc is UUID4 and load_timestamp """
    event = json.loads(element.decode("utf-8"))
    event_type = event.get("event_type", "")
    source_file_name = generate_source_file_name(event_type)
    batch_id = generate_batch_id()
    load_timestamp = datetime.datetime.now(datetime.timezone.utc)

    if event_type == "order":
        order_data = {
            "order_id": event["order_id"],
            "customer_id": event["customer_id"],
            "order_date": event["order_date"],
            "status": event["status"],
            "total_amount": event["total_amount"],
            "source_file": source_file_name,
            "batch_id": batch_id,
            "load_timestamp": load_timestamp,
        }
        yield ("order", order_data)

        for item in event["items"]:
            item_data = {
                "item_id": generate_batch_id(),
                "product_id": item["product_id"],
                "product_name": item["product_name"],
                "order_id": event["order_id"],
                "quantity": item["quantity"],
                "price": item["price"],
                "source_file": source_file_name,
                "batch_id": batch_id,
                "load_timestamp": load_timestamp,
            }
            yield ("item", item_data)

        shipping_address_data = {
            "shipping_address_id": generate_batch_id(),
            "order_id": event["order_id"],
            "street": event["shipping_address"]["street"],
            "city": event["shipping_address"]["city"],
            "country": event["shipping_address"]["country"],
            "source_file": source_file_name,
            "batch_id": batch_id,
            "load_timestamp": load_timestamp,
        }
        yield ("shipping_address", shipping_address_data)

    elif event_type == "inventory":
        inventory_data = {
            "inventory_id": event["inventory_id"],
            "product_id": event["product_id"],
            "warehouse_id": event["warehouse_id"],
            "quantity_change": event["quantity_change"],
            "reason": event["reason"],
            "timestamp": event["timestamp"],
            "source_file": source_file_name,
            "batch_id": batch_id,
            "load_timestamp": load_timestamp,
        }
        yield ("inventory", inventory_data)

    elif event_type == "user_activity":
        user_activity_data = {
            "user_activity_id": generate_batch_id(),
            "user_id": event["user_id"],
            "activity_type": event["activity_type"],
            "ip_address": event["ip_address"],
            "user_agent": event["user_agent"],
            "timestamp": event["timestamp"],
            "session_id": event["metadata"]["session_id"],
            "platform": event["metadata"]["platform"],
            "source_file": source_file_name,
            "batch_id": batch_id,
            "load_timestamp": load_timestamp,
        }
        yield ("user_activity", user_activity_data)


def write_to_gcs(element: dict | PCollection) -> None:
    """ Function save dict or PCollection to json file in GCS """
    event_type, event = element
    timestamp = event.get("load_timestamp", datetime.datetime.now(datetime.timezone.utc))
    
    year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute
    filename = f"{event_type}/{year}/{month:02d}/{day:02d}/{hour:02d}/{minute:02d}/{event_type}_{timestamp.strftime("%Y%m%d%H%M%S")}.json"

    bucket = storage.Client().get_bucket(BUCKET_NAME)
    output_path = f"gs://output/{filename}"
    blob = bucket.blob(output_path)
    blob.upload_from_string(
        data=json.dumps(event, indent=4, sort_keys=True, default=str), content_type='application/json'
        )
    

def write_to_bigquery(table_name: str) -> None:
    """ Function save to BigQuery with specific schema """
    schema = SCHEMAS.get(table_name)

    if not schema:
        raise ValueError(f"Unknown table: {table_name}")
    
    return bigquery.WriteToBigQuery(
            table=table_name,
            dataset=DATASET,
            project=PROJECT_ID,
            schema=schema,
            create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
            method=bigquery.BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy="RETRY_ALWAYS"
        )


def filter_by_event_type(element: tuple, event_type: str) -> PCollection:
    """ Filter event by type in tuple """
    return element[0] == event_type


def run(argv=None) -> None:
    """ Final method to run Apache Beam Pipeline """
    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        temp_location=f"gs://{PROJECT_ID}/temp",
        region="US",
        streaming=True,
    )

    with Pipeline(options=options) as p:
        pubsub_data = p | "ReadFromPubSub" >> ReadFromPubSub(
            topic=PUBSUB_TOPIC_NAME, 
            subscription=PUBSUB_TOPIC_NAME_SUBSCRIPTION
        )
        transformed_data = pubsub_data | "TransformEventData" >> Map(
            transform_event_data
        )

        transformed_data | "WriteToGCS" >> Map(write_to_gcs)

        _ = (
            transformed_data 
            | "Filter event_type order" >> Filter(filter_by_event_type, "order")
            | "Write orders to BigQuery" >> write_to_bigquery("order")
        )

        _ = (
            transformed_data
            | "Filter event_type item" >> Filter(filter_by_event_type, "item")
            | "Write items to BigQuery" >> write_to_bigquery("item")
        )
        
        _ = (
            transformed_data
            | "Filter event_type shipping_address" >> Filter(filter_by_event_type, "shipping_address")
            | "Write shipping_address to BigQuery" >> write_to_bigquery("shipping_address")
        )
        
        _ = (
            transformed_data
            | "Filter event_type inventory" >> Filter(filter_by_event_type, "inventory")
            | "Write inventory to BigQuery" >> write_to_bigquery("inventory")
        )

        _ = (
            transformed_data
            | "Filter event_type user_activity" >> Filter(filter_by_event_type, "user_activity")
            | "Write user_activity to BigQuery" >> write_to_bigquery("user_activity")
        )


if __name__ == "__main__":
    run()
