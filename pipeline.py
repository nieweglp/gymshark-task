import json
import datetime
from apache_beam import Map, Pipeline
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT_ID = "gymshark"
PUBSUB_TOPIC_NAME = "backend-events-topic"
PUBSUB_TOPIC_NAME_SUBSCRIPTION = "backend-events-topic-sub"


def transform_event_data(element):
    event = json.loads(element.decode('utf-8'))
    event_type = event.get("event_type", "")

    if event_type == "order":
        # Prepare order event for BigQuery and GCS
        order_data = {
            'order_id': event['order_id'],
            'customer_id': event['customer_id'],
            'order_date': event['order_date'],
            'status': event['status'],
            'total_amount': event['total_amount'],
            'source_file': 'source_file_name',  # Populate dynamically if needed
            'batch_id': 'batch_001'  # Populate dynamically if needed
        }
        yield ('order', order_data)
        
        # Process the items (separate table for items)
        for item in event['items']:
            item_data = {
                'product_id': item['product_id'],
                'product_name': item['product_name'],
                'order_id': event['order_id'],
                'quantity': item['quantity'],
                'price': item['price'],
                'source_file': 'source_file_name',
                'batch_id': 'batch_001'
            }
            yield ('items', item_data)

        # Process shipping address (separate table for shipping address)
        shipping_address_data = {
            'order_id': event['order_id'],
            'street': event['shipping_address']['street'],
            'city': event['shipping_address']['city'],
            'country': event['shipping_address']['country'],
            'source_file': 'source_file_name',
            'batch_id': 'batch_001'
        }
        yield ('shipping_address', shipping_address_data)
        
    elif event_type == "inventory":
        inventory_data = {
            'inventory_id': event['inventory_id'],
            'product_id': event['product_id'],
            'warehouse_id': event['warehouse_id'],
            'quantity_change': event['quantity_change'],
            'reason': event['reason'],
            'timestamp': event['timestamp'],
            'source_file': 'source_file_name',
            'batch_id': 'batch_001'
        }
        yield ('inventory', inventory_data)

    elif event_type == "user_activity":
        user_activity_data = {
            'user_id': event['user_id'],
            'activity_type': event['activity_type'],
            'ip_address': event['ip_address'],
            'user_agent': event['user_agent'],
            'timestamp': event['timestamp'],
            'session_id': event['metadata']['session_id'],
            'platform': event['metadata']['platform'],
            'source_file': 'source_file_name',
            'batch_id': 'batch_001'
        }
        yield ('user_activity', user_activity_data)


def write_to_gcs(element):
    event_type, event = element
    timestamp = event.get("timestamp", 0)
    dt = datetime.datetime.fromtimestamp(timestamp / 1000)  # Convert milliseconds to seconds
    year, month, day = dt.year, dt.month, dt.day
    filename = f"{event_type}/{year}/{month:02d}/{day:02d}/order_{dt.strftime("%Y%m%d%H%M%S")}.json"

    output_path = f"gs://your-bucket-name/output/{filename}"
    with open(output_path, "w") as f:
        json.dump(event, f)


def write_to_bigquery(table_name):
    def _write_to_bigquery(element):
        event_type, event = element
        
        # Define schema based on event_type
        if event_type == "order":
            schema = {
                "fields": [
                    {"name": "order_id", "type": "STRING"},
                    {"name": "customer_id", "type": "STRING"},
                    {"name": "order_date", "type": "DATE"},
                    {"name": "status", "type": "STRING"},
                    {"name": "total_amount", "type": "FLOAT64"},
                    {"name": "source_file", "type": "STRING"},
                    {"name": "batch_id", "type": "STRING"},
                ]
            }

        elif event_type == "items":
            schema = {
                "fields": [
                    {"name": "product_id", "type": "STRING"},
                    {"name": "product_name", "type": "STRING"},
                    {"name": "order_id", "type": "STRING"},
                    {"name": "quantity", "type": "INT64"},
                    {"name": "price", "type": "FLOAT64"},
                    {"name": "source_file", "type": "STRING"},
                    {"name": "batch_id", "type": "STRING"},
                ]
            }

        elif event_type == "shipping_address":
            schema = {
                "fields": [
                    {"name": "order_id", "type": "STRING"},
                    {"name": "street", "type": "STRING"},
                    {"name": "city", "type": "STRING"},
                    {"name": "country", "type": "STRING"},
                    {"name": "source_file", "type": "STRING"},
                    {"name": "batch_id", "type": "STRING"},
                ]
            }

        elif event_type == "inventory":
            schema = {
                "fields": [
                    {"name": "inventory_id", "type": "STRING"},
                    {"name": "product_id", "type": "STRING"},
                    {"name": "warehouse_id", "type": "STRING"},
                    {"name": "quantity_change", "type": "INT64"},
                    {"name": "reason", "type": "STRING"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "source_file", "type": "STRING"},
                    {"name": "batch_id", "type": "STRING"},
                ]
            }

        elif event_type == "user_activity":
            schema = {
                "fields": [
                    {"name": "user_id", "type": "STRING"},
                    {"name": "activity_type", "type": "STRING"},
                    {"name": "ip_address", "type": "STRING"},
                    {"name": "user_agent", "type": "STRING"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "session_id", "type": "STRING"},
                    {"name": "platform", "type": "STRING"},
                    {"name": "source_file", "type": "STRING"},
                    {"name": "batch_id", "type": "STRING"},
                ]
            }

        # Write to BigQuery
        yield bigquery.WriteToBigQuery(
            table=table_name,
            schema=schema,
            method=bigquery.BigQueryDisposition.WRITE_APPEND
        )


def run(argv=None):
    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        temp_location=f"gs://{PROJECT_ID}/temp",
        region="US",
        streaming=True
    )

    with Pipeline(options=options) as p:
        pubsub_data = (
            p | "ReadFromPubSub" >> ReadFromPubSub(
                topic=PUBSUB_TOPIC_NAME,
                subscription=PUBSUB_TOPIC_NAME_SUBSCRIPTION
            )
        )
        transformed_data = pubsub_data | "TransformEventData" >> Map(transform_event_data)

        # Write to GCS
        transformed_data | "WriteToGCS" >> Map(write_to_gcs)

        # Write to BigQuery
        transformed_data | "WriteOrderToBigQuery" >> Map(write_to_bigquery("gymshark.data_us.orders"))
        transformed_data | "WriteItemsToBigQuery" >> Map(write_to_bigquery("gymshark.data_us.items"))
        transformed_data | "WriteShippingAddressToBigQuery" >> Map(write_to_bigquery("gymshark.data_us.shipping_address"))
        transformed_data | "WriteInventoryToBigQuery" >> Map(write_to_bigquery("gymshark.data_us.inventory"))
        transformed_data | "WriteUserActivityToBigQuery" >> Map(write_to_bigquery("gymshark.data_us.user_activity"))


if __name__ == "__main__":
    run()
