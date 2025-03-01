import datetime
import json
from unittest.mock import patch
import uuid
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
import pytest
from gymshark.pipeline import (
    filter_by_event_type,
    generate_source_file_name,
    generate_batch_id,
    transform_event_data,
    write_to_gcs,
    write_to_bigquery,
)


def test_generate_source_file_name():
    event_type = "order"
    file_name = generate_source_file_name(event_type)
    assert file_name.startswith("order_")
    assert len(file_name) == 24


def test_generate_batch_id():
    batch_id = generate_batch_id()
    assert isinstance(batch_id, uuid.UUID)


@pytest.fixture
def sample_data():
    sample_order = {
        "event_type": "order",
        "order_id": "f652c643-29e6-46dd-9e62-144f5b442302",
        "customer_id": "22b30555-6efd-4e32-a9c8-8a4dca984da7",
        "order_date": "2023-01-01",
        "status": "delivered",
        "total_amount": 100.50,
        "items": [
            {
                "product_id": "2f4da12a-4334-4342-bbb8-d7fc2cafcb22", 
                "product_name": "Product 1", 
                "quantity": 2, 
                "price": 50.25
            }
        ],
        "shipping_address": 
            {
                "street": "123 St", 
                "city": "NYC", 
                "country": "USA"
            }
    }
    return sample_order


def test_transform_event_data(sample_data):
    sample_event = json.dumps(sample_data).encode("utf-8")

    results = list(transform_event_data(sample_event))

    assert len(results) == 3
    assert results[0][0] == "order"
    assert results[1][0] == "item"
    assert results[2][0] == "shipping_address"


@patch("gymshark.pipeline.storage.Client")
def test_write_to_gcs(mock_storage_client, sample_data):
    mock_bucket = mock_storage_client().get_bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    sample_data = ("order", sample_data)
    write_to_gcs(sample_data)

    mock_bucket.blob.assert_called()
    mock_blob.upload_from_string.assert_called()


@patch("gymshark.pipeline.bigquery.WriteToBigQuery")
def test_write_to_bigquery(mock_write_to_bigquery, sample_data):
    sample_data = ("order", sample_data)
    list(write_to_bigquery("order"))
    mock_write_to_bigquery.assert_called()


@pytest.mark.parametrize(
    "element, event_type, expected",
    [
        (("order", {"order_id": "123"}), "order", True),
        (("item", {"product_id": "456"}), "item", True),
        (("shipping_address", {"street": "123 St"}), "shipping_address", True),
        (("order", {"order_id": "123"}), "item", False),
        (("user_activity", {"user_id": "789"}), "inventory", False), 
        (("inventory", {"inventory_id": "999"}), "inventory", True),
        (("random_type", {"data": "test"}), "order", False), 
    ],
)
def test_filter_by_event_type(element, event_type, expected):
    assert filter_by_event_type(element, event_type) == expected


def is_valid_uuid(value):
    """ Check value for correct UUID. """
    return isinstance(value, uuid.UUID)

def is_valid_timestamp(value):
    """ Check value is correct timestamp datetime """
    return isinstance(value, datetime.datetime)

def test_transform_pipeline(sample_data):
    sample_event = json.dumps(sample_data).encode("utf-8")

    with TestPipeline() as p:
        input_data = p | "Create test input" >> beam.Create([sample_event])
        transformed_data = input_data | "Transform" >> beam.FlatMap(transform_event_data)
        
        def validate_output(actual):
            """ Function to validate types """
            for _, data in actual:
                assert is_valid_uuid(data["batch_id"])
                assert is_valid_timestamp(data["load_timestamp"])

        assert_that(transformed_data, validate_output)
