SCHEMAS = {
    "order": {
        "fields": [
            {"name": "order_id", "type": "STRING"},
            {"name": "customer_id", "type": "STRING"},
            {"name": "order_date", "type": "DATE"},
            {"name": "status", "type": "STRING"},
            {"name": "total_amount", "type": "FLOAT64"},
            {"name": "source_file", "type": "STRING"},
            {"name": "batch_id", "type": "STRING"},
            {"name": "load_timestamp", "type": "TIMESTAMP"},
        ]
    },
    "item": {
        "fields": [
            {"name": "item_id", "type": "STRING"},
            {"name": "product_id", "type": "STRING"},
            {"name": "product_name", "type": "STRING"},
            {"name": "order_id", "type": "STRING"},
            {"name": "quantity", "type": "INT64"},
            {"name": "price", "type": "FLOAT64"},
            {"name": "source_file", "type": "STRING"},
            {"name": "batch_id", "type": "STRING"},
            {"name": "load_timestamp", "type": "TIMESTAMP"},
        ]
    },
    "shipping_address": {
        "fields": [
            {"name": "shipping_address_id", "type": "STRING"},
            {"name": "order_id", "type": "STRING"},
            {"name": "street", "type": "STRING"},
            {"name": "city", "type": "STRING"},
            {"name": "country", "type": "STRING"},
            {"name": "source_file", "type": "STRING"},
            {"name": "batch_id", "type": "STRING"},
            {"name": "load_timestamp", "type": "TIMESTAMP"},
        ]
    },
    "inventory": {
        "fields": [
            {"name": "inventory_id", "type": "STRING"},
            {"name": "product_id", "type": "STRING"},
            {"name": "warehouse_id", "type": "STRING"},
            {"name": "quantity_change", "type": "INT64"},
            {"name": "reason", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"},
            {"name": "source_file", "type": "STRING"},
            {"name": "batch_id", "type": "STRING"},
            {"name": "load_timestamp", "type": "TIMESTAMP"},
        ]
    },
    "user_activity": {
        "fields": [
            {"name": "user_activity_id", "type": "STRING"},
            {"name": "user_id", "type": "STRING"},
            {"name": "activity_type", "type": "STRING"},
            {"name": "ip_address", "type": "STRING"},
            {"name": "user_agent", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"},
            {"name": "session_id", "type": "STRING"},
            {"name": "platform", "type": "STRING"},
            {"name": "source_file", "type": "STRING"},
            {"name": "batch_id", "type": "STRING"},
            {"name": "load_timestamp", "type": "TIMESTAMP"},
        ]
    }
}