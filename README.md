
Pub Sub Topic Name: backend-events-topic
Pub Sub Topic Subscription: backend-events-topic-sub
Note: All events pushed to one single topic


### Order Events Schema
```JSON
{
    "event_type": "order",
    "order_id": "uuid",
    "customer_id": "uuid",
    "order_date": "timestamp",
    "status": "enum(pending, processing, shipped, delivered)",
    "items": [
        {
            "product_id": "uuid",
            "product_name": "string",
            "quantity": "integer",
            "price": "float"
        }
    ],
    "shipping_address": {
        "street": "string",
        "city": "string",
        "country": "string"
    },
    "total_amount": "float"
}
```

### Inventory Events Schema
```JSON
{
    "event_type": "inventory",
    "inventory_id": "uuid",
    "product_id": "uuid",
    "warehouse_id": "uuid",
    "quantity_change": "integer(-100 to 100)",
    "reason": "enum(restock, sale, return, damage)",
    "timestamp": "timestamp"
}
```

### User Activity Events Schema
```JSON
{
    "event_type": "user_activity",
    "user_id": "uuid",
    "activity_type": "enum(login, logout, view_product, add_to_cart, remove_from_cart)",
    "ip_address": "string",
    "user_agent": "string",
    "timestamp": "timestamp",
    "metadata": {
        "session_id": "uuid",
        "platform": "enum(web, mobile, tablet)"
    }
}
```

### Data Modeling and Architecture
#### Design the BigQuery data model that will store these events.

#### Table structure and relationships
Tables:
- orders
- items
- shipping_address
- inventory
- user_activity


#### Partitioning and clustering strategies
Clustering works good on frequently filtered columns. That's why I choose e.g. customer_id in orders or user_id in user_activity.

Primary keys are unique and clustering by them is not an effective. Clustering has a sense, when values are repeating.


#### Tracking historical data and time travel.
BigQuery natively supports time travel and change tracking via snapshots. It allows users to query historical data from up to 7 days prior by specifying a timestamp in the FOR SYSTEM_TIME AS OF clause, enabling recovery and analysis of data as it existed at a specific point in time.

#### Deliverables:
Data model diagram in gymshark-schema.png

DDL statements for creating tables in ddl directory.

Explanation of design decisions.
