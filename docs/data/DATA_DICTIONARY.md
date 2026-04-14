# Data Dictionary

Derived from the Bronze profile report and Data Contract.

## cart_items

| Column | Bronze Type | Base Silver Type | Required | Description |
| --- | --- | --- | --- | --- |
| cart_item_id | int64 | int64 | Yes | Unique cart item identifier. |
| cart_id | string | string | Yes | Unique cart identifier. |
| product_id | int64 | int64 | Yes | Unique product identifier. |
| product_name | string | string | No | Product display name. |
| category | string | string | No | Product category. |
| added_at | string | timestamp | No | Timestamp when the item was added. |
| quantity | int64 | int64 | Yes | Quantity of items in the line. |
| unit_price | float64 | float64 | Yes | Unit sale price. |
| batch_id | string | string | No | Batch identifier for ingestion run. |
| ingestion_ts | string | timestamp | No | Ingestion timestamp for the record. |
| event_id | string | string | No | Unique event identifier for lineage. |
| source_file | string | string | No | Source file path in storage. |

## customers

| Column | Bronze Type | Base Silver Type | Required | Description |
| --- | --- | --- | --- | --- |
| customer_id | string | string | Yes | Unique customer identifier. |
| first_name | string | string | No |  |
| last_name | string | string | No |  |
| email | string | string | Yes | Customer email address. |
| phone_number | string | string | No | Customer phone number. |
| signup_date | string | date | Yes | Date when the customer signed up. |
| gender | string | string | No | Customer gender. |
| age | float64 | float64 | No | Customer age. |
| is_guest | boolean | bool | No | Whether the customer is a guest checkout. |
| customer_status | string | string | No | Customer lifecycle status. |
| signup_channel | string | string | No | Acquisition channel for the customer. |
| loyalty_tier | string | string | No | Customer loyalty tier. |
| initial_loyalty_tier | string | string | No | Initial loyalty tier at signup. |
| email_verified | boolean | bool | No | Whether the email is verified. |
| marketing_opt_in | boolean | bool | No | Whether the customer opted into marketing. |
| mailing_address | string | string | No |  |
| billing_address | string | string | No | Billing address text. |
| loyalty_enrollment_date | string | date | No | Date customer enrolled in loyalty program. |
| clv_bucket | string | string | No | Customer lifetime value bucket. |
| batch_id | string | string | No | Batch identifier for ingestion run. |
| ingestion_ts | string | timestamp | No | Ingestion timestamp for the record. |
| event_id | string | string | No | Unique event identifier for lineage. |
| source_file | string | string | No | Source file path in storage. |

## order_items

| Column | Bronze Type | Base Silver Type | Required | Description |
| --- | --- | --- | --- | --- |
| order_id | string | string | Yes | Unique order identifier. |
| product_id | int64 | int64 | Yes | Unique product identifier. |
| product_name | string | string | No | Product display name. |
| category | string | string | No | Product category. |
| quantity | int64 | int64 | Yes | Quantity of items in the line. |
| unit_price | float64 | float64 | Yes | Unit sale price. |
| discount_amount | float64 | float64 | No | Discount amount applied. |
| cost_price | float64 | float64 | No | Unit cost for the item. |
| batch_id | string | string | No | Batch identifier for ingestion run. |
| ingestion_ts | string | timestamp | No | Ingestion timestamp for the record. |
| event_id | string | string | No | Unique event identifier for lineage. |
| source_file | string | string | No | Source file path in storage. |

## orders

| Column | Bronze Type | Base Silver Type | Required | Description |
| --- | --- | --- | --- | --- |
| order_id | string | string | Yes | Unique order identifier. |
| total_items | int64 | int64 | No | Count of items in the order. |
| order_date | string | timestamp | Yes | Timestamp when the order was placed. |
| customer_id | string | string | Yes | Unique customer identifier. |
| email | string | string | No | Customer email address. |
| order_channel | string | string | No | Channel used to place the order (e.g., Web, Phone). |
| is_expedited | boolean | bool | No | Whether expedited shipping was selected. |
| customer_tier | string | string | No |  |
| gross_total | float64 | float64 | Yes | Gross order total before discounts/fees. |
| net_total | float64 | float64 | Yes | Net order total after discounts. |
| total_discount_amount | float64 | float64 | No | Total discount applied to the order. |
| payment_method | string | string | No | Payment method used for the order. |
| shipping_speed | string | string | No | Selected shipping speed. |
| shipping_cost | float64 | float64 | No | Shipping cost charged to customer. |
| agent_id | string | string | No | Sales or support agent identifier. |
| actual_shipping_cost | float64 | float64 | No | Actual shipping cost incurred. |
| payment_processing_fee | float64 | float64 | No | Fee charged by payment processor. |
| shipping_address | string | string | No | Shipping address text. |
| billing_address | string | string | No | Billing address text. |
| clv_bucket | string | string | No | Customer lifetime value bucket. |
| is_reactivated | boolean | bool | No | Whether the customer was reactivated. |
| batch_id | string | string | No | Batch identifier for ingestion run. |
| ingestion_ts | string | timestamp | No | Ingestion timestamp for the record. |
| event_id | string | string | No | Unique event identifier for lineage. |
| source_file | string | string | No | Source file path in storage. |

## product_catalog

| Column | Bronze Type | Base Silver Type | Required | Description |
| --- | --- | --- | --- | --- |
| product_id | int64 | int64 | Yes | Unique product identifier. |
| product_name | string | string | Yes | Product display name. |
| category | string | string | No | Product category. |
| unit_price | float64 | float64 | Yes | Unit sale price. |
| cost_price | float64 | float64 | No | Unit cost for the item. |
| inventory_quantity | int64 | int64 | No | Current inventory quantity. |
| batch_id | string | string | No | Batch identifier for ingestion run. |
| ingestion_ts | string | timestamp | No | Ingestion timestamp for the record. |
| event_id | string | string | No | Unique event identifier for lineage. |
| source_file | string | string | No | Source file path in storage. |

## return_items

| Column | Bronze Type | Base Silver Type | Required | Description |
| --- | --- | --- | --- | --- |
| return_item_id | int64 | int64 | Yes |  |
| return_id | string | string | Yes | Unique return identifier. |
| order_id | string | string | Yes | Unique order identifier. |
| product_id | int64 | int64 | Yes | Unique product identifier. |
| product_name | string | string | No | Product display name. |
| category | string | string | No | Product category. |
| quantity_returned | int64 | int64 | Yes | Quantity of items returned. |
| unit_price | float64 | float64 | No | Unit sale price. |
| cost_price | float64 | float64 | No | Unit cost for the item. |
| refunded_amount | float64 | float64 | No | Amount refunded to the customer. |
| batch_id | string | string | No | Batch identifier for ingestion run. |
| ingestion_ts | string | timestamp | No | Ingestion timestamp for the record. |
| event_id | string | string | No | Unique event identifier for lineage. |
| source_file | string | string | No | Source file path in storage. |

## returns

| Column | Bronze Type | Base Silver Type | Required | Description |
| --- | --- | --- | --- | --- |
| return_id | string | string | Yes | Unique return identifier. |
| order_id | string | string | Yes | Unique order identifier. |
| customer_id | string | string | Yes | Unique customer identifier. |
| email | string | string | No | Customer email address. |
| return_date | string | timestamp | Yes | Timestamp when the return was initiated. |
| reason | string | string | No | Return reason. |
| return_type | string | string | No | Type of return (e.g., refund, exchange). |
| refunded_amount | float64 | float64 | No | Amount refunded to the customer. |
| return_channel | string | string | No | Channel used to process return. |
| agent_id | string | string | No | Sales or support agent identifier. |
| refund_method | string | string | No | Refund method used. |
| batch_id | string | string | No | Batch identifier for ingestion run. |
| ingestion_ts | string | timestamp | No | Ingestion timestamp for the record. |
| event_id | string | string | No | Unique event identifier for lineage. |
| source_file | string | string | No | Source file path in storage. |

## shopping_carts

| Column | Bronze Type | Base Silver Type | Required | Description |
| --- | --- | --- | --- | --- |
| cart_id | string | string | Yes | Unique cart identifier. |
| customer_id | string | string | Yes | Unique customer identifier. |
| created_at | string | timestamp | Yes | Timestamp when the record was created. |
| updated_at | string | timestamp | No | Timestamp when the record was last updated. |
| cart_total | float64 | float64 | No | Total value of items in the cart. |
| status | string | string | No | Record status or state. |
| batch_id | string | string | No | Batch identifier for ingestion run. |
| ingestion_ts | string | timestamp | No | Ingestion timestamp for the record. |
| event_id | string | string | No | Unique event identifier for lineage. |
| source_file | string | string | No | Source file path in storage. |
<!-- GENERATED META -->
Last updated (UTC): 2026-01-24T20:59:59Z
Content hash (SHA-256): e430d2fbfd9f3dfa141a4825e6e94e70250b0d3eb46ae6d6a650d0a42c20258a
<!-- END GENERATED META -->
