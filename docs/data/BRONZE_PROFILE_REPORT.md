# Bronze Sample Profile Report

Generated from local parquet samples in `samples/bronze/`.

## Sample Scope

- **Date range**: 2020-01-01..2020-01-05

## Overview

- **Tables sampled**: 8
- **Partitions sampled**: 10
- **Total sample rows**: 152,029

### Per-Table Summary

| Table | Partitions | Sample Rows |
| --- | --- | --- |
| cart_items | 5 | 102,973 |
| customers | 5 | 353 |
| order_items | 5 | 16,079 |
| orders | 5 | 3,353 |
| product_catalog | 5 | 3,000 |
| return_items | 4 | 163 |
| returns | 4 | 39 |
| shopping_carts | 5 | 26,069 |

### Data Quality Flags

- ⚠️ **return_items.return_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=2, samples=return_items:2020-01-02, return_items:2020-01-03)
- ⚠️ **return_items.order_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=2, samples=return_items:2020-01-02, return_items:2020-01-03)
- ⚠️ **returns.return_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=2, samples=returns:2020-01-02, returns:2020-01-03)
- ⚠️ **returns.order_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=2, samples=returns:2020-01-02, returns:2020-01-03)
- ⚠️ **returns.customer_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=2, samples=returns:2020-01-02, returns:2020-01-03)
- 📊 **returns** partition `2020-01-05`: 16 rows (+64% above average) (count=1, samples=returns:2020-01-05)

## Schema Drift

- ✅ No schema drift detected across sampled partitions.

_Note: Partitions with all-null values for a column may show `Null` type instead of the actual type. These are treated as compatible schemas, not drift._

## Partition Coverage

Shows which partitions were sampled per table (for temporal schema drift detection).

**cart_items** (5 partitions):
- `2020-01`: 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05

**customers** (5 partitions):
- `2020-01`: 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05

**order_items** (5 partitions):
- `2020-01`: 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05

**orders** (5 partitions):
- `2020-01`: 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05

**product_catalog** (5 partitions):
- `non-date`: category=Books, category=Clothing, category=Electronics, category=Home, category=Toys

**return_items** (4 partitions):
- `2020-01`: 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05

**returns** (4 partitions):
- `2020-01`: 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05

**shopping_carts** (5 partitions):
- `2020-01`: 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05

## Canonical Schema Keys

| Table | Canonical Schema Key | Sample Partitions |
| --- | --- | --- |
| cart_items | `cart_item_id:Int64|cart_id:String|product_id:Int64|product_name:String|category:String|added_at:String|quantity:Int64|unit_price:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05 |
| customers | `customer_id:String|first_name:String|last_name:String|email:String|phone_number:String|signup_date:String|gender:String|age:Float64|is_guest:Boolean|customer_status:String|signup_channel:String|loyalty_tier:String|initial_loyalty_tier:String|email_verified:Boolean|marketing_opt_in:Boolean|mailing_address:String|billing_address:String|loyalty_enrollment_date:String|clv_bucket:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05 |
| order_items | `order_id:String|product_id:Int64|product_name:String|category:String|quantity:Int64|unit_price:Float64|discount_amount:Float64|cost_price:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05 |
| orders | `order_id:String|total_items:Int64|order_date:String|customer_id:String|email:String|order_channel:String|is_expedited:Boolean|customer_tier:String|gross_total:Float64|net_total:Float64|total_discount_amount:Float64|payment_method:String|shipping_speed:String|shipping_cost:Float64|agent_id:String|actual_shipping_cost:Float64|payment_processing_fee:Float64|shipping_address:String|billing_address:String|clv_bucket:String|is_reactivated:Boolean|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05 |
| product_catalog | `product_id:Int64|product_name:String|category:String|unit_price:Float64|cost_price:Float64|inventory_quantity:Int64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | Books, Clothing, Electronics, Home, Toys |
| return_items | `return_item_id:Int64|return_id:String|order_id:String|product_id:Int64|product_name:String|category:String|quantity_returned:Int64|unit_price:Float64|cost_price:Float64|refunded_amount:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05 |
| returns | `return_id:String|order_id:String|customer_id:String|email:String|return_date:String|reason:String|return_type:String|refunded_amount:Float64|return_channel:String|agent_id:String|refund_method:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05 |
| shopping_carts | `cart_id:String|customer_id:String|created_at:String|updated_at:String|cart_total:Float64|status:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05 |

## Column Statistics (Sample Partition)

Showing detailed stats for one representative partition per table.

### cart_items

**Sample**: `ingest_dt=2020-01-01` (19,821 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| cart_item_id | Int64 | 0.0% | 19,821 | Range: `79` to `709745`<br>p25=180088.0, p50=350733.0, p75=530500.0, p95=673046.0 |
| cart_id | String | 0.0% | 4,376 | Top: `CART-10417354` (15), `CART-10418311` (15), `CART-10461508` (15) |
| product_id | Int64 | 0.0% | 2,998 | Range: `1` to `3000`<br>p25=734.0, p50=1497.0, p75=2263.0, p95=2856.0 |
| product_name | String | 0.0% | 642 | Top: `Elegant Table` (370), `Cozy Lamp` (336), `Elegant Chair` (297) |
| category | String | 0.0% | 5 | Top: `Home` (4710), `Electronics` (3961), `Books` (3945) |
| added_at | String | 0.0% | 19,821 | Top: `2020-01-01T13:11:41.518678` (1), `2020-01-01T13:12:59.518678` (1), `2020-01-01T13:16:56.518678` (1) |
| quantity | Int64 | 0.0% | 6 | Range: `1` to `6`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 2,830 | Range: `5.02` to `249.86`<br>p25=64.35, p50=125.93, p75=188.18, p95=236.82 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (19821) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:38+00:00` (19821) |
| event_id | String | 0.0% | 19,821 | Top: `evt_11e0a256a3e93a1358475df0da29683c5369270b2ab195cf0e30649da0361fff` (1), `evt_2e56407d9c3fa10e073de74a6c28f4d7c75a145a1b98a0097edeb95d039a8bd5` (1), `evt_d4926c937dc8d9eac6ec3e7b98a6cb86c6013878773196287b917676f2822318` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/cart_items/ingest_dt=2020-01-01/part-0000.parquet` (19821) |

### customers

**Sample**: `signup_date=2020-01-01` (68 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| customer_id | String | 0.0% | 68 | Top: `CUST-9047` (1), `CUST-11164` (1), `CUST-11420` (1) |
| first_name | String | 0.0% | 59 | Top: `Lisa` (3), `Christopher` (2), `Charles` (2) |
| last_name | String | 0.0% | 61 | Top: `Smith` (3), `Moore` (3), `Johnson` (3) |
| email | String | 0.0% | 68 | Top: `tyler.harvey@gmail.com` (1), `brittany.jenkins@gmail.com` (1), `lisa.thomas@hotmail.com` (1) |
| phone_number | String | 0.0% | 68 | Top: `4627873451` (1), `001-498-498-5756` (1), `832.575.8251x4341` (1) |
| signup_date | String | 0.0% | 1 | Top: `2020-01-01` (68) |
| gender | String | 0.0% | 3 | Top: `Unknown` (26), `Male` (23), `Female` (19) |
| age | Float64 | 0.0% | 37 | Range: `18.0` to `70.0`<br>p25=29.0, p50=46.0, p75=60.0, p95=68.0 |
| is_guest | Boolean | 0.0% | 1 | — |
| customer_status | String | 0.0% | 3 | Top: `Active` (42), `Inactive` (15), `Dormant` (11) |
| signup_channel | String | 0.0% | 4 | Top: `Website` (38), `email` (14), `Phone` (8) |
| loyalty_tier | String | 5.88% | 5 | Top: `Bronze` (26), `Gold` (17), `Silver` (11) |
| initial_loyalty_tier | String | 5.88% | 5 | Top: `Bronze` (26), `Gold` (17), `Silver` (11) |
| email_verified | Boolean | 0.0% | 2 | — |
| marketing_opt_in | Boolean | 0.0% | 2 | — |
| mailing_address | String | 0.0% | 68 | Top: `33704 French Canyon, Lake Whitney, MT 02607` (1), `Unit 0236 Box 9238, DPO AA 77636` (1), `3529 Sherry Junction Suite 695, Michelleburgh, MH 40558` (1) |
| billing_address | String | 0.0% | 68 | Top: `33704 French Canyon, Lake Whitney, MT 02607` (1), `Unit 0236 Box 9238, DPO AA 77636` (1), `3529 Sherry Junction Suite 695, Michelleburgh, MH 40558` (1) |
| loyalty_enrollment_date | String | 5.88% | 63 | Top: `None` (4), `2023-11-08` (2), `2024-12-18` (2) |
| clv_bucket | String | 0.0% | 3 | Top: `Low` (30), `High` (27), `Medium` (11) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (68) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:28+00:00` (68) |
| event_id | String | 0.0% | 68 | Top: `evt_d31d1c2c62f307576b26c5a82fb0c699f6d4dd954ee723b93b9ff9eb2a23bd06` (1), `evt_3cace5b260660bd74f706ff96a84e910de9121cd855ba4ebef01d686f90edce6` (1), `evt_8fabf948fddfe3f94725ae8626da497ef0515f064c3b9604e9e961bdf87b8859` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/customers/signup_date=2020-01-01/part-0000.parquet` (68) |

### order_items

**Sample**: `ingest_dt=2020-01-01` (3,156 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| order_id | String | 0.0% | 652 | Top: `ORD-01337420` (15), `ORD-01337427` (15), `ORD-01337518` (15) |
| product_id | Int64 | 0.0% | 1,944 | Range: `3` to `3000`<br>p25=756.0, p50=1511.0, p75=2253.0, p95=2855.0 |
| product_name | String | 0.0% | 353 | Top: `Elegant Table` (63), `Modern Table` (47), `Modern Lamp` (46) |
| category | String | 0.0% | 5 | Top: `Home` (754), `Electronics` (615), `Books` (601) |
| quantity | Int64 | 0.0% | 6 | Range: `1` to `6`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 1,873 | Range: `5.02` to `249.86`<br>p25=64.35, p50=126.99, p75=186.92, p95=237.62 |
| discount_amount | Float64 | 0.0% | 592 | Range: `0.0` to `295.49`<br>p25=0.0, p50=0.0, p75=0.0, p95=56.66 |
| cost_price | Float64 | 0.0% | 1,835 | Range: `2.22` to `169.86`<br>p25=34.64, p50=68.58, p75=101.74, p95=141.91 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (3156) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:50+00:00` (3156) |
| event_id | String | 0.0% | 3,156 | Top: `evt_e624b098d1e73ed34c2f0ab533dd147fec1a11eb94c4c7d6d7d5dbd52c2c098a` (1), `evt_77e32bf8b876513559665abeb6b03f4bdeaa9ee470fa547e8dd8bbb11d4f3b13` (1), `evt_689c0e3f75d3a533f7873d56d44e33a20ff8f54f6c3bac6ba54a9e6030d701e7` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/order_items/ingest_dt=2020-01-01/part-0000.parquet` (3156) |

### orders

**Sample**: `ingest_dt=2020-01-01` (652 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| order_id | String | 0.0% | 652 | Top: `ORD-01336885` (1), `ORD-01336886` (1), `ORD-01336887` (1) |
| total_items | Int64 | 0.0% | 15 | Range: `1` to `22`<br>p25=4.0, p50=7.0, p75=9.0, p95=13.0 |
| order_date | String | 0.0% | 652 | Top: `2020-01-01 00:01:00.157383` (1), `2020-01-01 00:03:39.862704` (1), `2020-01-01 00:10:23.438977` (1) |
| customer_id | String | 0.0% | 652 | Top: `CUST-142659` (1), `GUEST-118771` (1), `CUST-90816` (1) |
| email | String | 0.0% | 652 | Top: `donna.morgan@hotmail.com` (1), `riosjohn@example.org` (1), `morgan.mccarty@yahoo.com` (1) |
| order_channel | String | 0.0% | 5 | Top: `Web` (297), `Phone` (119), `Social Media` (109) |
| is_expedited | Boolean | 0.0% | 2 | — |
| customer_tier | String | 0.0% | 4 | Top: `Platinum` (338), `Gold` (183), `Silver` (110) |
| gross_total | Float64 | 0.0% | 652 | Range: `5.11` to `8132.44`<br>p25=577.46, p50=1036.35, p75=1755.18, p95=4165.6 |
| net_total | Float64 | 0.0% | 650 | Range: `5.11` to `7801.96`<br>p25=570.82, p50=1017.05, p75=1727.78, p95=4099.3 |
| total_discount_amount | Float64 | 0.0% | 387 | Range: `0.0` to `433.39`<br>p25=0.0, p50=11.98, p75=55.94, p95=171.39 |
| payment_method | String | 0.0% | 28 | Top: `Credit Card` (336), `PayPal` (111), `Apple Pay` (70) |
| shipping_speed | String | 0.0% | 21 | Top: `Standard` (373), `Two-Day` (109), `Overnight` (83) |
| shipping_cost | Float64 | 0.0% | 3 | Range: `5.0` to `80.0`<br>p25=5.0, p50=5.0, p75=45.0, p95=80.0 |
| agent_id | String | 5.06% | 22 | Top: `ONLINE` (508), `None` (33), `CSR-0005` (8) |
| actual_shipping_cost | Float64 | 0.0% | 322 | Range: `3.83` to `76.86`<br>p25=4.23, p50=4.69, p75=36.77, p95=70.66 |
| payment_processing_fee | Float64 | 0.0% | 619 | Range: `0.1` to `183.97`<br>p25=12.56, p50=24.04, p75=42.81, p95=99.34 |
| shipping_address | String | 0.0% | 652 | Top: `13834 Connie Pass, South Troytown, KY 36821` (1), `4453 Alexander Creek, Contrerasview, WY 96279` (1), `68537 Johnson Run Suite 591, Bakertown, NJ 40065` (1) |
| billing_address | String | 0.0% | 652 | Top: `13834 Connie Pass, South Troytown, KY 36821` (1), `4453 Alexander Creek, Contrerasview, WY 96279` (1), `68537 Johnson Run Suite 591, Bakertown, NJ 40065` (1) |
| clv_bucket | String | 0.0% | 23 | Top: `High` (378), `Medium` (144), `Low` (48) |
| is_reactivated | Boolean | 0.0% | 1 | — |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (652) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:50+00:00` (652) |
| event_id | String | 0.0% | 652 | Top: `evt_070dff9749542e5cfa52fe766f3662808290815fcd6fdd56632a3f6ec35b597b` (1), `evt_1b260051fd929374f486658bd1881299ef89b68bbe397e652a38ad3918d9c0ad` (1), `evt_618bd82cb464be4be52a56cdbc9923640560b89e9d1e7906ae5591174844ff8d` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/orders/ingest_dt=2020-01-01/part-0000.parquet` (652) |

### product_catalog

**Sample**: `category=Books` (598 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| product_id | Int64 | 0.0% | 598 | Range: `8` to `2998`<br>p25=661.0, p50=1402.0, p75=2156.0, p95=2868.0 |
| product_name | String | 0.0% | 16 | Top: `Modern Guide` (45), `Modern Memoir` (45), `Illustrated Memoir` (45) |
| category | String | 0.0% | 1 | Top: `Books` (598) |
| unit_price | Float64 | 0.0% | 591 | Range: `5.36` to `249.61`<br>p25=65.0, p50=127.78, p75=190.36, p95=238.1 |
| cost_price | Float64 | 0.0% | 583 | Range: `2.46` to `164.64`<br>p25=34.59, p50=68.15, p75=101.18, p95=143.34 |
| inventory_quantity | Int64 | 0.0% | 147 | Range: `100` to `250`<br>p25=135.0, p50=175.0, p75=212.0, p95=243.0 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (598) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:34+00:00` (598) |
| event_id | String | 0.0% | 598 | Top: `evt_38bd8d51c5f4ad7c74f65ab8dfcf4d49ec54765ec7c05ecef3dae6b3b212a590` (1), `evt_62220d5155bb967bebe9dcbf358d96fee0fdc9ac7efc3429ab0e17506bdaf048` (1), `evt_e237c63d597aa9e870d2b749432e384ad8e93526f2e1d53aaa0053792a87e9ce` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/product_catalog/category=Books/part-0000.parquet` (598) |

### return_items

**Sample**: `ingest_dt=2020-01-02` (22 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_item_id | Int64 | 0.0% | 22 | Range: `40` to `588`<br>p25=189.0, p50=451.0, p75=583.0, p95=587.0 |
| return_id | String | 0.0% | 4 | Top: `RTN-00365422` (9), `RTN-00365390` (6), `RTN-00365317` (4) |
| order_id | String | 0.0% | 4 | Top: `ORD-01337503` (9), `ORD-01337362` (6), `ORD-01337059` (4) |
| product_id | Int64 | 0.0% | 22 | Range: `111` to `2928`<br>p25=434.0, p50=1908.0, p75=2531.0, p95=2822.0 |
| product_name | String | 0.0% | 22 | Top: `Colorful Blocks` (1), `  Durable Jacket  ` (1), `Interactive Car` (1) |
| category | String | 0.0% | 5 | Top: `Toys` (7), `Clothing` (5), `Electronics` (4) |
| quantity_returned | Int64 | 0.0% | 4 | Range: `1` to `4`<br>p25=1.0, p50=2.0, p75=3.0, p95=4.0 |
| unit_price | Float64 | 0.0% | 22 | Range: `14.36` to `246.41`<br>p25=61.55, p50=145.29, p75=183.45, p95=224.66 |
| cost_price | Float64 | 0.0% | 22 | Range: `5.87` to `152.84`<br>p25=33.79, p50=72.46, p75=102.17, p95=149.7 |
| refunded_amount | Float64 | 0.0% | 22 | Range: `27.28` to `985.64`<br>p25=96.88, p50=177.45, p75=318.75, p95=874.4 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (22) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:52+00:00` (22) |
| event_id | String | 0.0% | 22 | Top: `evt_28085a1b8243701fde2cea555dc3779a53c7474481f318930ca15f15bbacb5ef` (1), `evt_5e294c2e13848070d5d7b7186611d5bba460e6652c7c9f0871f5110021a913bf` (1), `evt_2d2281a2cf6152be16824edceb4da725b7c79ba478596c872ea5ca12ba8428ce` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/return_items/ingest_dt=2020-01-02/part-0000.parquet` (22) |

### returns

**Sample**: `ingest_dt=2020-01-02` (4 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_id | String | 0.0% | 4 | Top: `RTN-00365279` (1), `RTN-00365317` (1), `RTN-00365390` (1) |
| order_id | String | 0.0% | 4 | Top: `ORD-01336921` (1), `ORD-01337059` (1), `ORD-01337362` (1) |
| customer_id | String | 0.0% | 4 | Top: `CUST-60772` (1), `GUEST-186246` (1), `CUST-162336` (1) |
| email | String | 0.0% | 4 | Top: `heather.mitchell@gmail.com` (1), `cynthiabryant@example.org` (1), `kristin.murray@hotmail.com` (1) |
| return_date | String | 0.0% | 1 | Top: `2020-01-02` (4) |
| reason | String | 0.0% | 3 | Top: `Wrong item` (2), `Product did not match description` (1), `Damaged in transit` (1) |
| return_type | String | 0.0% | 2 | Top: `  Refund  ` (2), `Refund` (2) |
| refunded_amount | Float64 | 0.0% | 4 | Range: `680.91` to `3556.01`<br>p25=833.76, p50=865.86, p75=865.86, p95=3556.01 |
| return_channel | String | 0.0% | 4 | Top: `Phone` (1), `Social Media` (1), `  Ebay ` (1) |
| agent_id | String | 0.0% | 2 | Top: `ONLINE` (3), `CSR-0019` (1) |
| refund_method | String | 0.0% | 2 | Top: `Credit Card` (2), `PayPal` (2) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (4) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:52+00:00` (4) |
| event_id | String | 0.0% | 4 | Top: `evt_8563513393f58b97ecd65e8fa9333ee14e4289d1b2fe7b52d22de543e494184e` (1), `evt_b6edd01464560db8c51e415f42518a4c3e7f21e31054b3e1a02c6a992c67c614` (1), `evt_e45d3901fcf8bda4edb9af814cc0cf97ed0d8b4d16903572b51a478000456440` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/returns/ingest_dt=2020-01-02/part-0000.parquet` (4) |

### shopping_carts

**Sample**: `ingest_dt=2020-01-01` (5,053 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| cart_id | String | 0.0% | 5,053 | Top: `CART-10374276` (1), `CART-10374305` (1), `CART-10374346` (1) |
| customer_id | String | 0.0% | 5,053 | Top: `CUST-111247` (1), `GUEST-109600` (1), `GUEST-127425` (1) |
| created_at | String | 0.0% | 5,053 | Top: `2020-01-01T13:09:07.518678` (1), `2020-01-01T13:50:11.546285` (1), `2020-01-01T09:51:50.037233` (1) |
| updated_at | String | 3.86% | 4,859 | Top: `None` (195), `2020-01-01T13:20:24.518678` (1), `2020-01-01T10:14:38.037233` (1) |
| cart_total | Float64 | 0.0% | 4,348 | Range: `0.0` to `11021.29`<br>p25=345.69, p50=922.66, p75=1618.05, p95=3342.39 |
| status | String | 0.0% | 34 | Top: `abandoned` (3241), `converted` (576), `emptied` (573) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (5053) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:35+00:00` (5053) |
| event_id | String | 0.0% | 5,053 | Top: `evt_2c75c665c9b09dbaf4ba6b61da836d2d4fe2b26a6bf249cfe886f309c9c0349c` (1), `evt_4295e6d0c02e3b9ce3eaf31030ab41269c43e7ec1331de07c425a33f1c5d82ba` (1), `evt_eff45bca5379f09f49920208722cc3bc1403925b56b34fc086fa0f6d145eb8f2` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/shopping_carts/ingest_dt=2020-01-01/part-0000.parquet` (5053) |
<!-- GENERATED META -->
Last updated (UTC): 2026-01-26T05:39:19Z
Content hash (SHA-256): e7add49007139f4263a494a6b6917cc59f02aaf15a6b3c4f5eaf7a91ee11e3a5
<!-- END GENERATED META -->
