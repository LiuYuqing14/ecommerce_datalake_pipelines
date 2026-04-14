# Bronze Profile Spot Checks

Generated from local parquet samples in `samples/bronze/`.

## Returns and Return Items: ID Reuse

| Table | Column | Rows | Distinct | Top Repeats Across Partitions |
| --- | --- | --- | --- | --- |
| returns | return_id | 8,808 | 8,808 | — |
| returns | order_id | 8,808 | 8,624 | ORD-01422929(2), ORD-03067352(2), ORD-04582725(2), ORD-01428178(2), ORD-01424060(2) |
| returns | customer_id | 8,808 | 8,368 | CUST-71593(3), CUST-86748(3), CUST-104266(3), CUST-107167(3), CUST-23730(3) |
| return_items | return_item_id | 34,249 | 21,719 | 4627(4), 5258(4), 3692(4), 4353(4), 2772(4) |
| return_items | return_id | 34,249 | 8,629 | — |
| return_items | order_id | 34,249 | 8,576 | ORD-01425452(2), ORD-04591950(2), ORD-04585642(2), ORD-03034728(2), ORD-04583662(2) |
| return_items | product_id | 34,249 | 3,000 | 634(25), 2731(22), 196(21), 1849(20), 1627(20) |

## Returns Partition Coverage

| Table | Month | Expected Days | Observed Days | Missing Days |
| --- | --- | --- | --- | --- |
| returns | 2020-03 | 31 | 29 | 2020-03-01, 2020-03-31 |
| returns | 2023-01 | 31 | 30 | 2023-01-15 |
| returns | 2025-10 | 31 | 29 | 2025-10-01, 2025-10-31 |
| return_items | 2020-03 | 31 | 29 | 2020-03-01, 2020-03-31 |
| return_items | 2023-01 | 31 | 30 | 2023-01-15 |
| return_items | 2025-10 | 31 | 29 | 2025-10-01, 2025-10-31 |

## cart_items Spike Validation

- Partitions: 93; avg rows: 44146.55; median rows: 28694

| Partition | Rows | Files | Pct vs Avg |
| --- | --- | --- | --- |
| 2020-03-30 | 212,265 | 1 | 381% |
| 2025-10-30 | 211,617 | 1 | 379% |
| 2023-01-14 | 211,475 | 1 | 379% |
| 2020-03-29 | 127,789 | 1 | 189% |
| 2025-10-29 | 127,162 | 1 | 188% |
| 2023-01-13 | 127,029 | 1 | 188% |
| 2023-01-12 | 96,538 | 1 | 119% |
| 2025-10-28 | 96,122 | 1 | 118% |
| 2020-03-28 | 95,661 | 1 | 117% |
| 2020-03-27 | 77,978 | 1 | 77% |

### Cross-Table Alignment (Top Spikes)

| Partition | cart_items | shopping_carts | orders |
| --- | --- | --- | --- |
| 2020-03-30 | 212,265 | 46,389 | 6,113 |
| 2025-10-30 | 211,617 | 46,318 | 6,165 |
| 2023-01-14 | 211,475 | 46,354 | 6,115 |
| 2020-03-29 | 127,789 | 28,997 | 3,812 |
| 2025-10-29 | 127,162 | 28,852 | 3,723 |

### cart_items Distribution by cart_id (Top Spikes + Baselines)

| Partition | Carts | Items | Mean | Median | P90 | P95 | P99 | Max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2020-03-15 | 6266 | 28694 | 4.58 | 4 | 8 | 9 | 13 | 15 |
| 2020-03-29 | 25243 | 127789 | 5.06 | 5 | 9 | 10 | 14 | 15 |
| 2020-03-30 | 40410 | 212265 | 5.25 | 5 | 9 | 11 | 14 | 15 |
| 2020-03-31 | 4385 | 19709 | 4.49 | 4 | 8 | 8 | 13 | 15 |
| 2023-01-14 | 40452 | 211475 | 5.23 | 5 | 9 | 11 | 14 | 15 |
| 2025-10-29 | 25099 | 127162 | 5.07 | 5 | 9 | 10 | 14 | 15 |
| 2025-10-30 | 40451 | 211617 | 5.23 | 5 | 9 | 11 | 14 | 15 |

### cart_items Duplicate Checks (Top Spikes + Baselines)

| Partition | cart_item_id dupes | line_key dupes |
| --- | --- | --- |
| 2020-03-15 | 0 | 0 |
| 2020-03-29 | 0 | 0 |
| 2020-03-30 | 0 | 2 |
| 2020-03-31 | 0 | 0 |
| 2023-01-14 | 0 | 1 |
| 2025-10-29 | 0 | 0 |
| 2025-10-30 | 0 | 1 |
<!-- GENERATED META -->
Last updated (UTC): 2026-01-13T17:16:06Z
Content hash (SHA-256): e2546433be9a7524afdb3e9f0c748e3d3a7c67d54b134fbb16ddc13c72e600c8
Profile report hash (SHA-256): 7a45617d90ee8cdec879b2874db9896853bd5f3eaadf06fc0fb9490ffc019c56
<!-- END GENERATED META -->

---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
