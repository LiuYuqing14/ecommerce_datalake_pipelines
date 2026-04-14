#!/bin/bash
# Expand bronze samples to include consecutive dates for multi-day E2E testing
# This script duplicates existing 2025-10-01 partitions to create 2024-01-01, 2024-01-02, 2024-01-03

set -e

echo "Expanding bronze samples for multi-day testing..."
echo ""

# Tables with ingest_dt partitions
TABLES_WITH_INGEST_DT="orders order_items cart_items shopping_carts return_items returns"

# Create consecutive dates: 2024-01-01, 2024-01-02, 2024-01-03
for day in 01 02 03; do
  echo "Creating partitions for 2024-01-$day..."

  for table in $TABLES_WITH_INGEST_DT; do
    SOURCE_DIR="samples/bronze/$table/ingest_dt=2025-10-01"
    TARGET_DIR="samples/bronze/$table/ingest_dt=2024-01-$day"

    if [ -d "$SOURCE_DIR" ]; then
      echo "  - $table"
      cp -r "$SOURCE_DIR" "$TARGET_DIR"
    else
      echo "  - Skipping $table (source not found)"
    fi
  done
  echo ""
done

# Handle product_catalog (uses date= instead of ingest_dt=)
echo "Handling product_catalog partitions..."
for day in 01 02 03; do
  SOURCE_DIR="samples/bronze/product_catalog/date=2025-10-01"
  TARGET_DIR="samples/bronze/product_catalog/date=2024-01-$day"

  if [ -d "$SOURCE_DIR" ]; then
    echo "  - Creating date=2024-01-$day"
    cp -r "$SOURCE_DIR" "$TARGET_DIR"
  fi
done
echo ""

# Customers table has no partitions, just reference data - no action needed
echo "✓ Customers table (no partitions, using existing reference data)"
echo ""

# Re-zip
echo "Re-zipping bronze_samples.zip..."
cd samples
rm -f bronze_samples.zip
zip -r bronze_samples.zip bronze/ -q
cd ..

# Check size
SIZE=$(du -h samples/bronze_samples.zip | cut -f1)
echo ""
echo "✓ Done! New bronze_samples.zip size: $SIZE"
echo ""
echo "Sample data now includes:"
echo "  - 2024-01-01 (Day 1)"
echo "  - 2024-01-02 (Day 2)"
echo "  - 2024-01-03 (Day 3)"
echo "  - Plus original dates: 2020-03-01, 2023-01-01, 2025-10-01"
echo ""
echo "Next: Update .github/workflows/pipeline-e2e.yml to use 2024-01-01/02/03"
