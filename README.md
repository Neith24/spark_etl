# ETL Pipeline - Start Guide

## Build

```bash
docker compose build

Save to PostgreSQL
docker compose run etl python main.py \
  --source /opt/data/transaction.csv \
  --database warehouse \
  --table customers

Save as Parquet
docker compose run etl python main.py \
  --source /opt/data/transaction.csv \
  --destination /opt/data/output.parquet

From PostgreSQL
docker compose exec db psql --user postgres -d warehouse \
  -c 'SELECT * FROM customers LIMIT 10'

Run Unit Test 
docker compose run etl python -c "
import sys
sys.path.append('.')
from test.test_etl import test_longest_streak
test_longest_streak()
"
