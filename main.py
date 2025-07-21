import argparse
from etl.etl import run_etl

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, help="Source CSV path")
    parser.add_argument('--destination', help="Destination Parquet path")
    parser.add_argument('--database', help="Database name (e.g., warehouse)")
    parser.add_argument('--table', help="DB Table name (e.g., customers)")
    args = parser.parse_args()
    
    # Construct JDBC 
    db_url = None
    if args.database:
        db_url = f"jdbc:postgresql://db:5432/{args.database}"
    
    run_etl(args.source, args.destination, db_url, args.table)

if __name__ == "__main__":
    main()