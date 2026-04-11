from bronze.ingest_bronze import load_raw_files
from silver.flatten_json import flatten_all_matches
from gold.load_dimensions import load_dimensions
from gold.load_facts import load_facts

def run():
    load_raw_files()
    flatten_all_matches()
    load_dimensions()
    load_facts()

if __name__ == "__main__":
    run()
