# Bronze Layer Data Dictionary

## Overview

The Bronze layer stores raw source data exactly as received with minimal transformation.

Purpose:

- preserve original source files  
- support reprocessing into Silver  
- maintain lineage and auditability  
- allow schema evolution handling  
- provide fallback raw truth source

The Bronze layer currently ingests:
match JSON files
people master CSV

# Proposed Enhancements (Backlog)

The following items are proposed for future hardening of the Bronze layer.

They may be implemented later based on scale, source growth, and operational needs.

---

## Metadata Enhancements

| Column | Meaning |
|---|---|
| batch_id | Unique pipeline batch identifier for each load run |
| file_hash | Hash of source file contents for duplicate detection |
| source_system | Source provider / feed name |
| source_folder | Landing folder path |
| source_file_size | File size in bytes |

---

## Audit Enhancements

| Column | Meaning |
|---|---|
| load_status | SUCCESS / FAILED |
| parse_status | Parsed successfully or rejected |
| rows_extracted | Count of extracted records |
| error_message | Failure reason if applicable |
| retry_count | Number of reprocessing attempts |

---

## Schema Governance

| Column | Meaning |
|---|---|
| schema_version | Detected source schema version |
| json_structure_hash | Structural fingerprint of JSON payload |

Purpose:

- detect upstream schema drift  
- support parser upgrades  
- isolate breaking source changes

---

## Data Quality Controls

Planned checks:

- duplicate match files  
- null raw_json payloads  
- malformed JSON records  
- missing match identifiers  
- unexpected row count drops  
- stale ingestion timestamps

---

## Operational Improvements

Potential future capabilities:

- incremental ingestion only for new files  
- automatic duplicate file rejection  
- failed file quarantine area  
- replay specific batch runs  
- ingestion run dashboard

---

## Note

These enhancements are backlog ideas and not necessarily active in the current implementation.
