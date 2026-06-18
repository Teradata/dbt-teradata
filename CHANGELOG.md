## dbt-teradata 1.10.3a

### Features
- Open Table Format (OTF) support via Teradata DATALAKE objects — create and read Iceberg/Delta Lake tables (External OTF / JOTF) with `table` and `incremental` (`append`) materializations. See the "Open Table Format (OTF) support" section in the README for configuration, the supported/unsupported feature matrix, and limitations.

### Under the hood
- README updated with a restructured OTF section: External JOTF vs Managed OTF clarification, "what is / isn't supported" lists, consolidated incremental documentation, and limitations.
