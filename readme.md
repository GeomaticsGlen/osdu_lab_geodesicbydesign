**OSDU Reference Data Ingestion Pipeline**

This is a starter project to provide the basic foundations for an OSDU platform implementation. 

This project also serves as a personal deep dive into modern software engineering practices — from service-oriented architecture and RESTful API design to cloud-native deployment patterns and automated validation pipelines. It’s a hands-on vehicle for sharpening my fluency with contemporary web frameworks, modular backend design, and scalable ingestion workflows.

At the same time, it builds on two decades of experience architecting and implementing cutting-edge E&P and Geoscience data platforms across some of the world’s largest energy companies. That background informs my focus on auditability, standards compliance, and operational resilience — all critical for delivering trusted data infrastructure in high-stakes environments.

This repository provides a robust ingestion pipeline for OSDU-compliant reference data manifests. It validates, transforms, and ingests over 500+ records into a Storage Service backend, ensuring schema alignment, auditability, and reproducibility.

With the assistance of AI, I was able to accelerate the design and implementation of a fully automated ingestion and schema resolution pipeline tailored to the complexities of OSDU reference data. This includes a recursive engine for resolving `$ref` dependencies across abstract, reference, and master schemas, a Flask-based service layer for validation and registration, and a robust ingestion workflow capable of processing hundreds of manifests with audit-ready logging and standards compliance.

**Key Features**

- Parses all manifest formats: `ReferenceData` wrappers, flat arrays, and single-record files
    
- Validates each record against its registered schema
    
- Supports dry-run mode for safe validation before ingestion
    
- Logs summary results and per-record errors for auditability

- Backed by PostgreSQL with native JSONB support for efficient storage, indexing, and querying of semi-structured OSDU records
    

**Supported Manifest Structures**

- `{"records": [...]}`
    
- `{"ReferenceData": [...]}`
    
- `[record1, record2, ...]`
    
- `{single_record}`
    

**Usage**

- Set `DRY_RUN = True` to validate without ingesting
    
- Set `DRY_RUN = False` to ingest into the Storage Service
    
- Run `ingest_reference_values.py` to process all manifests listed in `IngestionSequence.json`
    

**Architecture**

- Modular Flask app with separate routes and services
    
- Schema validation handled via `validate_record()` in `services/schema_service.py`
    
- Ingestion sequence defined in `IngestionSequence.json`
    
- Summary logging to `ingestion_summary.log`
    

**Contributing**

- Ensure all changes are standards-compliant and audit-ready
    
- Match real-world manifest structure and edge cases
    
- Pull requests welcome
    
Environment-specific configuration (e.g. database credentials, service endpoints) managed via `.env` files, which are excluded from version control via '`.gitignore`'

