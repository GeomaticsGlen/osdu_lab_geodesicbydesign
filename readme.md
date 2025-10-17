**OSDU Reference Data Ingestion Pipeline**

This is a starter project to provide the basic foundations for an OSDU platform implementation. 

This project also serves as a personal deep dive into modern software engineering practices — from service-oriented architecture and RESTful API design to cloud-native deployment patterns and automated validation pipelines. It’s a hands-on vehicle for sharpening my fluency with contemporary web frameworks, modular backend design, and scalable ingestion workflows.

At the same time, it builds on two decades of experience architecting and implementing cutting-edge E&P and Geoscience data platforms across some of the world’s largest energy companies. That background informs my focus on auditability, standards compliance, and operational resilience — all critical for delivering trusted data infrastructure in high-stakes environments.

This repository provides a robust ingestion pipeline for OSDU-compliant reference data manifests. It validates, transforms, and ingests over 500+ records into a Storage Service backend, ensuring schema alignment, auditability, and reproducibility.


**Key Features**

- Parses all manifest formats: `ReferenceData` wrappers, flat arrays, and single-record files
    
- Validates each record against its registered schema
    
- Supports dry-run mode for safe validation before ingestion
    
- Logs summary results and per-record errors for auditability

- Backed by PostgreSQL with native JSONB support for efficient storage, indexing, and querying of semi-structured OSDU records
    

Addendum:
v2. Ingestion of the full reference values FIXED, LOCAL and OPEN. Created the logic and regex to flatten/unflatten, resolve $ref etc.

v3. Basic GET logic to return flattened schemas and records so these can be viewed as tabular and list rather than RAW JSON. Added some example HTML.
    

**Contributing**

- Ensure all changes are standards-compliant and audit-ready
    
- Match real-world manifest structure and edge cases
    
- Pull requests welcome
    
Environment-specific configuration (e.g. database credentials, service endpoints) managed via `.env` files, which are excluded from version control via '`.gitignore`'

