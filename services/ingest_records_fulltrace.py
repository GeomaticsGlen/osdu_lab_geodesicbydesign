from flask import current_app, jsonify
from services.schema_service import validate_record_against_schema
from db import db  # Adjust this import to match your actual DB module
from models import Record  # Replace with your actual ORM model

def ingest_records(payload):
    inserted = []
    errors = []

    for record in payload:
        record_id = record.get("id")
        current_app.logger.info(f"🔍 Validating record: {record_id}")

        try:
            # Validate against schema
            validate_record_against_schema(record)
            current_app.logger.info(f"✅ Schema validation passed for {record_id}")
        except Exception as e:
            current_app.logger.error(f"❌ Schema validation failed for {record_id}: {e}")
            errors.append({"id": record_id, "error": str(e)})
            continue

        try:
            # Convert to ORM object (adjust fields to match your model)
            db_record = Record(
                id=record["id"],
                kind=record["kind"],
                legal=record["legal"],
                acl=record["acl"],
                data=record["data"],
                create_user="system",
                modify_user="system"
            )

            db.session.add(db_record)
            db.session.commit()
            current_app.logger.info(f"📝 Record inserted: {record_id}")
            inserted.append(record_id)

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"❌ DB insert failed for {record_id}: {e}")
            errors.append({"id": record_id, "error": str(e)})

    status_code = 201 if inserted else 400
    return jsonify({
        "inserted": inserted,
        "recordErrors": errors
    }), status_code
