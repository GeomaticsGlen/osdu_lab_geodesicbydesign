# ------------------------------------------------------------------------------
# app.py
#
# Purpose:
# Entry point for the OSDU Storage + Schema Service Flask application.
# - Creates the Flask app instance
# - Configures CORS
# - Loads environment variables (e.g. DB credentials from osdudb.env)
# - Registers blueprints (routes)
#
# This file should remain minimal — all business logic lives in services/,
# and all HTTP routes live in routes/.
# ------------------------------------------------------------------------------

from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv
import logging
from routes.records import records_bp
from routes.schema import schema_bp


def create_app():
    # Load environment variables from backend/osdudb.env
    load_dotenv("backend/osdudb.env")

    # Create Flask app
    app = Flask(__name__)
    CORS(app)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
    )

    # Register blueprints
    app.register_blueprint(records_bp)
    app.register_blueprint(schema_bp)

    # Log all registered routes
    for rule in app.url_map.iter_rules():
        print(f"[ROUTE] {rule.endpoint}: {rule.methods} -> {rule}")

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)
