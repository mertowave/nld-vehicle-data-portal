"""Flask web interface for browsing real RDW vehicle data."""
from __future__ import annotations

import csv
import io
import os
from typing import Dict, List, Optional

from flask import (Flask, Response, flash, jsonify, redirect, render_template,
                   request, stream_with_context, url_for)
import requests

import rdw_client as rdw

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

MAX_WEB_LIMIT = 10000

app = Flask(__name__)
app.secret_key = "change-me"  # replace in production
app.config["JSON_AS_ASCII"] = False


def parse_limit(raw: Optional[str]) -> int:
    try:
        value = int(raw) if raw else 50
    except ValueError:
        return 50
    return max(1, min(MAX_WEB_LIMIT, value))


def query_records(category: str, license_plate: str, limit: int, timeout: float, brand: str = "", model: str = "", date_from: str = "", date_to: str = "", order_by_recent: bool = False) -> Dict[str, object]:
    # Convert English category back to Dutch for API call
    dutch_category = None
    if category:
        category_map = rdw.get_category_translation_map()
        # Find Dutch category for English category
        for dutch, english in category_map.items():
            if english == category:
                dutch_category = dutch
                break
        if not dutch_category:
            dutch_category = category  # fallback to original if not found
    
    filters = rdw.build_filters(dutch_category, license_plate or None, brand or None, model or None, date_from or None, date_to or None, order_by_recent)
    app_token = rdw.resolve_app_token()
    translated: List[Dict[str, object]] = []
    total = 0

    try:
        for record in rdw.fetch_rdw_data(
            limit=limit,
            page_size=rdw.DEFAULT_PAGE_SIZE,
            filters=filters,
            app_token=app_token,
            timeout=timeout,
        ):
            translated.append(rdw.translate_record(record))
            total += 1
    except requests.HTTPError as exc:
        message = "RDW API access error."
        if exc.response is not None and exc.response.status_code == 403:
            message += " Please use a Socrata app token. Set it like this: export RDW_APP_TOKEN='your_token_here'"
        raise RuntimeError(message) from exc
    except requests.RequestException as exc:
        raise RuntimeError("RDW API request failed.") from exc

    return {"records": translated, "total": total}


def ensure_categories(timeout: float) -> List[str]:
    app_token = rdw.resolve_app_token()
    try:
        dutch_categories = rdw.fetch_categories(app_token=app_token, timeout=timeout)
        return rdw.translate_categories(dutch_categories)
    except requests.RequestException:
        return []


@app.route("/")
def index() -> str:
    category = request.args.get("category", "")
    license_plate = request.args.get("license_plate", "")
    brand = request.args.get("brand", "")
    model = request.args.get("model", "")
    limit = parse_limit(request.args.get("limit"))
    timeout = request.args.get("timeout", type=float) or 30.0
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")

    records: List[Dict[str, object]] = []
    total = 0
    error: Optional[str] = None
    searched = "submitted" in request.args
    total_plates = 0

    # If no search was performed, show recent records by default
    if not searched:
        try:
            # Get recent records without any filters, ordered by registration date
            result = query_records("", "", 20, timeout, "", "", "", "", True)  # Default 20 recent records
            records = result["records"]
            total = result["total"]
        except RuntimeError as exc:
            error = str(exc)
    else:
        # User performed a search with filters
        try:
            result = query_records(category, license_plate, limit, timeout, brand, model, date_from, date_to)
            records = result["records"]
            total = result["total"]
            if total == 0:
                flash("No records found matching the selected filters.", "warning")
        except RuntimeError as exc:
            error = str(exc)
            flash(error, "danger")

    try:
        categories = ensure_categories(timeout)
        # Get total plate count
        app_token = rdw.resolve_app_token()
        total_plates = rdw.get_total_plate_count(app_token, timeout)
        
        # Get available brands
        brands = rdw.get_available_brands(app_token, timeout)
        
        # Get models for selected brand
        models = []
        if brand:
            models = rdw.get_models_for_brand(brand, app_token, timeout)
    except RuntimeError as exc:
        if not error:  # Only set error if not already set from search
            error = str(exc)
        categories = []
        brands = []
        models = []

    return render_template(
        "index.html",
        category=category,
        categories=categories,
        license_plate=license_plate,
        brand=brand,
        brands=brands,
        model=model,
        models=models,
        limit=limit,
        records=records,
        total=total,
        error=error,
        searched=searched,
        column_map=rdw.TURKISH_COLUMN_TRANSLATIONS,
        max_limit=MAX_WEB_LIMIT,
        total_plates=total_plates,
    )


@app.route("/api/total-count")
def api_total_count() -> Response:
    """API endpoint to get total plate count."""
    try:
        app_token = rdw.resolve_app_token()
        total_plates = rdw.get_total_plate_count(app_token, 30.0)
        return jsonify({"total_plates": total_plates})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/download.csv")
def download_csv() -> Response:
    category = request.args.get("category", "")
    license_plate = request.args.get("license_plate", "")
    brand = request.args.get("brand", "")
    model = request.args.get("model", "")
    limit = parse_limit(request.args.get("limit"))
    timeout = request.args.get("timeout", type=float) or 30.0
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")

    try:
        result = query_records(category, license_plate, limit, timeout, brand, model, date_from, date_to)
    except RuntimeError as exc:
        return Response(str(exc), status=502, mimetype="text/plain")

    def generate() -> str:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rdw.CSV_FIELDNAMES)
        writer.writeheader()
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for row in result["records"]:
            writer.writerow({field: row.get(field, "") for field in rdw.CSV_FIELDNAMES})
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    filename_parts = ["rdw_data"]
    if category:
        filename_parts.append(category.lower().replace(" ", "_"))
    if license_plate:
        filename_parts.append(license_plate.replace(" ", "").replace("-", "").upper())
    filename = "-".join(filename_parts) + ".csv"

    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return Response(stream_with_context(generate()), mimetype="text/csv", headers=headers)


@app.errorhandler(404)
def not_found(_: Exception) -> Response:
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True, port=5001)
