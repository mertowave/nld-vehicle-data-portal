#!/usr/bin/env python3
"""CLI tool for downloading real RDW vehicle data with translated columns."""
from __future__ import annotations

import argparse
import csv
import json
import sys
from typing import Dict, List, Optional

import requests

import rdw_client as rdw


def show_columns() -> None:
    print("Available columns (Dutch -> English):")
    for source, target in sorted(rdw.COLUMN_TRANSLATIONS.items()):
        print(f"- {source} -> {target}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download real RDW data, translate column names to English, "
            "filter by category or license plate, and export to Excel or CSV."
        )
    )
    parser.add_argument("--category", help="Filter by Dutch vehicle category (voertuigsoort).")
    parser.add_argument("--license-plate", help="Lookup a single license plate (kenteken).")
    parser.add_argument("--limit", type=int, help="Maximum number of records to download.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=rdw.DEFAULT_PAGE_SIZE,
        help="Rows fetched per API request (default: %(default)s).",
    )
    parser.add_argument("--excel-path", help="Path to save results as an .xlsx file.")
    parser.add_argument("--csv-path", help="Path to save results as CSV for Excel/BI tools.")
    parser.add_argument("--show-columns", action="store_true", help="List available columns and exit.")
    parser.add_argument(
        "--app-token",
        help="Socrata app token. Defaults to RDW_APP_TOKEN environment variable.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout per request in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=5,
        help="Print the first N translated records to stdout (default: %(default)s). Use 0 to disable.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.show_columns:
        show_columns()
        return 0

    if not args.excel_path and not args.csv_path and args.preview == 0:
        print("No output selected. Enable --preview, --excel-path, or --csv-path.", file=sys.stderr)
        return 2

    filters = rdw.build_filters(args.category, args.license_plate)
    app_token = rdw.resolve_app_token(args.app_token)

    translated_for_excel: List[Dict[str, object]] = []
    csv_writer = None
    csv_handle = None
    if args.csv_path:
        csv_handle = open(args.csv_path, "w", newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(csv_handle, fieldnames=rdw.CSV_FIELDNAMES)
        csv_writer.writeheader()
    preview_count = 0
    total = 0

    try:
        raw_records = rdw.fetch_rdw_data(
            limit=args.limit,
            page_size=args.page_size,
            filters=filters,
            app_token=app_token,
            timeout=args.timeout,
        )

        for record in raw_records:
            translated = rdw.translate_record(record)
            total += 1

            if args.preview and preview_count < args.preview:
                print(json.dumps(translated, ensure_ascii=False, indent=2))
                preview_count += 1

            if args.excel_path:
                translated_for_excel.append(translated)

            if csv_writer is not None:
                csv_writer.writerow({field: translated.get(field, "") for field in rdw.CSV_FIELDNAMES})

    except requests.HTTPError as exc:
        print("HTTP error while fetching data:", exc, file=sys.stderr)
        if exc.response is not None and exc.response.status_code == 403:
            print("Hint: provide an app token via --app-token or RDW_APP_TOKEN env var.", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print("Request failed:", exc, file=sys.stderr)
        return 1
    finally:
        if csv_handle is not None:
            csv_handle.close()

    if args.excel_path and translated_for_excel:
        try:
            rdw.export_to_excel(translated_for_excel, args.excel_path)
            print(f"Excel export written to {args.excel_path}")
        except Exception as exc:  # pragma: no cover
            print("Excel export failed:", exc, file=sys.stderr)
            return 1

    if args.csv_path and csv_writer is not None:
        print(f"CSV export written to {args.csv_path}")

    print(f"Total records retrieved: {total}")
    if args.preview:
        print(f"Previewed records: {preview_count}")
    if args.limit is None and args.page_size == rdw.DEFAULT_PAGE_SIZE:
        print("Tip: use --limit to control dataset size or --page-size to tune download batches.")

    if total == 0:
        print("No records matched the filters.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
