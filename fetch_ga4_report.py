import os
import sys

import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

# You can name your service account JSON file 'credentials.json' in this folder
CREDENTIALS_FILE = "credentials.json"


def run_ga4_report(
    property_id: str, credentials_path: str = CREDENTIALS_FILE, output_csv: str = "ga4_report.csv"
):
    # Resolve relative path to current directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cred_full_path = os.path.join(script_dir, credentials_path)

    if not os.path.exists(cred_full_path):
        print(f"\n[Error] Credentials file not found at: {cred_full_path}")
        print(
            "Please place your Google Cloud Service Account JSON file in this folder and rename it to 'credentials.json'.\n"
        )
        return False

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_full_path

    try:
        client = BetaAnalyticsDataClient()

        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="sessionDefaultChannelGroup"),
                Dimension(name="country"),
                Dimension(name="pagePath"),
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews"),
                Metric(name="conversions"),
                Metric(name="totalRevenue"),
            ],
            date_ranges=[DateRange(start_date="30daysAgo", end_date="today")],
            limit=100000,
            offset=0,
        )

        print(f"Fetching report for GA4 Property: {property_id}...")
        response = client.run_report(request)

        dimension_headers = [dim.name for dim in response.dimension_headers]
        metric_headers = [met.name for met in response.metric_headers]
        all_headers = dimension_headers + metric_headers

        rows_data = []
        for row in response.rows:
            row_values = [dim.value for dim in row.dimension_values] + [
                met.value for met in row.metric_values
            ]
            rows_data.append(row_values)

        output_path = os.path.join(script_dir, output_csv)
        df = pd.DataFrame(rows_data, columns=all_headers)
        df.to_csv(output_path, index=False)
        print(f"\n[Success] Retrieved {len(df)} rows and saved to:\n  -> {output_path}\n")
        return True

    except Exception as e:
        print(f"\n[API Error] {e}\n")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        prop_id = sys.argv[1].strip()
    else:
        prop_id = input("Enter your GA4 Property ID (numeric): ").strip()

    if not prop_id:
        print("Error: Property ID cannot be empty.")
        sys.exit(1)

    run_ga4_report(prop_id)
