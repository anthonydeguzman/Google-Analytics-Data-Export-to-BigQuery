import os
import csv
import time
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.api_core.exceptions import NotFound
from report_requests import get_report_requests

load_dotenv()

# Set up your service account key file path
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'temp.json'

# Set up your GA view ID
VIEW_ID = os.getenv('VIEW_ID')

YEAR = '2023'

START_DATE = f"{YEAR}-01-01"
END_DATE = f"{YEAR}-12-31"

newpath = f"./output/{YEAR}"

def initialize_analyticsreporting():
    credentials = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
        scopes=['https://www.googleapis.com/auth/analytics.readonly']
    )
    return build('analyticsreporting', 'v4', credentials=credentials)

def get_report(analytics, report_request):
    report_responses = []
    page_token = None

    while True:
        #  Avoid quotas
        time.sleep(2)

        # Add the page token to the report request
        report_request['pageToken'] = page_token

        # Execute the report request
        response = analytics.reports().batchGet(
            body={'reportRequests': [report_request]}
        ).execute()

        # Append the report response to the list
        report_responses.append(response)

        # Check if there are more pages available
        next_page_token = response['reports'][0].get('nextPageToken')
        if next_page_token:
            print("- Paginating")
            page_token = next_page_token
        else:
            break

    return report_responses

def main():
    if not os.path.exists(newpath):
        os.makedirs(newpath)

    analytics = initialize_analyticsreporting()
    report_requests = get_report_requests(VIEW_ID, START_DATE, END_DATE)

    for request in report_requests:
        table_id = request['table_id']
        report_request = request['report_request']

        file_output = f"output/{YEAR}/{table_id}.csv"

        # Skip if there's already a file i.e. don't overwrite
        if os.path.exists(file_output):
            continue

        print(table_id)
        report = get_report(analytics, report_request)

        # Extract schema from report
        dimensions = report_request['dimensions']
        metrics = report_request['metrics']
        schema = [dim['name'].replace('ga:', '') for dim in dimensions]
        schema += [metric['expression'].replace('ga:', '') for metric in metrics]


        with open(file_output, 'w', newline='') as csvfile:
            rows = []
            for response in report:
                if 'rows' in response['reports'][0]['data']:
                    rows += response['reports'][0]['data']['rows']
            rows_to_insert = []
            for row in rows:
                record = {}
                for i, dim in enumerate(dimensions):
                    record[dim['name'].replace('ga:', '')] = row['dimensions'][i]
                for i, metric in enumerate(metrics):
                    record[metric['expression'].replace('ga:', '')] = row['metrics'][0]['values'][i]
                rows_to_insert.append(record)

            if not rows_to_insert:
                print('- No data')
                os.remove(file_output)
                continue

            fieldnames = schema
            print('- Creating CSV')
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_to_insert)

if __name__ == '__main__':
    main()
