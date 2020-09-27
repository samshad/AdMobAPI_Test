from googleapiclient import discovery
from googleapiclient.http import build_http
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow

import json
import pandas as pd
import db


class AdmobApi:
    def __init__(self):
        self.api_data = []

        with open("Auth/configs.json") as f:
            self.configs = json.load(f)

        scope = 'https://www.googleapis.com/auth/admob.report'
        name = 'admob'
        version = 'v1'

        flow = OAuth2WebServerFlow(client_id=self.configs['client_id'], client_secret=self.configs['client_secret'],
                                   scope=scope)
        storage = Storage(name + '.dat')
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage)
        http = credentials.authorize(http=build_http())
        self.admob = discovery.build(name, version, http=http)

    def get_report(self, start_date, end_date):
        start_year = int(start_date.split('-')[0])
        start_month = int(start_date.split('-')[1])
        start_day = int(start_date.split('-')[2])
        end_year = int(end_date.split('-')[0])
        end_month = int(end_date.split('-')[1])
        end_day = int(end_date.split('-')[2])

        date_range = {'startDate': {'year': start_year, 'month': start_month, 'day': start_day},
                      'endDate': {'year': end_year, 'month': end_month, 'day': end_day}}
        dimensions = ['DATE', 'APP', 'PLATFORM', 'COUNTRY', 'AD_UNIT', 'FORMAT']
        metrics = ['ESTIMATED_EARNINGS', 'CLICKS', 'IMPRESSIONS', 'MATCHED_REQUESTS', 'MATCH_RATE', 'SHOW_RATE']
        sort_conditions = {'dimension': 'DATE', 'order': 'DESCENDING'}
        report_spec = {'dateRange': date_range,
                       'dimensions': dimensions,
                       'metrics': metrics,
                       'sortConditions': [sort_conditions]}

        request = {'reportSpec': report_spec}
        return self.admob.accounts().networkReport().generate(parent='accounts/{}'.format(self.configs['publisher_id']),
                                                              body=request).execute()

    def parse_data(self, report):
        for data in report:
            if 'row' in data:
                app = data['row']['dimensionValues']['APP']['displayLabel']
                country = data['row']['dimensionValues']['COUNTRY']['value']
                date = data['row']['dimensionValues']['DATE']['value']
                year = date[:4]
                month = date[4:6]
                day = date[6:]
                date_formated = '-'.join([year, month, day])
                platform = data['row']['dimensionValues']['PLATFORM']['value']
                ad_unit = data['row']['dimensionValues']['AD_UNIT']['displayLabel']
                ad_format = data['row']['dimensionValues']['FORMAT']['value']

                dimensions = [date_formated, app, country, platform, ad_format, ad_unit]

                estimated_earnings = float(data['row']['metricValues']['ESTIMATED_EARNINGS']['microsValue']) / 1000000.0
                clicks = data['row']['metricValues']['CLICKS']['integerValue']
                impressions = data['row']['metricValues']['IMPRESSIONS']['integerValue']
                match_request = data['row']['metricValues']['MATCHED_REQUESTS']['integerValue']
                match_rate = data['row']['metricValues']['MATCH_RATE']['doubleValue'] if 'MATCH_RATE' in data['row'][
                    'metricValues'] else 0.0
                show_rate = data['row']['metricValues']['SHOW_RATE']['doubleValue'] if 'SHOW_RATE' in data['row'][
                    'metricValues'] else 0.0

                eCPM = ((float(estimated_earnings) / float(impressions)) * 1000.0) if float(impressions) != 0 else 0

                metrics = [estimated_earnings, clicks, impressions, match_request, match_rate, show_rate]

                arr = dimensions
                for i in metrics:
                    arr.append(i)
                self.api_data.append(arr)

    def write_csv(self):
        df = pd.DataFrame(self.api_data,
                          columns=['date_formated', 'app', 'country', 'platform', 'ad_format', 'ad_unit',
                                   'estimated_earnings', 'clicks', 'impressions', 'match_request',
                                   'match_rate', 'show_rate'])
        df.to_csv('Data/sample_tmp.csv', index=False)

    def insert_db(self):
        dbObj = db.MySql()
        dbObj.drop_table()
        dbObj.create_table()
        for arr in self.api_data:
            dbObj.insert_data([arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7], arr[8], arr[9], arr[10],
                               arr[11], "admob"])
        dbObj.print_table()


if __name__ == '__main__':
    test = AdmobApi()
    report_json = test.get_report("2020-09-22", "2020-09-23")

    test.parse_data(report_json)
    test.insert_db()
