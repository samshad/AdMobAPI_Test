from googleapiclient import discovery
from googleapiclient.http import build_http
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow

import json
import pandas as pd


class AdmobApi:
    def __init__(self):
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

    def get_report(self):
        date_range = {'startDate': {'year': 2020, 'month': 9, 'day': 19},
                      'endDate': {'year': 2020, 'month': 9, 'day': 20}}
        """sort_conditions = {'dimension': 'DATE', 'order': 'DESCENDING'}
        dimension_filters = {
            'dimension': 'COUNTRY',
            'matchesAny': {
                'values': ['BD']
            }
        }"""
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
        arr_csv = []

        for data in report:
            if 'row' in data:
                app = data['row']['dimensionValues']['APP']['displayLabel']
                country = data['row']['dimensionValues']['COUNTRY']['value']
                date = data['row']['dimensionValues']['DATE']['value']
                year = date[:4]
                month = date[4:6]
                day = date[6:]
                date_formated = '-'.join([day, month, year])
                platform = data['row']['dimensionValues']['PLATFORM']['value']
                ad_unit = data['row']['dimensionValues']['AD_UNIT']['displayLabel']
                ad_format = data['row']['dimensionValues']['FORMAT']['value']

                dimensions = [day, month, year, app, country, platform, ad_format, ad_unit]

                estimated_earnings = float(data['row']['metricValues']['ESTIMATED_EARNINGS']['microsValue']) / 1000000.0
                clicks = data['row']['metricValues']['CLICKS']['integerValue']
                impressions = data['row']['metricValues']['IMPRESSIONS']['integerValue']
                match_request = data['row']['metricValues']['MATCHED_REQUESTS']['integerValue']
                match_rate = data['row']['metricValues']['MATCH_RATE']['doubleValue'] if 'MATCH_RATE' in data['row'][
                    'metricValues'] else 0.0
                show_rate = data['row']['metricValues']['SHOW_RATE']['doubleValue'] if 'SHOW_RATE' in data['row'][
                    'metricValues'] else 0.0

                eCPM = ((float(estimated_earnings) / float(impressions)) * 1000.0) if float(impressions) != 0 else 0

                metrics = [eCPM, estimated_earnings, clicks, impressions, match_request, match_rate, show_rate]

                arr = dimensions
                for i in metrics:
                    arr.append(i)
                arr_csv.append(arr)

        df = pd.DataFrame(arr_csv,
                          columns=['day', 'month', 'year', 'app', 'country', 'platform', 'ad_format', 'ad_unit',
                                   'eCPM', 'estimated_earnings', 'clicks', 'impressions', 'match_request',
                                   'match_rate', 'show_rate'])
        df.to_csv('Data/sample_tmp.csv', index=False)


if __name__ == '__main__':
    test = AdmobApi()
    # report = test.get_report()
    # print(type(report), len(report))

    with open("Data/data.json") as f:
        report = json.load(f)

    test.parse_data(report)

