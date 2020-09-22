from googleapiclient import discovery
from googleapiclient.http import build_http
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow

import json


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

    def generate_report(self):
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


if __name__ == '__main__':
    test = AdmobApi()
    report = test.generate_report()
    print(type(report), len(report))

    with open('sample_full.json', 'w') as f:
        json.dumps(report, f)

    """for data in report[1:2]:
        app = data['row']['dimensionValues']['APP']['displayLabel']
        country = data['row']['dimensionValues']['COUNTRY']['value']
        date = data['row']['dimensionValues']['DATE']['value']
        year = date[:4]
        month = date[4:6]
        day = date[6:]
        date_formated = '-'.join([day, month, year])
        print(date, date_formated)"""
