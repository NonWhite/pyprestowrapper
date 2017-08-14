import os
import time
import pandas
import requests
from copy import deepcopy as copy
from base64 import b64encode
from requests.auth import HTTPBasicAuth

class PrestoConnection :

    def __init__(self, host, user, catalog, port=8080, schema='default', password=None):
        self.presto_server = '%s:%s' % (host,port)
        self.user = user
        self.password = password
        self.set_headers(catalog, schema)

    def set_headers(self, catalog, schema) :
        self.__headers = {
            'X-Presto-Catalog': catalog,
            'X-Presto-Source': 'pypresto',
            'X-Presto-Schema': schema,
            'User-Agent': 'pypresto',
            'X-Presto-User': self.user
        }

    def send_query(self, sql_query, auth=False):
        req_url = os.path.join(self.presto_server,'v1/statement')
        if auth:
            authentication = HTTPBasicAuth(self.user, self.password)
            response = requests.post(req_url, headers=self.__headers, data=sql_query, auth=authentication)
        else:
            response = requests.post(req_url, headers=self.__headers, data=sql_query)
        if response.status_code != 200:
            response.raise_for_status()
        return response.json()

    def run_query(self, sql_query, auth=False):
        response = self.send_query(sql_query, auth)
        series = []
        columns = []
        while 'nextUri' in response:
            if auth:
                authentication = HTTPBasicAuth(self.user, self.password)
                response = requests.get(response['nextUri'], auth=authentication)
            else:
                response = requests.get(response['nextUri'])
            if response.status_code != 200:
                response.raise_for_status()
            response = response.json()
            if 'data' in response:
                columns = columns or [col['name'] for col in response['columns']]
                lst_rows = response['data']
                for row in lst_rows:
                    serie = {}
                    for idx in xrange(len(columns)):
                        serie[columns[idx]] = row[idx]
                    series.append(copy(serie))
                print 'Getting data (rows: %s)' % len(series)
                time.sleep(1)
            else:
                print 'Running query'
                time.sleep(5)
        df = pandas.DataFrame(series)
        if df.empty:
            raise Exception('No data returned')
        return df
