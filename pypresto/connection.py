import os
import time
import pandas
import requests
from copy import deepcopy as copy

class PrestoConnection :

    def __init__(self,host,user,catalog,port=8080,schema='default') :
        self.presto_server = '%s:%s' % (host,port)
        self.set_headers(catalog,schema,user)

    def set_headers(self,catalog,schema,user) :
        self.__headers = {
            'X-Presto-Catalog': catalog,
            'X-Presto-Source': 'pypresto',
            'X-Presto-Schema': schema,
            'User-Agent': 'pypresto',
            'X-Presto-User': user
        }

    def send_query(self,sql_query) :
        req_url = os.path.join(self.presto_server,'v1/statement')
        response = requests.post(req_url,headers=self.__headers,data=sql_query)
        if response.status_code != 200 :
            response.raise_for_status()
        return response.json()

    def run_query(self,sql_query) :
        response = self.send_query(sql_query)
        series = []
        columns = []
        while 'nextUri' in response :
            response = requests.get(response['nextUri'])
            if response.status_code != 200 :
                response.raise_for_status()
            response = response.json()
            if 'data' in response :
                columns = columns or [col['name'] for col in response['columns']]
                lst_rows = response['data']
                for row in lst_rows :
                    serie = {}
                    for idx in xrange(len(columns)) :
                        serie[columns[idx]] = row[idx]
                    series.append(copy(serie))
                print 'Getting data (rows: %s)' % len(series)
                time.sleep(1)
            else :
                print 'Running query'
                time.sleep(5)
        df = pandas.DataFrame(series)
        if df.empty :
            raise Exception('No data returned')
        return df
