import os
import time
import pandas
import requests
from base64 import b64encode
from copy import deepcopy as copy
from requests.auth import HTTPBasicAuth

class PrestoConnection :

    def __init(self,host,user,catalog,port=8080,schema='default',password=None) :
        self.presto_server = '%s:%s' % (host,port)
        self.user = user
        self.password = password
        self.set_headers(catalog,schema)
        self.set_auth(user,password)

    def set_headers(self,catalog,schema) :
        self.__headers = {
            'X-Presto-Catalog': catalog,
            'X-Presto-Source': 'pypresto',
            'X-Presto-Schema': schema,
            'User-Agent': 'pypresto',
            'X-Presto-User': self.user
        }

    def set_auth(self,user,pasword) :
        if pasword is None :
            self.__auth = None
        self.__auth = HTTPBasicAuth(user,pasword)

    def send_query(self,sql_query) :
        req_url = os.path.join(self.presto_server,'v1/statement')
        args = {
            'req_url': req_url,
            'data': sql_query
        }
        return self.make_request(**args)

    def make_request(self,req_url,method='GET',data=None) :
        if method == 'GET' :
            req_func = requests.get
        elif method == 'POST' :
            req_func = requests.post
        else :
            raise Exception('Method %s not supported' % method)
        args = {
            'url': req_url,
            'headers': self.__headers
        }
        if self.__auth :
            args['auth'] = self.__auth
        if data :
            args['data'] = data
        response = req_func(**args)
        if response.status_code != 200 :
            response.raise_for_status()
        response = response.json()
        if response.get('error',None) :
            raise Exception(response['error'])
        return response

    def run_query(self,sql_query) :
        response = self.send_query(sql_query)
        series = []
        columns = []
        while 'nextUri' in response :
            response = self.make_request(response['nextUri'])
            if 'data' in response :
                columns = columns or [col['name'] for col in response['columns']]
                lst_rows = response['data']
                for row in lst_rows:
                    serie = {}
                    for idx in xrange(len(columns)):
                        serie[columns[idx]] = row[idx]
                    series.append(copy(serie))
                #print 'Getting data (rows: %s)' % len(series)
                time.sleep(1)
            else:
                #print 'Running query'
                time.sleep(5)
        df = pandas.DataFrame(series)
        if df.empty :
            raise Exception('No data returned')
        return df
