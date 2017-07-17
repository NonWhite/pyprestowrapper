# pyprestowrapper
Package for making queries on Presto server

## 1. Install package

	pip install pypresto

## 2. Use package

	from pypresto import PrestoConnection
	conn = PrestoConnection(host,catalog,user)
	query = 'select * from my_table limit 1'
	conn.run_query(query)
