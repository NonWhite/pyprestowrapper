# pyprestowrapper
Package for making queries on Presto server

## 1. Install package

	pip install pypresto

## 2. Use package

	from pypresto import PrestoConnection
	conn = PrestoConnection(host, user, catalog)
	query = 'select * from my_table limit 1'
	conn.run_query(query)

## 3. Usage for authenticated Presto:

	from pypresto import PrestoConnection
	conn = PrestoConnection(host, user, catalog, port, schema, password)
	query = 'select * from my_table limit 1'
	conn.run_query(query, auth=True)
