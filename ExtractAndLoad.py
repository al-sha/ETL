# Objective: Transport two tables (orders and returns) from a database called operations to a database called python (using .pgpass to not expose our database password)

# Import needed libraries
import petl as etl, psycopg2 as pg, sys
from sqlalchemy import *



# DB Connection
DB_Connection = {'operations':'dbname=operations user=etl host=127.0.0.1',
				 'python'    :'dbname=python     user=etl host=127.0.0.1'}


# Set Cursors
sourceConn = pg.connect(DB_Connection['operations']) #grab value by referencing key dictionary
targetConn = pg.connect(DB_Connection['python']) #grab value by referencing key dictionary
sourceCursor = sourceConn.cursor()
targetCursor = targetConn.cursor()


# Retrieve the names of the source tables to be copied
sourceCursor.execute("""select table_name from information_schema.columns where table_name in ('orders','returns') group by 1""")
sourceTables = sourceCursor.fetchall()


# Iterate through table names to copy over
for tables in sourceTables:
    targetCursor.execute("drop table if exists %s" % (t[0]))

    # SQL statement : Assign the results of this SQL Statement from source DB 
    sourceDs = etl.fromdb(sourceConn, 'select * from %s' % (t[0]))
    etl.todb(sourceDs, targetConn, t[0])

