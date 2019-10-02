import psycopg2
conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
con = psycopg2.connect(conn_string);
statement = 'select column1 from clickstream.clickhits_1'
cur = con.cursor()
cur.execute(statement)
a = cur.fetchall()
print(a)
con.close()

