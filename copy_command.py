import psycopg2
#Amazon Redshift connect string
conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
#connect to Redshift (database should be open to the world)
con = psycopg2.connect(conn_string);
copy_command = "copy users from 's3://axlpoc2/AxelaarPoc2/' credentials 'aws_iam_role=arn:aws:iam::374091793621:role/redshift_to_s3_role' delimiter '/t'"


cur = con.cursor()
cur.execute(copy_command)
con.close()