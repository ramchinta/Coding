import psycopg2
#Amazon Redshift connect string
conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
#connect to Redshift (database should be open to the world)
con = psycopg2.connect(conn_string);
print("Successfully connected")
#copy_command = "copy clickstream.clickhits from 's3://axlpoc2/AxelaarPoc2/' credentials 'aws_iam_role=arn:aws:iam::374091793621:role/redshift_to_s3_role'  delimiter '\t'"
#copy_command = "copy clickstream.clickhits_2 from 's3://axlpoc2/AxelaarPoc2/' iam_role 'arn:aws:iam::374091793621:role/redshift_to_s3_role' delimiter '\t' acceptanydate dateformat 'auto'  NULL AS 'NULL' EMPTYASNULL ESCAPE ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF gzip;"
copy_command = "copy clickstream.clickhits_1 from 's3://axlpoc2/AxelaarPoc2/' iam_role 'arn:aws:iam::374091793621:role/redshift_to_s3_role' delimiter '\t' acceptanydate dateformat 'auto'  NULL AS 'NULL' EMPTYASNULL ESCAPE ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF gzip;"

cur = con.cursor()
print("Successfully assigned copy command")
cur.execute(copy_command)
print("Successfully exec copy ")
con.close()



