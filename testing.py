from datetime import datetime
import psycopg2
import psycopg2.extensions
x = datetime.now()
newDate = (x.strftime("%Y-%m-%d %H:%M:%S"))
fileID="Adarsh1"
fileName = "Adarsh542"
conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
con = psycopg2.connect(conn_string);
sql1 = ("insert into clickstream.logs(fileid,filename,starttime,endtime,status) values(%s,%s,%s,Null,1)")
val = (fileID,fileName,newDate)
print("executed")
cur = con.cursor()
cur.execute(sql1,val)
cur.execute("commit")
con.close()


#sql1 = insert into clickstream.logs(fileid,filename,starttime,endtime,status) values ('LakshmanTest','Lakshman','2015-04-10 18:45:09',Null,1)