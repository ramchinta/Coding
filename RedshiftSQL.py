
import psycopg2
from datetime import datetime

fileName = input("Enter File Name:")
startTime = datetime.now()
fileID = fileName+startTime
conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
con = psycopg2.connect(conn_string);
try:
    sql1 = 'insert into clickstream.logs (fileID,fileName,startTime,endTime,status)values(%s,%s,%s,Null,1)',(fileID,fileName,startTime)
    sql2 = 'insert into clickstream.warehouse select *,%s,%s from clickstream.warehouse as a left join clickstream.clickhits as b on a.columnx = b.columnx where b.columnx IS NULL ',(fileName,startTime)
    cur = con.cursor()
    cur.execute(sql1)
    cur.execute(sql2)
    cur.execute(sql3)
    con.close()
except:
    endTime = datetime.now()
    sql4 ='update clickstream.logs set endTime = %s,status = 3 where fieldID = %s'(endTime,fileID)
    cur = con.cursor()
    cur.execute(sql4)
    con.close()
else:
    endTime = datetime.now()
    sql3 = 'update clickstream.logs set endTime = %s,status = 2 where fileID = %s', (endTime, fileID)
    cur = con.cursor()
    cur.execute(sql3)
    con.close()



