
import psycopg2
from datetime import datetime

fileName = "Adarsh789"
x = datetime.now()
startTime = x.strftime("%Y-%m-%d %H:%M:%S")
fileID = fileName+str(startTime)
conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
con = psycopg2.connect(conn_string);
sql5 = "delete from clickstream.clickhits"
try:
    print("Entered")
    sql1 = ("insert into clickstream.logs (fileid,filename,starttime,endtime,status)values(%s,%s,%s,Null,1)")
    var1 =(fileID,fileName,startTime)
    sql2 = ("insert into clickstream.warehouse select *,%s,%s from clickstream.clickhits_1")
    var2 = (fileName,startTime)
    cur = con.cursor()
    cur.execute(sql1,var1)
    print("sql1 executed")
    cur.execute(sql2,var2)
    cur.execute(sql5)
    print("Worked fine")
    #con.close()
except:
    y = datetime.now()
    endTime = y.strftime("%Y-%m-%d %H:%M:%S")
    sql4 =("update clickstream.logs set endTime = %s,status = 3 where fileID = %s")
    var4 = (endTime,fileID)
    cur = con.cursor()
    cur.execute(sql4,var4)
    cur.execute(sql5)
    con.close()
else:
    y = datetime.now()
    endTime = y.strftime("%Y-%m-%d %H:%M:%S")
    sql3 = ("update clickstream.logs set endTime = %s,status = 2 where fileID = %s")
    var3 =(endTime, fileID)
    cur = con.cursor()
    cur.execute(sql3,var3)
    cur.execute("commit")
    con.close()



