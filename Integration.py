import boto3
import zipfile
from fsplit.filesplit import FileSplit
import os
import glob
import gzip
from datetime import datetime
import psycopg2
import psycopg2.extensions

'''Download zipped file from s3 to EC2'''
def download_file(bucketName,fileName):
    s3 = boto3.client('s3')
    s3.download_file(bucketName,"input_files/"+fileName,fileName)
    #s3.download_file('axlpoc2','sample','Testing')

'''Unzip the downloaded file and save in unzipfolder then remove the downloaded file'''
def unzipping_file(fileName):
    with zipfile.ZipFile(fileName, "r") as zip_ref:
        zip_ref.extractall("unzipfolder")
    f = fileName.split(".")
    file = f[0]

    with zipfile.ZipFile("unzipfolder/"+file+"/site_hits.zip", "r") as zip_ref:
        zip_ref.extractall("unzipfolder/"+file)
    os.remove(fileName)
    split_file(file)

'''Take the unzipped file split the file and save them into splitfiles folder then remove the unzipped file'''
def split_file(file):
    os.remove('unzipfolder/'+file+"/site_hits.zip")
    files = os.listdir('unzipfolder/'+file)
    print ('reading file')
    file_size = os.path.getsize('unzipfolder/'+file+"/"+str(files[0]))/4
    print(file_size)
    filesrm = os.listdir('splitfiles')
    for i in filesrm:
        print(i)
        os.remove('splitfiles/' + i)

    for i in files:
        fs = FileSplit('unzipfolder/'+file+"/"+i, splitsize=file_size, output_dir='splitfiles')
        fs.split()
        os.remove('unzipfolder/'+file+"/"+i)
    #fs.split(include_header=True)

'''Take the files in splitfiles zip each file separately and save them to zippingfile with .gz extension then clear the splitfiles folder '''
def zipping_file():
    files = os.listdir('splitfiles')
    for i in files:
        print(i)
        input = open('splitfiles/'+i,'rb')
        s = input.read()
        input.close()
        output = gzip.GzipFile('zippingfile/'+i+'.gz','wb')
        output.write(s)
        output.close()
        os.remove('splitfiles/' + i)

'''Take all the .gz files and upload them to a folder in s3 bucket then clear zippingfile folder  '''
def upload_file(bucketName,fileName):
    folder = fileName.split('.')
    folderName = folder[0]
    files = os.listdir('zippingfile')
    s3 = boto3.client('s3')

    #s3.put_object(Bucket=bucketName, Key=(folderName + '/'))
    for i in files:
        #f = i.split('.')
        #name = f[0]+f[1]
        s3.upload_file('zippingfile/'+i,bucketName,folderName+'/'+i)
        os.remove('zippingfile/'+i)

'''Take all the uploaded files and copy them to Redshift using copy command'''
def copy(bucketName,fileName):
    f = fileName.split('.')
    file = f[0]
    conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
    con = psycopg2.connect(conn_string);
    copy_command = "copy clickstream.clickhits from 's3://"+bucketName+"/"+file+"/' iam_role 'arn:aws:iam::374091793621:role/redshift_to_s3_role' delimiter '\t' acceptanydate dateformat 'auto'  NULL AS 'NULL' EMPTYASNULL ESCAPE ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF gzip;"
    cur = con.cursor()
    cur.execute(copy_command)
    cur.execute("commit")
    con.close()

'''Read all the new columns and update them into new table with additional two columns(startTime,fileName) and update the status in the logs table'''
def dataMove(fileName):
    #fileName = input("Enter File Name:")
    x = datetime.now()
    startTime = x.strftime("%Y-%m-%d %H:%M:%S")
    fileID = fileName + str(startTime)
    conn_string = "dbname='userbhv' port='5439' user='lakshman' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
    con = psycopg2.connect(conn_string);
    sql5 = "delete from clickstream.clickhits"
    try:
        print("Entered")
        sql1 = ("insert into clickstream.logs (fileid,filename,starttime,endtime,status)values(%s,%s,%s,Null,1)")
        var1 = (fileID, fileName, startTime)
        sql2 = ("insert into clickstream.warehouse select *,%s,%s from clickstream.clickhits")
        var2 = (fileName, startTime)
        cur = con.cursor()
        cur.execute(sql1, var1)
        print("sql1 executed")
        cur.execute(sql2, var2)
        cur.execute(sql5)
        print("Worked fine")
        cur.execute("commit")
        # con.close()
    except:
        y = datetime.now()
        endTime = y.strftime("%Y-%m-%d %H:%M:%S")
        sql4 = ("update clickstream.logs set endTime = %s,status = 3 where fileID = %s")
        var4 = (endTime, fileID)
        cur = con.cursor()
        cur.execute(sql4, var4)
        cur.execute(sql5)
        cur.execute("commit")
        con.close()
    else:
        y = datetime.now()
        endTime = y.strftime("%Y-%m-%d %H:%M:%S")
        sql3 = ("update clickstream.logs set endTime = %s,status = 2 where fileID = %s")
        var3 = (endTime, fileID)
        cur = con.cursor()
        cur.execute(sql3, var3)
        cur.execute("commit")
        con.close()


def final():
    bucketName = input('Enter Bucket Name :')
    #print ('Success')
    fileName = input('Enter File Name :')
    download_file(bucketName,fileName)
    print('Downloaded file')
    unzipping_file(fileName)
    print("unzipped file")
    print("Split file")
    zipping_file()
    print("Zipping")
    upload_file(bucketName,fileName)
    print("Uploaded back")
    copy(bucketName,fileName)
    print("Copying")
    dataMove(fileName)
    print("data moved")


final()
