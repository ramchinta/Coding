import boto3
import zipfile
from fsplit.filesplit import FileSplit
import os
import glob
import gzip
from datetime import datetime

'''Download zipped file from s3 to EC2'''
def download_file(bucketName,fileName):
    s3 = boto3.client('s3')
    s3.download_file(bucketName,fileName,fileName)
    #s3.download_file('axlpoc2','sample','Testing')

'''Unzip the downloaded file and save in unzipfolder then remove the downloaded file'''
def unzipping_file(fileName):
    with zipfile.ZipFile(fileName, "r") as zip_ref:
        zip_ref.extractall("unzipfolder")
    os.remove(fileName)

'''Take the unzipped file split the file and save them into splitfiles folder then remove the unzipped file'''
def split_file():
    files = os.listdir('unzipfolder')
    print ('reading file')
    file_size = os.path.getsize('unzipfolder/'+str(files[0]))/4
    print(file_size)
    for i in files:
        fs = FileSplit('unzipfolder/'+i, splitsize=file_size, output_dir='splitfiles')
        fs.split()
        os.remove('unzipfolder/'+i)
    #fs.split(include_header=True)

'''Take the files in splitfiles zip each file separately and save them to zippingfile with .gz extension then clear the splitfiles folder '''
def zipping_file():
    files = os.listdir('splitfiles')
    for i in files:
        input = open('splitfiles/'+i,'rb')
        s = input.read()
        input.close()
        output = gzip.GzipFile('zippingfile/'+i+'.gz','wb')
        output.write(s)
        output.close()
        os.remove('splitfiles/' + i)

'''Take all the .gz files and upload them to a folder in s3 bucket then clear zippingfile folder  '''
def upload_file(fileName):
    folder = fileName.split('.')
    folderName = folder[0]
    files = os.listdir('zippingfile')
    for i in files:
        f = i.split('.')
        name = f[0]
        s3 = boto3.client('s3')
        s3.upload_file('zippingfile/'+i,'axlpoc2',folderName+'/'+name+'.gz')
        os.remove('zippingfile/'+i)

'''Take all the uploaded files and copy them to Redshift using copy command'''
def copy(bucketName,fileName):
    f = fileName.split('.')
    file = f[0]
    conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
    con = psycopg2.connect(conn_string);
    copy_command = "copy clickstream.clickhits_1 from 's3://%s/%s/' iam_role 'arn:aws:iam::374091793621:role/redshift_to_s3_role' delimiter '\t' acceptanydate dateformat 'auto'  NULL AS 'NULL' EMPTYASNULL ESCAPE ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF gzip;",(bucketName,file)
    cur = con.cursor()
    cur.execute(copy_command)
    con.close()

'''Read all the new columns and update them into new table with additional two columns(startTime,fileName) and update the status in the logs table'''
def dataMove(fileName):
    #fileName = input("Enter File Name:")
    startTime = datetime.now()
    fileID = fileName + startTime
    conn_string = "dbname='userbhv' port='5439' user='pujita' password='AxlCs*123*' host='axlpoc2rs.cphm5aouzbjy.us-east-1.redshift.amazonaws.com'"
    con = psycopg2.connect(conn_string);
    try:
        sql1 = 'insert into clickstream.logs (fileID,fileName,startTime,endTime,status)values(%s,%s,%s,Null,1)', (
        fileID, fileName, startTime)
        sql2 = 'insert into clickstream.warehouse select *,%s,%s from clickstream.warehouse as a left join clickstream.clickhits as b on a.columnx = b.columnx where b.columnx IS NULL ', (
        fileName, startTime)
        cur = con.cursor()
        cur.execute(sql1)
        cur.execute(sql2)
        cur.execute(sql3)
        con.close()
    except:
        endTime = datetime.now()
        sql4 = 'update clickstream.logs set endTime = %s,status = 3 where fieldID = %s'(endTime, fileID)
        cur = con.cursor()
        cur.execute(sql4)
        con.close()
    else:
        endTime = datetime.now()
        sql3 = 'update clickstream.logs set endTime = %s,status = 2 where fileID = %s', (endTime, fileID)
        cur = con.cursor()
        cur.execute(sql3)
        con.close()


def final():
    bucketName = input('Enter Bucket Name :')
    #print ('Success')
    fileName = input('Enter File Name :')
    download_file(bucketName,fileName)
    unzipping_file(fileName)
    split_file()
    zipping_file()
    upload_file(fileName)
    copy(bucketName,fileName)
    dataMove(fileName)


final()


