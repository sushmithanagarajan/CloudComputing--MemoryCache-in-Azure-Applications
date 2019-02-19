#References
#https://stackoverflow.com/questions/29638136/how-to-speed-up-bulk-insert-to-ms-sql-server-from-csv-using-pyodbc
#https://docs.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql
#https://pypi.python.org/pypi/redis
#https://docs.objectrocket.com/redis_python_examples.html
#https://docs.microsoft.com/en-us/azure/storage/blobs/storage-dotnet-how-to-use-blobs
#https://github.com/MicrosoftDocs/azure-docs/blob/master/articles/storage/blobs/storage-python-how-to-use-blob-storage.md
#https://azure.microsoft.com/en-us/services/cache/
#https://stackoverflow.com/questions/39956798/using-the-right-redis-data-types-for-time-based-comparisons
#https://www.fullstackpython.com/blog/install-redis-use-python-3-ubuntu-1604.html
#http://www.bogotobogo.com/python/python_redis_with_python.php
#https://stackoverflow.com/questions/19205033/automatically-load-sql-table-by-reading-data-from-text-file



import csv
import sys
from flask import Flask,render_template,request
import pymysql  #connect to rds--mysql
import time   #query time
import cStringIO
import pymysql
from azure.storage.blob import PublicAccess
from flask import Flask
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
from flask import render_template
from flask import request
import glob
import os
import time
import datetime
import redis
import hashlib
import pickle as cPickle

#Connection parameters to the Host in Azure and the Database MYSQL in AZURE
hostname = 'm'
username = ''
password = 'S'
database = ''
myConnection = pymysql.connect( host=hostname, user=username, passwd=password, db=database, cursorclass=pymysql.cursors.DictCursor, local_infile=True)
#Establish connection with Pymysql connector
print 'DB connected'
# Connect with the Azure Blob storage with help of azure credentials , account details
block_blob_service = BlockBlobService(account_name='', account_key='')
print ('Blob connected')
global R_server
R_server = redis.StrictRedis(host='st',port=6380,db=0,password='kU=',ssl=True)   
myConnection.ping()
print 'Connected!'

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_ROOT = os.path.dirname(APP_ROOT)
print APP_ROOT

app = Flask(__name__)
#Function call to Main program and its definition
@app.route('/')
def main():
    print "Welcome to the PhotoSharing Application"
    return render_template('memcache.html')

@app.route('/Upload',methods=['get','post'])
def Upload():
    return render_template('Upload.html')

@app.route('/uploadImage',methods=['get','post'])
def upload1():
    global file_name
    f = request.files['upload_files']
    file_name = f.filename
    print (file_name)
    titlename = request.form['file']
    #Get the filename from the user and open the windows explorer to select images
    global newfile
    newfile = os.path.abspath(file_name)
    print "new file down"
    print newfile
    #Get the Blob storage details , container name from azure and it connects to the respective blob , saves image files there
    block_blob_service.create_blob_from_path('memcachecontainer',file_name,newfile,content_settings=ContentSettings(content_type='text/csv'))
    imgUrl = 'https://cs' + file_name
    #This provides the ImageURL to the azure account and its blob location
    return render_template('CSVExecution.html')

@app.route('/csvimport',methods=['get','post'])
def csvimport():
    print "i am here"
    cur = myConnection.cursor()
    cur.text_factory = str  # allows utf-8 data to be stored
    # traverse the directory and process each .csv file
    print newfile
    global split_file
    split_file = file_name.split('.')[0]
    tablename = split_file
    print "table name is"
    print tablename
    with open(newfile) as f:
        reader = csv.reader(f)
        header = True
        print "value"
        print tablename
        sql1 = "DROP TABLE IF EXISTS %s" %tablename
        print  sql1
        cur.execute(sql1)
        print cur.execute(sql1)
        for row in reader:
                # gather column names from the first row of the csv
                header = False
                line=row
                break
        column_name="( "
        sql="CREATE TABLE %s"%tablename
        for i in line:
                #sql = "%s", ".join(["%s varchar(50)"%i ])
                column_name+=i+" VARCHAR(50), "

        sql+=column_name+" id_no int  PRIMARY KEY NOT NULL AUTO_INCREMENT);"
        print sql
        cur.execute(sql)
        print "sql executed the create query"
        #sql3 = "ALTER TABLE " +tablename + " ADD id_no int  PRIMARY KEY NOT NULL AUTO_INCREMENT"
        #print sql3
        #cur.execute(sql3)
        #print row
        newline="\\\n"
        print newfile
        print "this is it"
        insert_str="""LOAD DATA LOCAL INFILE \'""" +newfile+ """\' INTO TABLE """ +tablename+ """ FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;"""
        print insert_str
        cur.execute(insert_str)
        myConnection.commit()
        cur.close()
        return render_template("success.html")

@app.route('/Limit',methods=['get','post'])
def limit():
        limit = request.form['limit']
        query1 = "Select * from all_month where locationSource='" + limit + "';"
        starttime = time.time()
        print(starttime)
        with myConnection.cursor() as cursor:
            cursor.execute(query1)
        myConnection.commit()
        cursor.close()
        endtime = time.time()
        print('endtime')
        totalsqltime = endtime - starttime
        print(totalsqltime)
        return render_template("success.html", time1=totalsqltime)

@app.route('/querywithparam', methods=['POST'])
def querywithparam():
    	satavg1= request.form['satavg1']
	satavg2= request.form['satavg2']
	zip1 = request.form['zip1']
	zip2 = request.form['zip2']
	locquery="(select USZipcodes.city , Education.INSTURL , Education.INSTNM from USZipcodes INNER JOIN Education on USZipcodes.city  = Education.CITY where USZipcodes.zip between   "+zip1+" and "+zip2+" AND Education.SAT_AVG between  "+satavg1+" and "+satavg2+" limit 10);"
	print locquery
    	starttime = time.time()
    	print(starttime)
    	with myConnection.cursor() as cursor:
          cursor.execute(locquery)
          myConnection.commit()
	  data = cursor.fetchall()
    	  cursor.close()        
    	endtime = time.time()
    	print('endtime')
    	totalsqltime = endtime - starttime
    	print(totalsqltime)
    	return render_template('success.html', time2=totalsqltime, result = data)

@app.route('/memexec', methods=['POST'])
def memexec():
 	TTL = 36
 	limit = request.form['limit']
	sql="select * from all_month limit " +limit
	print "I am atlast here" + sql
        beforeTime = time.time()
	hash=hashlib.sha224(sql).hexdigest()
	key="sql_cache:" + hash
	print "Created Key\t: %s" % key
	if (R_server.get(key)):
    		print "it is returned from redis"
    		return cPickle.loads(R_server.get(key))
    	else:
        # Do MySQL query 
	   with myConnection.cursor() as cursor:
              cursor.execute(sql)
	      data = cursor.fetchall()
              myConnection.commit()
              cursor.close()	
         # Put data into cache for 1 hour
       	R_server.set(key, cPickle.dumps(data) )
       	R_server.expire(key, TTL);
       	print "Set data redis and return the data"
       	afterTime = time.time()
	Totaltime = afterTime-beforeTime
	#print (str(float(Totaltime)))
   	return 'Took time : ' + str(Totaltime)

	



#the main function to call the main definition first
if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')

