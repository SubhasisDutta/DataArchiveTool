'''
Created on Nov 13, 2015

@author: Subhasis
'''
import datetime
import xml.etree.ElementTree as ET
from dao.CassandraManager import CassandraManager
# import bz2
import tarfile
import os
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key

class DataArchive(object):
    '''
    classdocs
    '''


    def __init__(self, inputConfigFile):
        '''
        Constructor
        '''
        self.inputConfigFile=inputConfigFile
        self.config=self.processXMLConfig()
        self.sourceManager=self.setSourceManager()
        self.tempManager=self.setTempManager()
     
    def setSourceManager(self):
        managerType=self.config.find('source').find('type').text
        if managerType == 'CASSANDRA':
            return CassandraManager(self.config.find('source'))
        else:
            return None
    
    def setTempManager(self):
        managerType=self.config.find('temporarydump').find('type').text
        if managerType == 'CASSANDRA':
            return CassandraManager(self.config.find('temporarydump'))
        else:
            return None
       
    def processXMLConfig(self):
        '''
        This method parses a XML file and returns an Object that acts as the input for generating data.
        '''        
        tree = ET.parse(self.inputConfigFile)
        root = tree.getroot()              
        return root
    
    def startArchive(self):        
        self.sourceManager.makeSourcePreparedStatements()
        self.tempManager.makeTempPreparedStatements()
        #Copy data from active table to temp table  
        print "Moving Data from main table to temporart Table .... "      
        status=self.copyRecordFromSourceToTemporary(self.config.find('condition'))
        if status:
            print "Data moved to Temporary Table"
        else:
            print "Data Transfer to Temporary Table failed."
            return False
        #Get the location of temporary table
        temp_table_folder=self.config.find('temporarydump').find('location').text
        print "Compressing data from Location : ",temp_table_folder
        #Get the location where the compressed file can be stored
        temp_compressed_location=self.config.find('temporarydump').find('compressLocation').text
        key_name=self.config.find('temporarydump').find('dataSource').find('keyspace').text+'.'+self.config.find('temporarydump').find('dataSource').find('table').text
        key_name+='-'+datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")        
        temp_compressed_location+=key_name
        tar_file=temp_compressed_location+'_archive.tar.gz'
#         temp_compressed_location+='_archive.bz2' 
        #This delay is to let the database system get all files consistent. 
        time.sleep(30)
        with tarfile.open(tar_file, "w:gz") as tar:
            tar.add(temp_table_folder, arcname=os.path.basename(temp_table_folder))
        tar.close()
        print "Table Compressed to :",tar_file
#         with open(tar_file, 'rb') as input_tar:
#             with bz2.BZ2File(temp_compressed_location, 'wb', compresslevel = 9) as output:
#                 while True:
#                     block = input_tar.read(900000)
#                     if not block:
#                         break
#                     output.write(block)
#         input_tar.close()
#         output.close()
#         except IOError:
#             raise IOError        
        print "Archive File Created :",temp_compressed_location                
        #move compressed file to Amazon S3
        acess_key=self.config.find('transfer').find('connection').find('AccessKey').text
        secret_key=self.config.find('transfer').find('connection').find('SecretAccessKey').text
        try:
            conn = S3Connection(acess_key, secret_key)
        except Exception:
            print "Error in Connecting to Amazon S3."
            return False
             
        #The access key needs write access to Amazon S3 
        bucket_name=self.config.find('transfer').find('connection').find('Bucket').text
#         bucket = conn.create_bucket(bucket_name)
        try:
            bucket = conn.get_bucket(bucket_name)
        except Exception:
            print "This bucket doesn't exist."
            return False
        s3_key = Key(bucket)
        s3_key.key = key_name
        s3_key.set_contents_from_filename(tar_file)
        #Let File be moved to Amazon S3
        time.sleep(60)
        #check if file copied to S3
           
        if not s3_key.exists():
            time.sleep(60)
        else:
            print "File moved to Amazon S3. Key :",key_name
        #Delete file from file system
        os.remove(tar_file)   
        print "Removed Temporary Data File"     
        #truncate temporary table   
        self.tempManager.truncateTable()
        print "Temporary Table Truncated"
        return True
        
        
    def copyRecordFromSourceToTemporary(self,conditionConfig):
        condition=self.prepareCondition(conditionConfig)
        print "Executing Condition : ",condition
        return self.sourceManager.moveData(self.tempManager,condition)
        
    def prepareCondition(self,conditionConfig): 
        column=conditionConfig.find('columnname').text   
        operator=conditionConfig.find('operator').text 
        valueType=conditionConfig.find('valuetype').text
        value=conditionConfig.find('value').text 
        condition=column+' '+self.sourceManager.operatorSymbol(operator)+' '+self.getConditionValue(valueType,value)
        return condition
    
    def getConditionValue(self,valueType,value):
        if valueType == 'DATETIME':            
            return self.processDateValue(value)
        elif valueType == 'INTEGER':
            return value
        elif valueType == 'BOOLEAN':
            return '\''+value+'\''
        else: # String and other type
            return '\''+value+'\''
        
    def processDateValue(self,value):        
        value=datetime.datetime.now()-self.getTimeDelta(value)
        return '\''+value.strftime("%Y-%m-%d %H:%M:%S")+'\''
    
    def getTimeDelta(self,value):
        timeDelta=value.split(':')[0]
        timedeltaUnit=value.split(':')[1]
        #microseconds, milliseconds, seconds, minutes, hours, days, weeks        
        if timedeltaUnit == 'HOURS':
            return datetime.timedelta(hours=int(timeDelta))
        if timedeltaUnit == 'DAYS':
            return datetime.timedelta(days=int(timeDelta))
        if timedeltaUnit == 'WEEKS':
            return datetime.timedelta(weeks=int(timeDelta)) 
        return datetime.timedelta(seconds=int(timeDelta))