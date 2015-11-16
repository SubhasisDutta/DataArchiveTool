'''
Created on Nov 13, 2015

@author: Subhasis
'''
import datetime
import xml.etree.ElementTree as ET
from dao.CassandraManager import CassandraManager

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
        status=self.copyRecordFromSourceToTemporary(self.config.find('condition'))
        if status:
            print "Data moved to Temporary Table"
        else:
            print "Data Transfer to Temporary Table failed."
            return False
        #extract data from temp table to local file
        
        #read the column names from source
        coulmnList=self.sourceManager.getColumnList()
        
        
        #compress file using bz2
        
        
        #move compressed file to Amazon S3
        
        
        #check if file copied to S3
        
        
        #Delete file from file system
        
        
        #truncate temporary table   
        
        #return True
        
        
    def copyRecordFromSourceToTemporary(self,conditionConfig):
        condition=self.prepareCondition(conditionConfig)
        return self.sourceManager.copyData(self.tempManager,condition)
        
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