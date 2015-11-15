'''
Created on Nov 13, 2015

@author: Subhasis
'''

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
        
        #initilize the temp manager with config
        
        
        
        #read the column names from source
        coulmnList=self.sourceManager.getColumnList()
        print coulmnList
        
        
        
        
        
        