'''
Created on Sep 16, 2015

@author: Subhasis
'''

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

class CassandraManager(object):
    '''
    This class takes care of writing the results into a cassandra file.
    '''
    def __init__(self, config):
        '''
        Constructor
        '''
        self.config=config        
        self.keyspace=self.config.find('dataSource').find('keyspace').text
        self.table=self.config.find('dataSource').find('table').text
                
        self.cluster = Cluster(self.getNodesInCluster())        
        self.session = self.cluster.connect(self.keyspace)
        
        self.columnList=self.getColumnList()
        

            
        
    
    '''
    This gets the Column names of the table.
    '''  
    def getColumnList(self):        
        query='SELECT column_name FROM system.schema_columns where keyspace_name=\''+self.keyspace+'\' AND columnfamily_name=\''+self.table+'\''
        columnsList=[]        
        columns=self.session.execute(query)
        for column in columns:                 
            columnsList.append(str(column.column_name))
        return columnsList
     
    def getNodesInCluster(self):
        clusterNodes=[]
        for clusterNode in self.config.find('clusters').findall('cluster'):
            clusterNodes.append(clusterNode.text)        
        return clusterNodes
    
    def makeSourcePreparedStatements(self):
        self.selectlimit=int(self.config.find('dataSource').find('selectlimit').text)
        
    
    def makeTempPreparedStatements(self):
        self.batchlimit=int(self.config.find('dataSource').find('batchlimit').text)
        insertPoints=self.getInsertPointString()
        self.insertBatch= BatchStatement()
        self.batchCount=0        
        columnstr = ','.join(self.columnList) 
        self.insertQuery=self.session.prepare('INSERT INTO '+self.table+' ('+columnstr+') VALUES ('+insertPoints+')')
    
    def getInsertPointString(self):
        insertPoints=''        
        for i in range(len(self.columnList)):
            insertPoints+="?,"
        return insertPoints[:-1]