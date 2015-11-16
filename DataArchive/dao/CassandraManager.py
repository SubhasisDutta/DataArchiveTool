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
        self.columnstr = ','.join(self.columnList)
        
        
            
        
    
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
        self.delRecord=self.session.prepare('DELETE FROM '+self.table+' WHERE id = ?')
        self.delBatch= BatchStatement()
        self.delbatchCount=0
    
    def makeTempPreparedStatements(self):
        self.batchlimit=int(self.config.find('dataSource').find('batchlimit').text)
        insertPoints=self.getInsertPointString()
        self.insertBatch= BatchStatement()
        self.batchCount=0
        self.insertQuery=self.session.prepare('INSERT INTO '+self.table+' ('+self.columnstr+') VALUES ('+insertPoints+')')
    
    def getInsertPointString(self):
        insertPoints=''        
        for i in range(len(self.columnList)):
            insertPoints+="?,"
        return insertPoints[:-1]
    
    def operatorSymbol(self,operator): 
        if operator == 'LESS':
            return '<'
        elif operator == 'GREATER':
            return '>'
        elif operator == 'LESS_EQUAL':
            return '<='
        elif operator == 'GRATER_EQUAL':
            return '>='
        elif operator == 'EQUAL':
            return '='
        else:
            return '='
        
    def copyData(self,destinationManager,condition):
        query='SELECT '+self.columnstr+' from '+self.table+' WHERE '+condition+' LIMIT '+str(self.selectlimit)+' ALLOW FILTERING'        
        while True:
            rows = self.session.execute(query)
            if len(rows) == 0:
                break
            else:
                for row in rows:
                    data=[]    
                    for r in row:
                        data.append(r)                    
                    s=destinationManager.push(data,destinationManager.insertBatch)
                    if s:
                        #delete row from source
                        print row.id
                        pass
                        
                        
        
        return False
     
    def pop(self,id,writeType='ab'):        
        return self.batchQuery(self.delRecord, id)
    
           
    def push(self,dataList,batch,writeType='ab'):        
        return self.batchQuery(self.insertQuery, dataList,batch)
    
    def batchQuery(self,statement,data,batch):
        if self.batchCount < self.batchlimit:
            batch.add(statement,data)
            self.batchCount +=1
        else:
            batch.add(statement,data)
            self.session.execute(batch)
            self.batchCount =0
            batch= BatchStatement()
        return True
    
    def flushBatch(self,batch):
        self.session.execute(batch)
        return True 
