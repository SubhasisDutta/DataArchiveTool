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
        self.batchCount=0
        self.batchlimit=int(self.config.find('selectlimit').text)
            
        
    
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
        self.delRecord_Statement=self.session.prepare('DELETE FROM '+self.table+' WHERE id = ?')
        self.delBatch= BatchStatement()        
    
    def makeTempPreparedStatements(self):        
        insertPoints=self.getInsertPointString()
        self.insertBatch= BatchStatement()        
        self.insertQuery_Statement=self.session.prepare('INSERT INTO '+self.table+' ('+self.columnstr+') VALUES ('+insertPoints+')')
    
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
        
    def moveData(self,destinationManager,condition):
        query='SELECT '+self.columnstr+' from '+self.table+' WHERE '+condition+' LIMIT '+str(self.batchlimit)+' ALLOW FILTERING'        
        while True:
            rows = self.session.execute(query)
            if len(rows) == 0 or rows == None:
                break
            else:
                for row in rows:
                    data=[]    
                    for r in row:
                        data.append(r)                    
                    s=destinationManager.push(data,destinationManager.insertBatch)
                    if s:
                        #delete row from source
                        self.pop(row.id,self.delBatch)
                        #print row.id
                    else:
                        print "There was some Problem in moving data from Main table to Temporary table."
                        
                        
                        
                        
        destinationManager.flushBatch(destinationManager.insertBatch)            
        self.flushBatch(self.delBatch)
        return True
     
    def pop(self,record_id,batch,writeType='ab'): 
        dataList=[]
        dataList.append(record_id)      
        return self.batchQuery(self.delRecord_Statement, dataList,batch)
    
           
    def push(self,dataList,batch,writeType='ab'):        
        return self.batchQuery(self.insertQuery_Statement, dataList,batch)
    
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
    
    def truncateTable(self):
        query='TRUNCATE '+self.table
        self.session.execute(query)
        return True
        