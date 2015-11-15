'''
Created on Nov 1 2015
@author: Subhasis
'''

import sys
from datetime import datetime
from service.DataArchive import DataArchive

if __name__ == '__main__': 
    print "Starting @ ",str(datetime.now())
    inputConfigurationFile=sys.argv[1]
    obj=DataArchive(inputConfigurationFile)
    
    processCompleate=obj.startArchive()
    if processCompleate:
        print "DONE"
    else:
        print "Archive FAILED."
    print "Finished @ ",str(datetime.now())