from enum import Enum

class ImportMode(Enum):
    SPICE = 1
    DIRECT_QUERY = 2
    

class QSDataSetDef:
    name =''
    id = ''
    placeholdername = ''
    dependingDSources = []
    physicalTableMap = []
    refreshSchedules = []
    CFNId = ''
    importMode = None
    def __init__(self, name: str, id: str, importMode: ImportMode, placeholdername: str, refreshSchedules: list, physicalTableMap: object):
        self.name = name
        self.id = id
        self.CFNId = 'DSet{id}'.format(id=id.replace('-', ''))        
        self.placeholdername = placeholdername
        self.physicalTableMap = physicalTableMap
        if 'RefreshSchedules' in refreshSchedules: 
            self.refreshSchedules = refreshSchedules['RefreshSchedules']        

        if not isinstance(importMode, ImportMode):
            raise Exception('resources.datasets.QSDataSetDef Error:importMode must be of type ImportMode')
        
        self.importMode = importMode
