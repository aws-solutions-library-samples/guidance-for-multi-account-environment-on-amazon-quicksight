class QSAnalysisDef:
    name =''
    id = ''
    CFNId = ''
    datasets = {}    
    QSUser = ''
    QSRegion = ''
    QSAdminRegion = ''
    AccountId = ''
    TemplateId = ''
    
    def __init__(self, name: str, id: str, QSUser:str, QSRegion:str, QSAdminRegion:str, AccountId:str, TemplateId:str):
        self.name = name
        self.id = id
        self.CFNId = 'ANA{analysis_id}'.format(analysis_id=id.replace('-', ''))
        self.QSUser = QSUser
        self.QSRegion = QSRegion
        self.QSAdminRegion = QSAdminRegion
        self.AccountId = AccountId
        self.TemplateId = TemplateId
        

    def getDependingDatasets(self):
                
        return ['DSet{dataset_id}'.format(dataset_id=datasetId.replace('-', '')) for datasetId in self.datasets ]


