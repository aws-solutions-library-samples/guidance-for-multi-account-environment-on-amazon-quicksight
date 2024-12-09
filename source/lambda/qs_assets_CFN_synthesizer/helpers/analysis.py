class QSAnalysisDef:
    name =''
    id = ''
    arn = ''
    CFNId = ''
    datasets = {}    
    QSUser = ''
    QSRegion = ''
    QSAdminRegion = ''
    AccountId = ''
    TemplateId = ''
    PipelineName = ''
    AssociatedDashboardId = ''
    
    def __init__(self, name: str, arn: str, QSUser:str, QSRegion:str, QSAdminRegion:str, AccountId:str,  PipelineName:str, AssociatedDashboardId: str):
        self.name = name
        self.arn = arn
        self.id = arn.split('analysis/')[-1]
        self.CFNId = 'ANA{analysis_id}'.format(analysis_id=self.id.replace('-', ''))
        self.QSUser = QSUser
        self.QSRegion = QSRegion
        self.QSAdminRegion = QSAdminRegion
        self.AccountId = AccountId
        self.TemplateId = '{analysis_name}-template'.format(analysis_name=name.replace(' ', '-'))
        self.PipelineName = PipelineName
        self.AssociatedDashboardId = AssociatedDashboardId
        

    def getDependingDatasets(self):
                
        return ['DSet{dataset_id}'.format(dataset_id=dataset.id.replace('-', '')) for dataset in self.datasets ]
    
    def getDatasetById(self, datasetId:str):
        for dataset in self.datasets:
            if dataset.id == datasetId:
                return dataset
        return None


