
from modules.datasetcollector.loganalyzer import LogAnalyzer

class DatasetMessage:
    MESSAGE_CREATION_DATE = 'submission_time'
    
    CREATION_DATA_KEY = "creation_date"
    PROVIDER_ID = 'provider_id'
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    
    KEY_TO_REMOVE = [
        CREATION_DATA_KEY,
        PROVIDER_ID,
        LogAnalyzer.LAST_SUBMITTION_DATE
    ]
    
    def __init__(self, prov, depl_status):
        self.msg = prov | depl_status
        self.msg[self.MESSAGE_CREATION_DATE] = self.msg[LogAnalyzer.LAST_SUBMITTION_DATE].strftime(self.DATETIME_FORMAT)
        for key in self.KEY_TO_REMOVE:
            del self.msg[key]
        
    def get_message(self):
        return self.msg
    
    
    