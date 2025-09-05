import pandas as pd 
from datetime import datetime, timedelta

class Rally:
    
    ## Dataset column names
    PROVIDER_COL = 'provider'
    STATUS_COL = 'status'
    TEST_RESULT_COL = 'test_result'
    TIMESTAMP_COL = 'timestamp'
    MSG_VERSION = 'msg_version'
    
    ## Column values
    STATUS_FINISHED = 'finished'
    TEST_RESULT_TRUE = 'True'
    TEST = False
    
    ## Monitoring
    MON_RECEIVED_MESSAGES = 'rally_rec_msgs'
    MON_DATASET_SIZE = 'rally_dataset_size'
    
    def __init__(self, logger=None):
        self.logger = logger
        self.df = pd.DataFrame()
        self.received_messages = 0
    
    def size(self) -> int:
        return self.get_mon_data()[self.MON_DATASET_SIZE]

    def get_mon_data(self) -> dict[str,int]:
        return {
            self.MON_RECEIVED_MESSAGES: self.received_messages, 
            self.MON_DATASET_SIZE: self.df.shape[0]
            }
    
    def import_multiple_messages(self, messages: list):
        
        self.received_messages += len(messages)
        # Create new dataframe from new message
        new_df = pd.DataFrame(messages).drop(self.MSG_VERSION, axis=1)
        
        # Format the timestamp and test result columns
        new_df[self.TIMESTAMP_COL] = pd.to_datetime(new_df[self.TIMESTAMP_COL])
        new_df[self.TEST_RESULT_COL] = new_df[self.TEST_RESULT_COL] == self.TEST_RESULT_TRUE
        
        # Merge two dataframes
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        
        # Filter out rows older than 30 days
        cutoff_date = datetime.now() - timedelta(days=30)
        self.df = self.df[self.df[self.TIMESTAMP_COL] >= cutoff_date]
        
        # Drop duplicates
        self.df = self.df.drop_duplicates()
        
    def import_single_message(self, message: dict):
        
        self.received_messages += 1
        # Create new dataframe from new message
        new_df = pd.DataFrame([message]).drop(self.MSG_VERSION, axis=1)
        
        # Format the timestamp and test result columns
        new_df[self.TIMESTAMP_COL] = pd.to_datetime(new_df[self.TIMESTAMP_COL])
        new_df[self.TEST_RESULT_COL] = new_df[self.TEST_RESULT_COL] == self.TEST_RESULT_TRUE
        
        # Merge two dataframes
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        
        # Filter out rows older than 30 days
        cutoff_date = datetime.now() - timedelta(days=30)
        self.df = self.df[self.df[self.TIMESTAMP_COL] >= cutoff_date]
        
        # Drop duplicates
        self.df = self.df.drop_duplicates()
    
    def get_providers(self) -> list[str] | None:
        if len(self.df.shape[0]) > 0:
            prov_list = self.df[self.PROVIDER_COL].unique()
            return list(prov_list)
        else:
            self.logger.info("No provider in data")
            return None
    
    def get_rally_perc_failures(self, provider_name, days: int | float) -> float | None:
        
        if not isinstance(days,(int,float)):
            raise TypeError("'Days' is not integer or float")
        
        if self.df.shape[0] > 0:
            provider_list = list(self.df[self.PROVIDER_COL].unique())
            if provider_name in provider_list:
                now = datetime.now() if not self.TEST else self.df.timestamp.max()
                prov_df = self.df[(self.df[self.TIMESTAMP_COL] > now - timedelta(days=days)) &
                                  (self.df[self.STATUS_COL] == self.STATUS_FINISHED) &                         
                                  (self.df[self.PROVIDER_COL] == provider_name) ]
                
                failure = 1 - prov_df[self.TEST_RESULT_COL].sum() / prov_df.shape[0] 
                failure_perc = round(failure, 3)
                return failure_perc
            else:
                msg = f"No '{provider_name}' in the monitored providers: {provider_list}"
                self.logger.warn(msg)
                return None
        else:
            self.logger.error("No stored values")
            return None