class FederationRegistryFeeder:
    
    TIMESTAMP_KEY = 'timestamp'
    PROVIDER_TO_REJECT = ["CLOUD-CNAF-TB"]
    PROVIDER_NAME_KEY = 'provider_name'
    REGION_NAME_KEY = 'region_name'
    PROJECT_ID_KEY = 'project_id'
    
    ## Monitoring
    MON_RECEIVED_MESSAGES = 'fedreg_rec_msgs'
    MON_DATABASE_SIZE = 'fedreg_database_size'
    
    ## Constant Collections
    PROVIDER_ID_KEYS = (PROVIDER_NAME_KEY, 
                        REGION_NAME_KEY, 
                        PROJECT_ID_KEY)

    def __init__(self, logger=None):
        self.logger = logger
        self.data = {}
        self.received_messages = 0
        
    def get_mon_data(self) -> dict[str,int]:
        return {
            self.MON_RECEIVED_MESSAGES: self.received_messages, 
            self.MON_DATABASE_SIZE: len(self.data)
            }
        
    def are_keys_present(self, m):
        return all(key in m for key in self.PROVIDER_ID_KEYS)
    
    def get_key(self, m):
        return '-'.join([m[k] for k in self.PROVIDER_ID_KEYS])
    
    def update_providers_data(self, msg: dict):
        self.received_messages += 1
        m = msg 
        if isinstance(m, dict):
            if m[self.PROVIDER_NAME_KEY] not in self.PROVIDER_TO_REJECT:
                if self.are_keys_present(m):
                    str_k = self.get_key(m)
                    if str_k not in self.data \
                    or self.TIMESTAMP_KEY not in self.data[str_k] \
                    or m[self.TIMESTAMP_KEY] > self.data[str_k][self.TIMESTAMP_KEY]:
                        self.data[str_k] = m
                else:
                    return f"Error: Required keys {self.PROVIDER_ID_KEYS} not present {m.keys()}"
                
    def print_deep(self):
        self.logger.debug(f"prov.data contains {len(self.data.keys())}")
        for key in self.data:
            self.logger.debug(key)
            
    def size(self) -> int:
        return self.get_mon_data()[self.MON_DATABASE_SIZE]