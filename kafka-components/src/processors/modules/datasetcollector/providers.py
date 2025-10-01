

import json 

class Providers:
    PROVIDERS_KEY   = 'providers'
    PROV_NAME_KEY   = 'provider_name'
    REG_NAME_KEY    = 'region_name'
    MSG_VERSION_KEY = 'msg_version'
    TEMPL_NAME_KEY  = 'template_name'
    USER_GROUP_KEY  = 'user_group'
    UUID_KEY        = 'uuid'
    KEYS_TO_COPY = [
        MSG_VERSION_KEY,
        TEMPL_NAME_KEY,
        USER_GROUP_KEY,
        UUID_KEY]   
    
    def __init__(self, logger=None):
        self.data = {}
        self.logger = logger # check if logger is not none
    
    def generate_key(self, obj:dict) -> str:
        k_list = [
            obj[self.PROV_NAME_KEY],
            obj[self.REG_NAME_KEY],
            obj[self.UUID_KEY]
        ]
        new_key = "_".join(k_list).lower()
        self.logger.debug(f"[providers][generate_key] Generated new key: {new_key}")
        return new_key

    def import_msg(self, msg_str: str):
        msg: dict = json.loads(msg_str)
        for prov_data in msg[self.PROVIDERS_KEY]:
            unified_msg = prov_data | { k:v for k,v in msg.items()
                                        if k in self.KEYS_TO_COPY 
                                        }
            prov_reg_uuid_key = self.generate_key(unified_msg)
            self.data[prov_reg_uuid_key] = unified_msg 
            if prov_reg_uuid_key in self.data:
                self.logger.debug(f"[providers][import_msg] Provider selector message already in memory. uuid: {prov_reg_uuid_key}.")
            else:
                self.logger.debug(f"[providers][import_msg] Added new Provider selector message. uuid: {prov_reg_uuid_key}.")
    
    def keys(self) -> set:
        prov_keys = set(self.data.keys())
        return prov_keys
    
    def get(self, key) -> dict:
        return self.data.get(key, None)