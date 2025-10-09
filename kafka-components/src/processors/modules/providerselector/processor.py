
import threading
from sys import maxsize as sys_maxsize
from time import sleep, time
from modules.providerselector.fedregfeeder import FederationRegistryFeeder
from modules.providerselector.rally import Rally 
from modules.providerselector.settings import ProviderSelectorConfig
from modules.utilities.kafka_client import KafkaClient
from modules.providerselector import conf as ipc
import json

class ProviderSelectorProcessor:
    
    def __init__(self, settings: ProviderSelectorConfig, logger = None):
        self.rally_data = Rally(logger)
        self.fedreg_mon  = FederationRegistryFeeder(logger)
        self.logger = logger
        self.settings = settings
        self.output_uuids = None
        self.input_valtempl_uuids = None
        self.settings.KAFKA_INPUT_TOPICS = [self.settings.KAFKA_INPUT_RALLY_TOPIC,
                                   self.settings.KAFKA_INPUT_FEDREG_TOPIC,
                                   self.settings.KAFKA_INPUT_VALTEMPL_TOPIC]
        self.RESTORE_TOPICS = [self.settings.KAFKA_INPUT_RALLY_TOPIC,
                               self.settings.KAFKA_INPUT_FEDREG_TOPIC,
                               self.settings.KAFKA_OUTPUT_TOPIC]
        
        self.kafka_client = KafkaClient(logger, **settings.get_values())
        self.msg_sent = 0
        
        if self.settings.MONITORING_ENABLED:
            monitor_thread = threading.Thread(target=self.monitoring_task)
            monitor_thread.daemon = True
            monitor_thread.start()
        
    def monitoring_task(self):
        while True:
            mon = self.get_mon_data()
            self.logger.info(f"Monitoring data: {mon}")
            sleep(self.settings.MONITORING_PERIOD)
        
    def restore_history(self):
        self.logger.info("Start of initialization phase : Collecting message from output topic")
        start_time = time()
        self.logger.debug(f"Topics to collect messages: {self.RESTORE_TOPICS}")
        collected_msgs = self.kafka_client.collect_all_msgs_from_topics(self.RESTORE_TOPICS)
        interval_s = round(time()-start_time,2)
        tot_msg_num = sum([len(v) for v in collected_msgs.values()])
        self.logger.debug(f"Collected {tot_msg_num} tot messages in {interval_s} s")
        for topic, messages in collected_msgs.items():
            self.logger.debug(f"Collected {len(messages)} from topic {topic}")    
            
            # Collect messages from the output topic and extract template uuids
            if topic == self.settings.KAFKA_OUTPUT_TOPIC:
                self.output_uuids = {message.key.decode('utf-8') for message in messages if message.key is not None and message.key != ''}
                self.logger.debug(f"Imported {len(self.output_uuids)} messages from topic {self.settings.KAFKA_OUTPUT_TOPIC}")
                self.logger.debug(f"{self.output_uuids=}")
                
            # Collect messages from rally topic and aggregate them
            if topic == self.settings.KAFKA_INPUT_RALLY_TOPIC:
                msgs = [ msg.value for msg in messages if isinstance(msg.value, dict) ]
                self.rally_data.import_multiple_messages(msgs)
                self.logger.debug(f"Imported {self.rally_data.size()} rally messages from topic {self.settings.KAFKA_INPUT_RALLY_TOPIC}")
            
            # Collect messages from federation registry feeder topic and aggregate them
            if topic == self.settings.KAFKA_INPUT_FEDREG_TOPIC:
                for msg in messages:
                    self.fedreg_mon.update_providers_data(msg.value)
                self.logger.debug(f"Imported {self.fedreg_mon.size()} project mondata from topic {self.settings.KAFKA_INPUT_FEDREG_TOPIC}")

        mon = self.rally_data.get_mon_data() | self.fedreg_mon.get_mon_data()
        self.logger.debug(f"Monitoring data: {mon}")
        self.logger.info("End of initialization phase")    

    def get_mon_data(self) -> dict:
        return self.rally_data.get_mon_data() | self.fedreg_mon.get_mon_data() | {"msg_sent": self.msg_sent}
    
    
    def import_size(self, str_value):
        if   ipc.VT_GB_SIZE_SUFFIX in str_value: 
            value = float(str_value.replace(ipc.VT_GB_SIZE_SUFFIX, "")) 
        elif ipc.VT_TB_SIZE_SUFFIX in str_value: 
            value = float(str_value.replace(ipc.VT_TB_SIZE_SUFFIX, "")) * 1_000
        elif ipc.VT_PB_SIZE_SUFFIX in str_value: 
            value = float(str_value.replace(ipc.VT_PB_SIZE_SUFFIX, "")) * 1_000 * 1_000
        else:
            value = -1
        return value
    
    def get_compute_info(self, vt_c_obj: dict) -> dict:
        """vt_c_obj: Validated Template Computing Section Object"""

        info = { item:None for item in ipc.DD_COMP_KEYS }
        if ipc.VT_SCAL_KEY in vt_c_obj and \
        ipc.VT_PROP_KEY in vt_c_obj[ipc.VT_SCAL_KEY] and \
        ipc.VT_COUNT_KEY in vt_c_obj[ipc.VT_SCAL_KEY][ipc.VT_PROP_KEY]:
            info[ipc.DD_INSTANCE_KEY] = vt_c_obj[ipc.VT_SCAL_KEY][ipc.VT_PROP_KEY][ipc.VT_COUNT_KEY]
        if ipc.VT_HOST_KEY in vt_c_obj and \
        ipc.VT_PROP_KEY in vt_c_obj[ipc.VT_HOST_KEY]:
            host_obj = vt_c_obj[ipc.VT_HOST_KEY][ipc.VT_PROP_KEY] 
            info[ipc.DD_VCPUS_KEY] = int(host_obj[ipc.VT_NUM_CPUS])
            info[ipc.DD_RAM_KEY] = self.import_size(host_obj[ipc.VT_MEM_SIZE_KEY])
            if ipc.VT_DISK_SIZE_KEY in host_obj:
                info[ipc.DD_DISK_KEY] = self.import_size(host_obj[ipc.VT_DISK_SIZE_KEY])
            if ipc.VT_NUM_GPUS_KEY in host_obj:
                info[ipc.DD_GPUS_KEY] = int(host_obj[ipc.VT_NUM_GPUS_KEY])
            if info[ipc.DD_INSTANCE_KEY] is None: 
                info[ipc.DD_INSTANCE_KEY] = 1
        if ipc.VT_OS_KEY in vt_c_obj and \
        ipc.VT_PROP_KEY in vt_c_obj[ipc.VT_OS_KEY]:
            os_obj = vt_c_obj[ipc.VT_OS_KEY][ipc.VT_PROP_KEY]
            if ipc.VT_DISTRIBUTION_KEY in os_obj:
                info[ipc.DD_OS_DISTRO_KEY] = str(os_obj[ipc.VT_DISTRIBUTION_KEY])
            if ipc.VT_VERSION_KEY in os_obj:
                info[ipc.DD_OS_VERSION_KEY] = str(os_obj[ipc.VT_VERSION_KEY]) ## KeyError: 'version'
        return info

    def get_storage_info(self, vt_s_obj: dict) -> dict:
        """vt_s_obj = Validated Template Storage Section Object"""
        info = dict()
        if ipc.VT_SIZE_KEY in vt_s_obj:
            info[ipc.DD_STORAGE_KEY] = self.import_size(vt_s_obj[ipc.VT_SIZE_KEY])
            info[ipc.DD_VOLUMES_KEY] = 1
        return info

    # Import data in template
    def extract_data_from_valid_template(self, val_templ: dict) -> dict:
        if "dep_name" in val_templ:
            val_templ['template_name'] = val_templ['dep_name']
            del val_templ['dep_name']
        dep_data = {k:val_templ[k] for k in ipc.VT_KEYS_TO_IMPORT}
        dep_data[ipc.DD_NODE_INFO_KEY] = dict()
        dep_data[ipc.DD_STORAGE_INFO_KEY] = dict()
        dep_data[ipc.DD_PROVIDERS_KEY] = dict()
        top_templ = val_templ[ipc.VT_TOPOLOGY_KEY]
        for node_key, node_obj in top_templ[ipc.VT_NODE_KEY].items():
            if ( node_obj[ipc.VT_TYPE_KEY] == ipc.VT_BLOCKSTORAGE_TYPE or \
                node_obj[ipc.VT_TYPE_KEY] == ipc.VT_BLOCKSTORAGE_INDIGO_TYPE ) and \
                    ipc.VT_PROP_KEY in node_obj:
                vt_s_obj = node_obj[ipc.VT_PROP_KEY]
                dep_data[ipc.DD_STORAGE_INFO_KEY][node_key] = self.get_storage_info(vt_s_obj)
            elif node_obj[ipc.VT_TYPE_KEY] == ipc.VT_COMPUTER_TYPE and \
                ipc.VT_CAPABILITIES_KEY in node_obj:
                vt_c_obj = node_obj[ipc.VT_CAPABILITIES_KEY]
                dep_data[ipc.DD_NODE_INFO_KEY][node_key] = self.get_compute_info(vt_c_obj)
        for node_key, node_obj in top_templ[ipc.VT_OUTPUTS_KEY].items():
            if ipc.VT_IP_SUFFIX in node_key:
                if ipc.VT_VALUE_KEY in node_obj and \
                ipc.VT_GET_ATTR_KEY in node_obj[ipc.VT_VALUE_KEY] and \
                len(node_obj[ipc.VT_VALUE_KEY][ipc.VT_GET_ATTR_KEY]) > 1 and \
                ipc.VT_PUB_ADDR_KEY in node_obj[ipc.VT_VALUE_KEY][ipc.VT_GET_ATTR_KEY][1]:
                    node_name = node_obj[ipc.VT_VALUE_KEY][ipc.VT_GET_ATTR_KEY][0]
                    if node_name in dep_data[ipc.DD_NODE_INFO_KEY]:
                        dep_data[ipc.DD_NODE_INFO_KEY][node_name][ipc.DD_FLOATINGIPS_KEY] = 1
        return dep_data
    
    def get_info_from_provider(self, obj: dict) -> dict:
        info = {ipc.DD_IMAGES_KEY: False,
                ipc.DD_QUOTAS_KEY: {},
                ipc.DD_BEST_FLAVOR_KEY: {}
            }   
        
        info.update(
            {dd_key:obj[pd_key] for dd_key, pd_key in ipc.BASIC_PROV_DATA_INFO if pd_key in obj}
        )
        return info
    
    def create_key(self, base: str, usage: bool) -> str:
        return (base + ipc.DD_USAGE_SUFFIX) if usage else (base + ipc.DD_QUOTA_SUFFIX)

    def collect_quotas(self, prov_data: dict) -> dict:
        quotas = {}
        for prov_key,prov_obj in prov_data.items():
            if prov_key in ipc.M_QUOTAS_KEYS:
                for obj in prov_obj:
                    for q_elem in obj[ipc.PD_QUOTAS_KEY]:
                        for dd_key, pd_key, factor in ipc.M_QUOTAS_KEYS[prov_key]:
                            obj_key = self.create_key(dd_key, q_elem[ipc.PD_USAGE_KEY])
                            obj_value = int(q_elem[pd_key]/factor) # TypeError: unsupported operand type(s) for /: 'NoneType' and 'int'
                            quotas[obj_key] = obj_value
        return quotas

    def check_single_image(self, node, image):
        return node[ipc.DD_OS_DISTRO_KEY]  == image[ipc.PD_OS_DISTRO_KEY] and \
               node[ipc.DD_OS_VERSION_KEY] == image[ipc.PD_OS_VERSION_KEY]
           
    def is_image_present(self, node, data):
        images = data[ipc.PD_COMPUTE_SERVICES_KEY][0][ipc.PD_IMAGES_KEY]
        return any(self.check_single_image(node,image) for image in images)

    def are_present_images(self, dep_data, prov_data):
        nodes = dep_data[ipc.DD_NODE_INFO_KEY].values() 
        return all(self.is_image_present(node,prov_data) for node in nodes
                if node[ipc.DD_INSTANCE_KEY] > 0)
    
    def is_param_inclused(self, dn_param, pf_param):
        return dn_param <= pf_param if dn_param is not None and pf_param is not None else True

    def is_param_equal(self, dn_param, pf_param):
        return dn_param == pf_param if dn_param is not None and pf_param is not None else True

    def are_params_inclused(self, n_obj, f_obj):
        temp_list = []
        for dn_key, pf_key, f in ipc.MISC_BEST_MATCHER_KEYS:
            v = self.is_param_inclused(n_obj[dn_key], f_obj[pf_key] / f)
            temp_list.append(v)
        return all(temp_list)
    
    def are_params_equal(self, n_obj, f_obj):
        temp_list = []
        for dn_key, pf_key, f in ipc.MISC_BEST_MATCHER_KEYS:
            temp_list.append(self.is_param_equal(n_obj[dn_key], f_obj[pf_key] / f))
        return all(temp_list)
    
    def get_best_flavor(self, node_obj, prov_flavors):
        best_flavor = dict.fromkeys(ipc.DD_FLAVOR_KEYS, sys_maxsize)
        for prov_flavor in prov_flavors:
            if self.are_params_inclused(node_obj, prov_flavor):
                temp_list = []
                for dn_key, pf_key, f in ipc.MISC_BEST_MATCHER_KEYS:
                    temp_list.append(prov_flavor[pf_key]/f <= best_flavor[dn_key])
                if all(temp_list):
                    best_flavor = {dn_key: prov_flavor[pf_key]/f 
                                for dn_key, pf_key, f in ipc.MISC_BEST_MATCHER_KEYS}
                    best_flavor[ipc.DD_EXACT_FLAVOR] = self.are_params_equal(node_obj, prov_flavor)
        
        temp = node_obj.copy()
        temp.update(best_flavor)
        flavor_found = not any(
            best_flavor[k] == sys_maxsize 
            for k in ipc.DD_FLAVOR_KEYS 
        )
        return temp, flavor_found

    def best_match_finder(self, dep_data):
        p_key_to_remove = set()
        
        for p_key, p_obj in self.fedreg_mon.data.items():
            if p_obj[ipc.PD_USER_GROUP_KEY] != dep_data[ipc.DD_USER_GROUP_KEY]: 
                continue
    
            info = self.get_info_from_provider(p_obj)
            
            info[ipc.DD_QUOTAS_KEY] = self.collect_quotas(p_obj)
            info[ipc.DD_IMAGES_KEY] = self.are_present_images(dep_data, p_obj)
            
            for n_name, n_obj in dep_data[ipc.DD_NODE_INFO_KEY].items():
                if n_obj[ipc.DD_INSTANCE_KEY] < 1: 
                    continue
                p_flavors = p_obj[ipc.PD_COMPUTE_SERVICES_KEY][0][ipc.PD_FLAVORS_KEY]
                best_flavor_data, flavor_found = self.get_best_flavor(n_obj, p_flavors)
                if flavor_found:
                    info[ipc.DD_BEST_FLAVOR_KEY][n_name] = best_flavor_data
                    dep_data[ipc.DD_PROVIDERS_KEY][p_key] = info
                else:
                    p_key_to_remove.add(p_key)
                    self.logger.warning(f"No Flavor found for: {p_key} -> to remove")
        
        for p_key in p_key_to_remove:
            if p_key in dep_data[ipc.DD_PROVIDERS_KEY]:
                del dep_data[ipc.DD_PROVIDERS_KEY][p_key] 
        
        return dep_data

    def remove_null(self, dep_data: dict) -> dict:
        for _, prov_data in dep_data[ipc.DD_PROVIDERS_KEY].items():
            for flavor_obj in prov_data[ipc.DD_BEST_FLAVOR_KEY].values():
                for flavor_key, flavor_value in flavor_obj.items():
                    if flavor_value is None:
                        flavor_obj[flavor_key] = 0
        return dep_data

    def get_rally_provider_name(self, prov_data):
        keys_to_use = [ipc.PD_PROVIDER_NAME_KEY, 
                    ipc.PD_REGION_NAME_KEY]
        str_key = '-'.join([prov_data[k] for k in keys_to_use])
        return ipc.RALLY_PROVIDER_NAME_MAPPING[str_key]

    def compute_aggregated_resource(self, data):
        for dep_prov_obj in data[ipc.DD_PROVIDERS_KEY].values():
            aggr_res = {}
            nodes = dep_prov_obj[ipc.DD_BEST_FLAVOR_KEY].values()
            for fl_key in sorted(ipc.DD_AGGR_KEYS):
                acc_value = sum(node[fl_key] * node[ipc.DD_INSTANCE_KEY]        
                                for node in nodes 
                                if node[ipc.DD_INSTANCE_KEY] > 0)
                aggr_res[fl_key + ipc.DD_REQUIRED_SUFFIX] = acc_value
            acc_value = sum([node[ipc.DD_INSTANCE_KEY] for node in nodes])
            aggr_res[ipc.DD_INSTANCE_KEY + ipc.DD_REQUIRED_SUFFIX] = acc_value
            dep_prov_obj[ipc.DD_AGGREGATED_RESOURCES_KEY] = aggr_res

            rally_prov_name = self.get_rally_provider_name(dep_prov_obj)
            for n_days in ipc.DD_RALLY_N_DAYS:
                rally_value = self.rally_data.get_rally_perc_failures(rally_prov_name, n_days)
                rally_key = f"{ipc.DD_RALLY_VALUE_KEY}_{n_days}{ipc.DD_RALLY_DAY}"
                dep_prov_obj[rally_key] = rally_value if rally_value is not None else -1

        data[ipc.DD_AGGREGATED_STORAGE_KEY] = {}
        for stor_key in ipc.DD_STORAGE_SEC_KEYS:
            dep_stor_key = stor_key + ipc.DD_REQUIRED_SUFFIX
            dep_storage_obj = data[ipc.DD_STORAGE_INFO_KEY].values()
            acc_value = sum([storage_value.get(stor_key,0) 
                            for storage_value in dep_storage_obj])
            data[ipc.DD_AGGREGATED_STORAGE_KEY][dep_stor_key] = acc_value
        return data

    def get_msg(self, dep_data: dict) -> dict:
        m = {o_k:dep_data[dd_k] for o_k, dd_k in ipc.O_DD_KEYS_TO_IMPORT} 
        m[ipc.O_MSG_VERSION] = ipc.O_MSG_VERSION_VALUE
        m[ipc.O_PROVIDERS_KEY] = list()
        for provider in dep_data[ipc.DD_PROVIDERS_KEY].values():
            p_info = provider[ipc.DD_AGGREGATED_RESOURCES_KEY]
            p_info[ipc.DD_EXACT_FLAVORS] = p_info[ipc.DD_EXACT_FLAVOR + ipc.DD_REQUIRED_SUFFIX]
            del p_info[ipc.DD_DISK_KEY + ipc.DD_REQUIRED_SUFFIX]
            del p_info[ipc.DD_EXACT_FLAVOR + ipc.DD_REQUIRED_SUFFIX]
            for o_key,dd_key in ipc.O_PROV_DATA:
                if dd_key in provider:
                    p_info[o_key] = provider[dd_key]
            for k in provider:
                if ipc.DD_RALLY_VALUE_KEY in k:
                    nk = ipc.O_TEST_FAILURE_PERC + k.split(ipc.DD_RALLY_VALUE_KEY)[1]
                    p_info[nk] = provider[k]
            p_info.update(provider[ipc.DD_QUOTAS_KEY])
            p_info.update(dep_data[ipc.DD_AGGREGATED_STORAGE_KEY])
            for k,v in p_info.items():
                if isinstance(v,int): 
                    p_info[k] = float(v)
            m[ipc.O_PROVIDERS_KEY].append(p_info)
        return m

    def process_new_messages(self):
        for message in self.kafka_client.consumer:
            try:
                topic = str(message.topic)
                if topic == self.settings.KAFKA_INPUT_RALLY_TOPIC:
                    self.rally_data.import_single_message(message.value)
                elif topic == self.settings.KAFKA_INPUT_FEDREG_TOPIC:
                    self.fedreg_mon.update_providers_data(message.value)
                elif topic == self.settings.KAFKA_INPUT_VALTEMPL_TOPIC:
                    dep_data = self.extract_data_from_valid_template(message.value)
                    dep_data = self.best_match_finder(dep_data)
                    n_found_projects = len(dep_data['providers'])
                    if n_found_projects < 1:
                        self.logger.warning(f"No found any available projects for the deployment with uuid: {dep_data['uuid']}")
                        continue
                    
                    dep_data = self.remove_null(dep_data)
                    dep_data = self.compute_aggregated_resource(dep_data) 
                    msg = self.get_msg(dep_data)
                    
                    msg_uuid = msg['uuid']
                    if msg_uuid not in self.output_uuids:
                        self.kafka_client.send(msg)
                        self.logger.info(f"Template with uuid {msg_uuid} has been processed and sent successfully")
                        self.output_uuids.add(msg_uuid)
                        self.msg_sent += 1
                    else:
                        self.logger.info(f"Template with uuid {msg_uuid} already in the output topic")
            except Exception as e:
                self.logger.error(f"Error processing message from topic {topic}: {e}", exc_info=True)
                self.logger.error(f"Message: {json.dumps(message.value, indent=2)}")
                continue
                    
                    