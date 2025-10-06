import json
from datetime import datetime
import pytz

class LogAnalyzer:
    ORCLOG_SUBMISSION_LINE    = "Submission of deployment request to the IM. "
    ORCLOG_COMPLETED_LINE     = "Deployment completed successfully. "
    ORCLOG_ERROR_LINE         = "Deployment in error. "
    # ORCLOG_ORCHESTARTOR_TAG   = " paas-orchestrator-pre orchestrator/"
    LOG_SEP_DEFAULT = 'paas-orchestrator orchestrator/'
    LOG_SEP_KEY = "log_sep"
    ORCLOG_ERROR_SUMMARY_LINE = " Retries on cloud providers exhausted."
    ORCLOG_SYSLOG_TS_FORMAT   = "%Y-%m-%dT%H:%M:%S%z" # YYYY-MM-DD HH:MM:SS+ZZ:ZZ
    ORCLOG_FILEBEAT_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ" 
    ORCLOG_PROVIDER_NAME      = "provider_name"
    ORCLOG_PROVIDER_REGION    = 'provider_region'
    ORCLOG_UUID               = 'uuid'
    ORCLOG_STATUS             = 'status'
    ORCLOG_STATUS_REASON      = 'status_reason'
    
    LINE_MESSAGE = "message"
    LINE_TIMESTAMP = "timestamp"
    
    LINES_TO_REJECT = [
        'it.reply.orchestrator.exception.service.DeploymentException: Error executing request to IM'
    ]

    # Internal variables
    TIMESTAMP   = 'timestamp'
    CREATION_DATE = 'creation_date' 
    STATUS = 'status'
    STATUS_REASON = 'status_reason'
    N_FAILURES = 'n_failures'
    TOT_FAILURE_TIME = 'tot_failure_time_s'
    COMPL_TIME = 'completion_time_s'
    LAST_SUBMITTION_DATE = 'last_sub_date'
    PROVIDER_ID = 'provider_id'
    UUID = 'uuid'
    PROV_ID = [
        ORCLOG_PROVIDER_NAME,
        ORCLOG_PROVIDER_REGION
    ]
    
    # Status
    STATUS_SUBMITTED = 'SUBMISSION_EVENT'
    STATUS_COMPLETED = 'CREATE_COMPLETE'
    STATUS_FAILED    = 'CREATE_FAILED'

    def __init__(self, settings, logger = None):
        self.logger = logger
        self.depl_status = {}
        self.data = {}
        
        # Init Monitoring metrics:
        self.detected_events = 0
        
        # Get log separator from settings
        if hasattr(settings, self.LOG_SEP_KEY):
            self.ORCLOG_ORCHESTARTOR_TAG = settings.LOG_SEP
        else:
            self.ORCLOG_ORCHESTARTOR_TAG = self.LOG_SEP_DEFAULT
        self.logger.info(f"Using log separator: {self.LOG_SEP}")
        
    def is_line_to_reject(self, line):
        msg = line[self.LINE_MESSAGE]
        return self.ORCLOG_ORCHESTARTOR_TAG not in msg or \
            any(item in msg for item in self.LINES_TO_REJECT)

    def is_submission_event(self, line):
        msg = line[self.LINE_MESSAGE]
        return self.ORCLOG_SUBMISSION_LINE in msg

    def is_completed_event(self, line):
        msg = line[self.LINE_MESSAGE]
        return self.ORCLOG_COMPLETED_LINE in msg

    def is_error_event(self, line):
        msg = line[self.LINE_MESSAGE]
        return self.ORCLOG_ERROR_LINE in msg
    
    def get_mon_data(self):
        return {"detected_events": self.detected_events}
    
    def generate_key(self, obj):
        if  self.ORCLOG_PROVIDER_NAME in obj and \
            self.ORCLOG_PROVIDER_REGION in obj and \
            self.ORCLOG_UUID in obj:
            k_list = [
                obj[self.ORCLOG_PROVIDER_NAME],
                obj[self.ORCLOG_PROVIDER_REGION],
                obj[self.ORCLOG_UUID]
            ]
            return "_".join(k_list).lower()
        else:
            self.logger.debug(f"[log-analyzer][generate_key] It is not possible create key from obj: {obj}")
            self.logger.debug(f"[log-analyzer][generate_key] Missed {self.ORCLOG_PROVIDER_NAME} and/or {self.ORCLOG_PROVIDER_REGION} and/or {self.ORCLOG_UUID} keys")
    
    def print_special_chars(self, my_string):
        for char in my_string:
            print(f"    {char}     |      {hex(ord(char))}")
    
    # Controllare se ci sono ancora problemi di lunghezza del messaggio
    def get_info_from_line(self, line: dict, split_str: str)-> dict:
        
        msg = line[self.LINE_MESSAGE]
        
        output_msg = ""
        msg = msg.strip()
        if len(msg) < 8100:
            output_msg = msg
        elif msg.endswith('\\'):
            output_msg = msg[:-1] + ' "}'
        else:
            output_msg = msg + ' "}'
        
        msg = output_msg
        
        try:
            msg_data = json.loads(msg.split(split_str)[1].strip())
        except Exception as e:
            self.logger.error(f"[log-analyzer][get_info_from_line] Error parsing JSON from log message: {msg}")
            self.logger.error(f"[log-analyzer][get_info_from_line] Error parsing JSON from log message: {self.print_special_chars(msg[-10:])}")
            self.logger.error(f"[log-analyzer][get_info_from_line] Exception: {e}")
            raise Exception("Error parsing JSON from log message")
        
        msg_data[self.TIMESTAMP] = line[self.LINE_TIMESTAMP]
        return msg_data

    def get_interval_s(self, start_ts, end_ts):
        return (end_ts - start_ts).total_seconds()

    def get_provider_id(self, data: dict):
        if self.ORCLOG_PROVIDER_NAME in data and \
                self.ORCLOG_PROVIDER_REGION in data:
            str_key = '_'.join([
                data[self.ORCLOG_PROVIDER_NAME],
                data[self.ORCLOG_PROVIDER_REGION]
            ]).lower()
            return str_key
        else:
            self.logger.debug(f"[log-analyzer][get_provider_id] It is not possible create key from obj: {data}")
            self.logger.debug(f"[log-analyzer][get_provider_id] Missed {self.ORCLOG_PROVIDER_NAME} and/or {self.ORCLOG_PROVIDER_REGION} keys")
            return None

    def init_state_dep(self, msg_data: dict):
        return {self.UUID: msg_data[self.ORCLOG_UUID],
                self.CREATION_DATE: msg_data[self.TIMESTAMP],
                self.STATUS: self.STATUS_SUBMITTED,
                self.STATUS_REASON: None,
                self.N_FAILURES: 0,
                self.TOT_FAILURE_TIME: 0,
                self.COMPL_TIME: 0,
                self.LAST_SUBMITTION_DATE: msg_data[self.TIMESTAMP],
                self.PROVIDER_ID: self.get_provider_id(msg_data)
            }

    def store_depl_status(self, data: dict):
        key = '_'.join([
                data[self.PROVIDER_ID],
                data[self.UUID]
            ]).lower()
        self.logger.debug(f"[log-analyzer][store_dep_status] Generated key: {key}")
        if key in self.data:
            self.logger.warn("[log-analyzer][store_dep_status] Deployment status already in the data. Overwrite")
        else:
            self.logger.info("[log-analyzer][store_dep_status] Added deployment in data")
        self.data[key] = data.copy()

    def update_sub_event(self, msg) -> bool:
        self.detected_events += 1
        msg_data = self.get_info_from_line(msg, self.ORCLOG_SUBMISSION_LINE)
        uuid = msg_data[self.UUID]
        self.logger.info(f"[log-analysis][update_sub_event] Detected Submission event for uuid: {uuid}")
        if uuid not in self.depl_status:
            self.depl_status[uuid] = self.init_state_dep(msg_data)
            self.logger.debug("[log-analysis][update_sub_event] Added to deployment status")
        else:
            prov_id = self.get_provider_id(msg_data)
            if prov_id != self.depl_status[uuid][self.PROVIDER_ID]:
                self.logger.debug("[log-analysis][update_sub_event] Submission event on a differnt provider than last stored")
                # E' stato sottomesso su un altro provider...
                if self.depl_status[uuid][self.STATUS] == self.STATUS_FAILED:
                    self.logger.debug("[log-analysis][update_sub_event] Last status deployment is FAILED. Store last data in DB and init new one.")
                    # ... dopo che e' stato registrato un errore in un tentativo passato
                    # I dati relativi al vecchio provider possono essere raccolti e spediti
                    self.store_depl_status(self.depl_status[uuid]) 
                    self.depl_status[uuid] = self.init_state_dep(msg_data)
                    return True
                else:
                    # ... dopo uno stato sconosciuto/completed/submitted. 
                    # In ogni caso si considera il passato indefinito e si sovrascrive.
                    self.depl_status[uuid] = self.init_state_dep(msg_data)
                    self.logger.debug("[log-analysis][update_sub_event] Last status is UNKNOWN/COMPLETED/SUBMITTED. Should never happens. Init new one")
                    return False
            else:
                # il sistema sta sottomettendo nuovamente sullo stesso provider
                # Aggiorno il timestamp relativo all'ultima sottomissione e 
                # resetto i campi status e status_reason
                self.depl_status[uuid][self.LAST_SUBMITTION_DATE] = msg_data[self.TIMESTAMP]
                self.depl_status[uuid][self.STATUS] = self.STATUS_SUBMITTED
                self.depl_status[uuid][self.STATUS_REASON] = None
                self.logger.debug("[log-analysis][update_sub_event] Updating the information on the provider.")
                return False

    def update_completed_event(self, msg):
        self.detected_events += 1
        msg_data = self.get_info_from_line(msg, self.ORCLOG_COMPLETED_LINE)
        uuid = msg_data[self.UUID]
        self.logger.info(f"[log-analysis][update_completed_event] Detected Completed event for uuid: {uuid}")
        if uuid in self.depl_status: # and prov_id == self.depl_status[uuid][self.PROVIDER_ID]:
            # Evento di CREATE_COMPLETED dopo un CREATE_IN_PROGRESS sullo stesso provider
            # L'unico che dovrebbe accadere
            if self.depl_status[uuid][self.STATUS] == self.STATUS_SUBMITTED:
                self.depl_status[uuid][self.STATUS] = self.STATUS_COMPLETED
                self.depl_status[uuid][self.COMPL_TIME] = self.get_interval_s(self.depl_status[uuid][self.CREATION_DATE],
                                                                              msg_data[self.TIMESTAMP])
                # Trasmetti le informazioni riguardo il deployment completato
                # dato che non ce ne saranno piu' sullo stesso provider_id
                self.store_depl_status(self.depl_status[uuid]) # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                # Cancella 
                del self.depl_status[uuid]
                return True
            else:
                # In tutti gli altri casi l'evento non sara' considerato 
                # Perche' e' accettato solo dopo un evento di sottomissione
                self.logger.info("[log-analysis][update_completed_event] "
                                 "Detected a weird state of the last deployment event different from SUBMITTED."
                                 f"Last status: {self.depl_status[uuid][self.STATUS]}")
                return False
        else:
            # Se non e' presente l'uuid del deployment vuol dire che non c'e'
            # alcuna informazione sulla sottomissione, quindi e' da scartare
            #self.logger.info("[log-analysis][update_completed_event] Event refers to a uuid and provider/region not stored. Rejected")
            self.logger.info("[log-analysis][update_completed_event] Event refers to a uuid not stored. Rejected")
            return False

    def update_error_event(self, msg):
        self.detected_events += 1
        msg_data = self.get_info_from_line(msg, self.ORCLOG_ERROR_LINE)
        uuid = msg_data[self.UUID]
        self.logger.info(f"[log-analysis][update_error_event] Detected Error event for uuid: {uuid}")
        final_error = True if self.ORCLOG_ERROR_SUMMARY_LINE in msg_data[self.STATUS_REASON] else False
        if uuid in self.depl_status:
            if final_error:
                # Se qui, allora il messaggio di errore e' quello riassuntivo.
                # si prevede che gia' un altro errore sia stato registrato per 
                # il deployment corrente. Altrimenti si considerera' questo come 
                # ultimo errore relativo a quel prov_id
                
                if self.depl_status[uuid][self.STATUS] == self.STATUS_FAILED:
                    # Questo messaggio di errore dovrebbe essere mostrato dopo un altro
                    # messaggio di errore piu' specifico, quindi dovrebbe trovare come stato
                    # CREATE_FAILED. In tal caso, sovrascrive la ragione dell'errore con 
                    # quella riassuntiva di tutti i tentativi e spedisce le metriche raccolte
                    self.depl_status[uuid][self.STATUS_REASON] = msg_data[self.ORCLOG_STATUS_REASON]
                    self.store_depl_status(self.depl_status[uuid]) # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                    # Cancella ?
                    del self.depl_status[uuid]
                    self.logger.info("[log-analyzer][update_error_event] Final Error after Error event. Trigger sending to Kafka.")
                    return True
                
                elif self.depl_status[uuid][self.STATUS] == self.STATUS_SUBMITTED:
                    # Questo vuol dire che non ci sono stati eventi di errori precedenti
                    # Sara' considerato questo come evento sia di errore che si fine di 
                    # tentativi su un dato provider_id
                    self.depl_status[uuid][self.STATUS] = self.STATUS_FAILED
                    self.depl_status[uuid][self.STATUS_REASON] = msg_data[self.ORCLOG_STATUS_REASON]
                    self.depl_status[uuid][self.TOT_FAILURE_TIME] += self.get_interval_s(self.depl_status[uuid][self.LAST_SUBMITTION_DATE],
                                                                                        msg_data[self.TIMESTAMP])
                    self.depl_status[uuid][self.N_FAILURES] += 1
                    self.store_depl_status(self.depl_status[uuid])  # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                    # Cancella ?
                    del self.depl_status[uuid]
                    self.logger.info("[log-analyzer][update_error_event] Final Error after Submission event. Trigger sending to Kafka.")
                    return True
                else:
                    self.logger.info("[log-analyzer][update_error_event] Final Error after Completed or unknown event. Rejected.")
                    return False
            else:        
                if self.depl_status[uuid][self.STATUS] == self.STATUS_SUBMITTED:
                    # La procedura prevede che lo stato precedente sia quello di sottomissione. 
                    # In quel caso vanno aggiornate le metriche.
                    self.depl_status[uuid][self.STATUS] = self.STATUS_FAILED
                    self.depl_status[uuid][self.STATUS_REASON] = msg_data[self.ORCLOG_STATUS_REASON]
                    self.depl_status[uuid][self.TOT_FAILURE_TIME] += self.get_interval_s(self.depl_status[uuid][self.LAST_SUBMITTION_DATE],
                                                                            msg_data[self.TIMESTAMP])
                    self.depl_status[uuid][self.N_FAILURES] += 1    
                    self.logger.info("[log-analyzer][update_error_event] Error after Submission event. Update information.")
                    return False
                
                elif self.depl_status[uuid][self.STATUS] == self.STATUS_FAILED:
                    # Se qui, un messaggio di errore e' stato ricevuto dopo un altro. 
                    # In questo caso vanno aggiornate tutte le metriche tranno n_failure
                    self.depl_status[uuid][self.STATUS_REASON] = msg_data[self.ORCLOG_STATUS_REASON]
                    self.depl_status[uuid][self.TOT_FAILURE_TIME] += self.get_interval_s(self.depl_status[uuid][self.LAST_SUBMITTION_DATE],
                                                                            msg_data[self.TIMESTAMP])
                    self.logger.info("[log-analyzer][update_error_event] Error after Error event. Update information.")
                    return False
                elif self.depl_status[uuid][self.STATUS] == self.STATUS_COMPLETED:
                    # Se qui, un messaggio di errore e' stato ricevuto dopo un messaggio di CREATE_COMPLETED
                    # Questo non dovrebbe mai accadere sia perche' manca l'evento di sottomissione, sia perche'
                    # dopo un evento CREATE_COMPLETED non si aspetta nessun'altro evento. Sara' rigettato
                    self.logger.info("[log-analyzer][update_error_event] Error after Completed event. Rejected.")
                    return False
                else:
                    # Se qui, nessuno dei test precedenti e' andato a buon fine, quindi verra' scartato
                    self.logger.info("[log-analyzer][update_error_event] Error after Completed event. Rejected.")
                    return False
        else:
            self.logger.info("[log-analyzer][update_error_event] "
                             "Event refers to a uuid not stored. Rejected")
            #"Event refers to a uuid and provider/region not stored. Rejected")
            return False
        
    def keys(self) -> set:
        return set(self.data.keys())
    
    def get(self, key) -> dict:
        return self.data.get(key, None)
    
    def timestamp(self, ts_str):
        dt_utc = datetime.strptime(ts_str, self.ORCLOG_FILEBEAT_TS_FORMAT )
        dt_utc = dt_utc.replace(tzinfo=pytz.utc)

        italy_tz = pytz.timezone("Europe/Rome")
        dt_local = dt_utc.astimezone(italy_tz)
        return dt_local
    
    def import_line(self, line) -> bool:
        try:
            dec_line = json.loads(line)
        except json.JSONDecodeError:
            self.logger.error(f"[log-analyzer][import_line] Line is not a valid JSON message: {line}")
            return False   
        except KeyError:
            self.logger.error(f"[log-analyzer][import_line] Line does not contain 'message' key: {line}")
            return False
        except Exception as e:
            self.logger.error(f"[log-analyzer][import_line] Unexpected error while processing line: {line}")
            self.logger.error(f"[log-analyzer][import_line] Error: {e}")
            return False
        else:
            line = {
                self.LINE_MESSAGE: dec_line['message'],
                self.LINE_TIMESTAMP: self.timestamp(dec_line['@timestamp'])
            }
            
        if self.is_line_to_reject(line):
            return False

        # SUBMISSION EVENT
        if self.is_submission_event(line):
            return self.update_sub_event(line)
            
        elif self.is_completed_event(line): # COMPLETED EVENT
            return self.update_completed_event(line)
            
        elif self.is_error_event(line): # ERROR EVENT
            return self.update_error_event(line)
            
        return False