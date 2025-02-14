import json
from modules import training_processor_conf as tpc
from modules import kafka_module as km
from datetime import datetime

# Data structures
training_sent = list()
infer_msgs = dict()
training = list()
depl_status = dict()

# Functions
def is_line_to_reject(line):
    return tpc.ORCLOG_ORCHESTARTOR_TAG not in line or \
           any(item in line for item in tpc.LINES_TO_REJECT)

def is_submission_event(msg):
    return tpc.ORCLOG_SUBMISSION_LINE in msg

def is_completed_event(msg):
    return tpc.ORCLOG_COMPLETED_LINE in msg

def is_error_event(msg):
    return tpc.ORCLOG_ERROR_LINE in msg

def get_key(obj):
    str_key = "-".join((v for k,v in obj.items() 
                        if k in sorted(tpc.ARI_FIELD_TO_KEY)))
    return str_key.lower()

# Import training messages
def import_ai_ranker_training_msg(msg_json:dict):
    t_key = get_key(msg_json)
    training_sent.append(t_key)
    
def import_ai_ranker_inference_msg(ari_json:dict):
    if isinstance(ari_json,str):
        ari_json = json.loads(ari_json)
    for ari_prov_data in ari_json[tpc.ARI_PROVIDERS]:
        ari_dict = ari_prov_data.copy()
        ari_dict.update({ 
            k:v for k,v in ari_json.items()
            if k in tpc.ARI_FIELD_TO_COPY 
            })
        uuid_key = get_key(ari_dict)
        infer_msgs[uuid_key] = ari_dict 
        km.write_log(uuid=uuid_key, status="AI_RANKER_NEW_MSG", msg="Added new deployment-provider-region")

def get_info_from_line(msg:str, split_str:str)-> dict:
    msg = msg if len(msg) < 8100 else msg.strip() + '"}'
    msg_data = json.loads(msg.split(split_str)[1].strip())
    syslog_ts = str(msg.split(tpc.ORCLOG_ORCHESTARTOR_TAG)[0]).strip()
    msg_data[tpc.INT_TIMESTAMP] = syslog_ts
    return msg_data

def get_interval_s(start_ts, end_ts):
    end_ts =  datetime.strptime(end_ts, tpc.ORCLOG_SYSLOG_TS_FORMAT)
    start_ts = datetime.strptime(start_ts, tpc.ORCLOG_SYSLOG_TS_FORMAT)
    return (end_ts - start_ts).total_seconds()

def get_provider_id(data: dict):
    if tpc.ORCLOG_PROVIDER_NAME in data and \
       tpc.ORCLOG_PROVIDER_REGION in data:
        str_key = f"{data[tpc.ORCLOG_PROVIDER_NAME]}-"
        str_key += f"{data[tpc.ORCLOG_PROVIDER_REGION]}"
        return str_key
    else:
        return None

def init_state_dep(msg_data: dict):
    return {tpc.INT_UUID: msg_data[tpc.ORCLOG_UUID],
            tpc.INT_CREATION_DATE: msg_data[tpc.INT_TIMESTAMP],
            tpc.INT_STATUS: tpc.STATUS_SUBMITTED,
            tpc.INT_STATUS_REASON: None,
            tpc.INT_N_FAILURES: 0,
            tpc.INT_TOT_FAILURE_TIME: 0,
            tpc.INT_COMPL_TIME: 0,
            tpc.INT_LAST_SUBMITTION_DATE: msg_data[tpc.INT_TIMESTAMP],
            tpc.INT_PROVIDER_ID: get_provider_id(msg_data)
           }

def send_msg(data: dict):
    uuid_key = f"{data[tpc.INT_PROVIDER_ID]}-{data[tpc.INT_UUID]}".lower()
    uuid = data[tpc.INT_UUID]
    if uuid_key in infer_msgs:
        output_msg = infer_msgs[uuid_key]
        output_msg.update({
                art_k:data[o_k] for art_k,o_k in tpc.ART_FIELDS_TO_COPY
            })
        if get_key(output_msg) not in training_sent:
            km.write_output_topic_kafka(output_msg)
            km.write_log(uuid=uuid, status=tpc.LOG_STATUS_OK_SENT, msg=tpc.LOG_STATUS_COLLECTED_AND_SENT)
            import_ai_ranker_training_msg(output_msg)
        else:
            km.write_log(uuid=uuid, status=tpc.LOG_STATUS_OK_NOT_SENT, msg=tpc.LOG_STATUS_COLLECTED)
    else:
        km.write_log(uuid=uuid, status=tpc.LOG_STATUS_NOT_UUID_FOUND, msg=tpc.LOG_STATUS_COLLECTED)
    
def update_sub_event(msg):
    global depl_status
    msg_data = get_info_from_line(msg, tpc.ORCLOG_SUBMISSION_LINE)
    uuid = msg_data[tpc.INT_UUID]
    km.write_log(timestamp=msg_data['timestamp'], msg=f"{tpc.LOG_SUBMISSION_EVENT}{uuid}", status=tpc.STATUS_SUBMITTED)
    if uuid not in depl_status:
        depl_status[uuid] = init_state_dep(msg_data)
    else:
        prov_id = get_provider_id(msg_data)
        if prov_id != depl_status[uuid][tpc.INT_PROVIDER_ID]:
            # E' stato sottomesso su un altro provider...
            if depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_FAILED:
                # ... dopo che e' stato registrato un errore in un tentativo passato
                # I dati relativi al vecchio provider possono essere raccolti e spediti
                send_msg(depl_status[uuid])
                depl_status[uuid] = init_state_dep(msg_data)
            elif depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_SUBMITTED:
                # ... dopo un'altra sottomissione dove non e' stato registrato l'esito 
                # Qui si sovrascrivera' lo stato del deployment
                depl_status[uuid] = init_state_dep(msg_data)
            elif depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_COMPLETED:
                # ... dopo un deployment avviato con successo
                # Non dovrebbe mai capitare, ma in tal caso, si sovrascrive, anche in
                # caso lo stato del deployment alla sottomissione
                depl_status[uuid] = init_state_dep(msg_data)
            else:
                # ... dopo uno stato sconosciuto. 
                # In ogni caso si considera il passato indefinito e si sovrascrive.
                depl_status[uuid] = init_state_dep(msg_data)
        else:
            # il sistema sta sottomettendo nuovamente sullo stesso provider
            # Aggiorno il timestamp relativo all'ultima sottomissione e 
            # resetto i campi status e status_reason
            depl_status[uuid][tpc.INT_LAST_SUBMITTION_DATE] = msg_data[tpc.INT_TIMESTAMP]
            depl_status[uuid][tpc.INT_STATUS] = tpc.STATUS_SUBMITTED
            depl_status[uuid][tpc.INT_STATUS_REASON] = None

def update_completed_event(msg):
    global depl_status
    msg_data = get_info_from_line(msg, tpc.ORCLOG_COMPLETED_LINE)
    uuid = msg_data[tpc.INT_UUID]
    km.write_log(timestamp=msg_data['timestamp'], msg=f"{tpc.LOG_SUCCESSFUL_EVENT}{uuid}", status=tpc.STATUS_COMPLETED)
    if uuid in depl_status:
        # Evento di CREATE_COMPLETED dopo un CREATE_IN_PROGRESS
        # L'unico che dovrebbe accadere
        if depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_SUBMITTED:
            depl_status[uuid][tpc.INT_STATUS] = tpc.STATUS_COMPLETED
            depl_status[uuid][tpc.INT_COMPL_TIME] = get_interval_s(depl_status[uuid][tpc.INT_CREATION_DATE],
                                                               msg_data[tpc.INT_TIMESTAMP])
            # Trasmetti le informazioni riguardo il deployment completato
            # dato che non ce ne saranno piu' sullo stesso provider_id
            send_msg(depl_status[uuid])

            # Cancella ?
            del depl_status[uuid]
        else:
            # In tutti gli altri casi l'evento non sara' considerato 
            # Perche' e' accettato solo dopo un evento di sottimissione
            pass
    else:
        # Se non e' presente l'uuid del deployment vuol dire che non c'e'
        # alcuna informazione sulla sottomissione, quindi e' da scartare
        pass

def update_error_event(msg):
    global depl_status
    msg_data = get_info_from_line(msg, tpc.ORCLOG_ERROR_LINE)
    uuid = msg_data[tpc.INT_UUID]
    km.write_log(timestamp=msg_data['timestamp'], msg=f"{tpc.LOG_ERROR_EVENT}{uuid}", status=tpc.STATUS_FAILED)
    final_error = True if tpc.ORCLOG_ERROR_SUMMARY_LINE in msg_data[tpc.INT_STATUS_REASON] else False
    if final_error:
        # Se qui, allora il messaggio di errore e' quello riassuntivo.
        # si prevede che gia' un altro errore sia stato registrato per 
        # il deployment corrente. Altrimenti si considerera' questo come 
        # ultimo errore relativo a quel prov_id
        
        if depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_FAILED:
            # Questo messaggio di errore dovrebbe essere mostrato dopo un altro
            # messaggio di errore piu' specifico, quindi dovrebbe trovare come stato
            # CREATE_FAILED. In tal caso, sovrascrive la ragione dell'errore con 
            # quella riassuntiva di tutti i tentativi e spedisce le metriche raccolte
            depl_status[uuid][tpc.INT_STATUS_REASON] = msg_data[tpc.ORCLOG_STATUS_REASON]
            send_msg(depl_status[uuid])

            # Cancella ?
            del depl_status[uuid]
        
        elif depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_SUBMITTED:
            # Questo vuol dire che non ci sono stati eventi di errori precedenti
            # Sara' considerato questo come evento sia di errore che si fine di 
            # tentativi su un dato provider_id
            depl_status[uuid][tpc.INT_STATUS] = tpc.STATUS_FAILED
            depl_status[uuid][tpc.INT_STATUS_REASON] = msg_data[tpc.ORCLOG_STATUS_REASON]
            depl_status[uuid][tpc.INT_TOT_FAILURE_TIME] += get_interval_s(depl_status[uuid][tpc.INT_LAST_SUBMITTION_DATE],
                                                                        msg_data[tpc.INT_TIMESTAMP])
            depl_status[uuid][tpc.INT_N_FAILURES] += 1
            send_msg(depl_status[uuid])

            # Cancella ?
            del depl_status[uuid]
            
    if uuid in depl_status:
        # Un evento di errore e' stato rilevato e il corrispondente uuid e' stato
        # registrato precedentemente. 

        if depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_SUBMITTED:
            # La procedura prevede che lo stato precedente sia quello di sottomissione. 
            # In quel caso vanno aggiornate le metriche.
            depl_status[uuid][tpc.INT_STATUS] = tpc.STATUS_FAILED
            depl_status[uuid][tpc.INT_STATUS_REASON] = msg_data[tpc.ORCLOG_STATUS_REASON]
            depl_status[uuid][tpc.INT_TOT_FAILURE_TIME] += get_interval_s(depl_status[uuid][tpc.INT_LAST_SUBMITTION_DATE],
                                                                      msg_data[tpc.INT_TIMESTAMP])
            depl_status[uuid][tpc.INT_N_FAILURES] += 1
        elif depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_FAILED:
            # Se qui, un messaggio di errore e' stato ricevuto dopo un altro. 
            # In questo caso vanno aggiornate tutte le metriche tranno n_failure
            depl_status[uuid][tpc.NT_STATUS_REASON] = msg_data[tpc.ORCLOG_STATUS_REASON]
            depl_status[uuid][tpc.INT_TOT_FAILURE_TIME] += get_interval_s(depl_status[uuid][tpc.INT_LAST_SUBMITTION_DATE],
                                                                      msg_data[tpc.INT_TIMESTAMP])
        elif depl_status[uuid][tpc.INT_STATUS] == tpc.STATUS_COMPLETED:
            # Se qui, un messaggio di errore e' stato ricevuto dopo un messaggio di CREATE_COMPLETED
            # Questo non dovrebbe mai accadere sia perche' manca l'evento di sottomissione, sia perche'
            # dopo un evento CREATE_COMPLETED non si aspetta nessun'altro evento. Sara' rigettato
            pass 
        else:
            # Se qui, nessuno dei test precedenti e' andato a buon fine, quindi verra' scartato
            pass
    else:
        # E' arrivato un messaggio di errore relativo non ancora registrato, quindi non essendoci un evento 
        # di sottomissione non si possono fare le considerazioni relative ad essa.
        pass