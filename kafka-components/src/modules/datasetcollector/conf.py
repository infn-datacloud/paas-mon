# # Internal variables
INT_CREATION_DATE = 'creation_date' # submission date
INT_STATUS = 'status'
INT_STATUS_REASON = 'status_reason'
INT_N_FAILURES = 'n_failures'
INT_TOT_FAILURE_TIME = 'tot_failure_time_s'
INT_COMPL_TIME = 'completetion_time_s'
INT_LAST_SUBMITTION_DATE = 'last_sub_date'
INT_PROVIDER_ID = 'provider_id'
INT_UUID = 'uuid'

# AI-Ranker Inference Constants
ARI_PROVIDERS   = 'providers'
ARI_PROV_NAME   = 'provider_name'
ARI_REG_NAME    = 'region_name'
ARI_MSG_VERSION = 'msg_version'
ARI_TEMPL_NAME  = 'template_name'
ARI_USER_GROUP  = 'user_group'
ARI_UUID        = 'uuid'
ARI_FIELD_TO_COPY = [
    ARI_MSG_VERSION,
    ARI_TEMPL_NAME,
    ARI_USER_GROUP,
    ARI_UUID
]

ARI_FIELD_TO_KEY = [
    ARI_UUID,
    ARI_PROV_NAME,
    ARI_REG_NAME
]

ART_COMPL_TIME = 'completetion_time_s'
ART_CREATION_DATE = 'submission_time'
ART_N_FAILURE = 'n_failures'
ART_STATUS = 'status'
ART_STATUS_REASON = 'status_reason'
ART_TOT_FAILURE_TIME = 'tot_failure_time_s'

ART_FIELDS_TO_COPY = [
    (ART_COMPL_TIME, INT_COMPL_TIME),
    (ART_CREATION_DATE, INT_CREATION_DATE),
    (ART_N_FAILURE, INT_N_FAILURES),
    (ART_STATUS, INT_STATUS),
    (ART_STATUS_REASON, INT_STATUS_REASON),
    (ART_TOT_FAILURE_TIME, INT_TOT_FAILURE_TIME),
]


LINES_TO_REJECT = [
    'it.reply.orchestrator.exception.service.DeploymentException: Error executing request to IM'
]
