import json
import yaml
from datetime import datetime
from sys import exit
import pytz

class LogOrchestratorCollector:
    """
    Class to manage the state of the log orchestrator.
    It provides methods to check if a line is the start of a template collection,
    if a line should be rejected, and to extract information from log lines.
    """
    
    START_TEMPLATE_STRING = ": Creating deployment with template"
    EVENT_TEMPLATE_SEP = " i.r.o.service.DeploymentServiceImpl      : "
    EVENT_TEMPLATE_STRING = " i.r.o.service.DeploymentServiceImpl      : {"
    
    INFO_TEMPLATE_STRING = "{\"uuid\""  # JSON string containing the UUID   
    # LOG_SEP = "]: "  # Separator for log lines 
    TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"  # Format for syslog timestamp
    SYSLOG_TS_FORMAT = "%Y-%m-%dT%H:%M:%S%z"  # Format for syslog timestamp
    LOG_FILTER = " paas-orchestrator orchestrator/"  # Filter for log lines to reject
    
    LOG_INPUT_FORMAT = "%b %d %H:%M:%S %Y"
    LOG_LOCAL_TIMEZONE = pytz.timezone("Europe/Rome")
    LOG_SEP_DEFAULT = 'paas-orchestrator orchestrator/'
    LOG_SEP_KEY = "log_sep"
    KEY_TIMESTAMP = 'timestamp'
    
    def __init__(self, settings, logger):
        self.collect = False  # Flag to indicate if template collection is in progress
        self.str_template = []  # List to collect template lines
        self.logger = logger  # Logger instance for logging messages
        self.collect_template = False  # Flag to indicate if template collection is in progress
        self.str_template = []
        self.received_user_parameters = False  # Flag to indicate if user parameters have been received
        
        self.logger.info(f"{settings.keys()}")
        
        if hasattr(settings, self.LOG_SEP_KEY):
            self.LOG_SEP = settings.LOG_SEP
        else:
            self.LOG_SEP = self.LOG_SEP_DEFAULT
        self.logger.info(f"Using log separator: {self.LOG_SEP}")
    
    # Parse timestamp
    def extract_timestamp(self, line) -> tuple[datetime, datetime]:
        """
        Extracts the syslog timestamp and the ORC timestamp from a log line.           
        Args:
            line (str): The log line to parse.  
        Returns:    
            tuple: A tuple containing the syslog timestamp and the ORC timestamp.
                   Returns (None, None) if the line is invalid or does not contain timestamps.
        """
        if not line:
            self.logger.error("Line is empty or None.")
            return None, None
        if isinstance(line, str) and not line.strip():
            self.logger.error("Line is an empty string.")
            return None, None
        if self.LOG_SEP not in line:
            self.logger.error(f"Line does not contain a valid log separator: {line}")
            return None, None
        if not line.split(self.LOG_SEP)[1]:
            self.logger.error(f"Line does not contain a valid log message: {line}")
            return None, None
        syslog_ts = datetime.strptime(line.split()[0], self.SYSLOG_TS_FORMAT)
        try:
            orc_ts_str = " ".join(line.split(self.LOG_SEP)[1].split()[0:2])
            orc_ts = datetime.strptime(orc_ts_str, self.TS_FORMAT)
        except (ValueError, IndexError):
            orc_ts = None
        return syslog_ts, orc_ts

    def is_start_to_collect(self, ts, line) -> bool:
        """ 
        Checks if the line indicates the start of a template collection.    
        Args:
            ts (datetime): The timestamp extracted from the log line.
            line (str): The log line to check.  
        Returns:
            bool: True if the line indicates the start of a template collection, False otherwise.
        """
        if not isinstance(line, str):
            self.logger.error("Line is not a valid string.")
            return False
        return ts and self.START_TEMPLATE_STRING in line

    def is_line_to_reject(self, line) -> bool:
        """ 
        Checks if the line should be rejected based on specific criteria.
        Args:
            line (str): The log line to check.  
        Returns:
            bool: True if the line should be rejected, False otherwise.
        """
        if not isinstance(line, str):
            self.logger.error("Line is not a valid string.")
            return True
        return self.LOG_FILTER not in line

    def is_user_parameter_line(self, line):
        """
        Checks if the line contains user parameters and metadata.
        Args:
            line (str): The log line to check.
        Returns:
            bool: True if the line contains user parameters and metadata, False otherwise.
        """
        if not isinstance(line, str):
            self.logger.error("Line is not a valid string.")
            return False
        return self.EVENT_TEMPLATE_STRING in line

    def extract_info(self, line) -> tuple [datetime, datetime, str]:
        """
        Extracts the syslog timestamp, ORC timestamp, and the log message from a log line.      
        Args:
            line (str): The log line to parse.
        Returns:
            tuple: A tuple containing the syslog timestamp, ORC timestamp, and the log message.
                   Returns (None, None, None) if the line is invalid or does not contain timestamps.
        """
        if not line:
            self.logger.error("Line is empty or None.")
            return None, None, None
        if isinstance(line, str) and not line.strip():
            self.logger.error("Line is an empty string.")
            return None, None, None
        syslog_ts = datetime.strptime(line.split()[0], self.SYSLOG_TS_FORMAT)
        pre_line = " ".join(line.split()[:3]) + " "
        line = line.split(pre_line)[1]
        try:
            orc_ts_str = " ".join(line.split()[0:2])
            orc_ts = datetime.strptime(orc_ts_str, self.TS_FORMAT)
        except (ValueError, IndexError):
            orc_ts = None
            
        return syslog_ts, orc_ts, line
    
    # Extract user parameter from json
    def extract_user_parameters(self, line: str) -> dict | None:
        """
        Extract user parameters from a log line.
        Args:
            line (str): Log line containing user parameters in JSON format.
        Returns:
            dict | None: Returns a dictionary of user parameters if successful, None otherwise.
        """
        if not line:
            self.logger.error("Line is empty or None.")
            return None
        if isinstance(line, str) and not line.strip():
            self.logger.error("Line is an empty string.")
            return None
        if self.EVENT_TEMPLATE_SEP not in line:
            self.logger.error(f"Line does not contain user parameters [1]: {line}")
            return None
        if not line.split(self.EVENT_TEMPLATE_SEP)[1]:
            self.logger.error(f"Line does not contain user parameters [2]: {line}")
            return None
        if not line.split(self.EVENT_TEMPLATE_SEP)[1].startswith("{"):
            self.logger.error(f"Line does not contain valid JSON user parameters [3]: {line}")
            return None 
        if not line.split(self.EVENT_TEMPLATE_SEP)[1].endswith("}"):
            self.logger.error(f"Line does not contain valid JSON user parameters [4] {line}")
            return None
        
        str_json = line.split(self.EVENT_TEMPLATE_SEP)[1]
        self.received_user_parameters = True
        try:
            return json.loads(str_json)
        except Exception as e:
            self.logger.error(f"Error extracting user parameters from line: {str_json}")
            self.logger.error(f"Error: {e}")
            return None
    
    # Validate YAML
    def import_template(self) -> dict | None:
        """
        Import the template from the collected log lines.

        Returns:
            dict | None: Returns the parsed template as a dictionary if successful, None otherwise.
        """
        try:
            template = yaml.safe_load("\n".join(self.str_template))
        except yaml.YAMLError as e:
            self.logger.debug(f"{json.dumps(self.str_template, indent=2, sort_keys=True)}")
            self.logger.error(f"Error importing template. YAML Error: {e}")
            return None
        except Exception as e:
            self.logger.debug(f"{json.dumps(self.str_template, indent=2, sort_keys=True)}")
            self.logger.error(f"Error importing template. Generic Error: {e}")
            return None
        else:
            template['is_automatic'] = "policies" not in template['topology_template'] 
            return template
    
    def convert_message(self, log) -> str:
        """
        Convert a log message to a standardized format.
        Args:
            log (str): The log message to convert.
        Returns:
            str: The converted log message in ISO 8601 format.
        """
        current_year = datetime.now().year
        self.logger.info(f"{current_year=}")
        log_timestamp_str = ' '.join(log.split()[0:3])
        self.logger.info(f"{log_timestamp_str=}")
        self.logger.info(f"{len(log.split(self.LOG_SEP))}")
        self.logger.info(f"{log.split(self.LOG_SEP)}")
        log_value = self.LOG_SEP + log.split(self.LOG_SEP)[1]
        self.logger.info(f"{log_value=}")
        full_timestamp_str = f"{log_timestamp_str} {current_year}"
        self.logger.info(f"{full_timestamp_str=}")
        dt_object = datetime.strptime(full_timestamp_str, self.LOG_INPUT_FORMAT)
        self.logger.info(f"{dt_object=}")
        localized_dt_object = self.LOG_LOCAL_TIMEZONE.localize(dt_object, is_dst=None)
        self.logger.info(f"{localized_dt_object=}")
        utc_dt_object = localized_dt_object.astimezone(pytz.utc)
        self.logger.info(f"{utc_dt_object=}")
        iso_format = utc_dt_object.strftime(self.SYSLOG_TS_FORMAT)
        self.logger.info(f"{iso_format=}")
        new_log_format = f"{iso_format[:-2]}:{iso_format[-2:]} {log_value}"
        self.logger.info(f"{new_log_format=}")
        return new_log_format

    def import_line(self, line: str) -> bool:
        """
        Finite state machine to process log lines and manage template collection.
        
        Args:
            line (str): Log line to process.
        
        Returns:
            bool: True if the line is processed successfully, False if it is not completed yet
        """
        
        try:
            message = json.loads(line)['message']
        except json.JSONDecodeError:
            self.logger.error(f"Line is not a valid JSON message: {line}")
            return False   
        except KeyError:
            self.logger.error(f"Line does not contain 'message' key: {line}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error while processing line: {line}")
            self.logger.error(f"Error: {e}")
            return False
        
        try:
            line = self.convert_message(message)
        except Exception as e:
            self.logger.error(f"Error converting log line: {message}")
            self.logger.error(f"Error: {e}")
            return False
        
        if self.is_line_to_reject(line): 
            self.logger.debug("Messages rejected")
            return False

        _ , ts, orc_log = self.extract_info(line)
        
        if self.is_start_to_collect(ts, orc_log):
            if not self.received_user_parameters:
                self.logger.debug("Not received user parameters in the last template")
            else:
                self.received_user_parameters = False

            self.collect_template = True
            self.logger.debug("Reset State and Start of template collection")
            self.str_template = []
            self.template = None
            self.depl_data = None
            return False
        
        if ts and self.collect_template:
            self.collect_template = False
        
        if not ts and self.collect_template:
            self.str_template.append(orc_log) 
        
        if self.is_user_parameter_line(orc_log):
            self.received_user_parameters = True
           
            self.depl_data = self.extract_user_parameters(line)
            if self.depl_data:
                self.depl_data[self.KEY_TIMESTAMP] = ts.strftime(self.TS_FORMAT)
            else:
                msg = "Error during the extraction of user parameters from the log line."
                self.logger.error(msg)
                self.logger.debug(f"Collected template lines: {line}")
                exit(0)
            
            self.template = self.import_template()
            if not self.template:
                msg = "Error during the import of the template from the log line."
                self.logger.error(msg)
                self.logger.debug(f"Collected template lines: {self.str_template}")
            return True
        return False
    
    def get_template_and_user_parameters(self) -> dict | None:
        """
        Get the template and user parameters from the collected log lines.
        
        Returns:
            tuple: (template, user_parameters) if successful, (None, None) if not.
        """
        if self.template and self.depl_data:
            return self.template | self.depl_data
        else:
            self.logger.error("Template or user parameters not available.")
            self.logger.debug(f"Template: {self.template}, User Parameters: {self.depl_data}")
            return None