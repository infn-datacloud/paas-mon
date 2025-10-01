import json
    
class TemplateParserMessage:
    """
    Class to handle template parsing messages.
    It provides methods to extract information from log lines, validate templates,
    and manage user parameters.
    """
    
    MSG_PARAM_ERROR1 = "User did not provide the parameter, not default provided and parameter not required."
    MSG_PARAM_ERROR2 = "User did not provide the parameter and parameter required."
    MSG_PARAM_ERROR3 = "No constraint collected"
    MSG_NO_ERR = ""
    
    TEMPL_MSG_VERSION = 'msg_version'
    TEMPL_MSG_VERSION_VALUE = '1.0'
    
    TEMPL_TEMPL_NAME = 'template_name'
    TEMPL_TEMPL_NAME_DEFAULT = 'default_template_name'
    TEMPL_METADATA = 'metadata'
    TEMPL_TOPOLOGY_TEMPL = 'topology_template'
    TEMPL_NODE_TEMPL = 'node_templates'
    TEMPL_INPUTS = 'inputs'
    TEMPL_USER_PARAMS = 'user_parameters'
    
    PARAM_DEFAULT = 'default'
    PARAM_REQUIRED = 'required'
    PARAM_REQUIRED_TRUE = True
    PARAM_REQUIRED_FALSE = False
    PARAM_CONSTRAINTS = 'constraints'
    PARAM_VALID_VALUES = 'valid_values'
    PARAM_N_CPUS = 'num_cpus'
    PARAM_MEM = 'mem_size'
    PARAM_PORTS = 'ports'
    PARAM_SERVICE_PORTS = 'service_ports'
    PARAM_TO_NOT_CHECK = [PARAM_N_CPUS,PARAM_MEM,PARAM_PORTS]
    
    TEMPL_USER_GROUP = 'user_group'
    TEMPL_UUID = 'uuid'
    TEMPL_TIMESTAMP = 'timestamp'
    TEMPL_GET_INPUT = 'get_input'
    TEMPL_TOPOLOGY_TEMPL = 'topology_template'
    TEMLP_INPUTS = 'inputs'
    TYPE_FIELD = 'type'
    TYPE_INTEGER = 'integer'
    TYPE_SCALAR_UNIT_SIZE = "scalar-unit.size"
    TYPE_STRING = 'string'
    TYPE_VERSION = 'version'

    TYPES_TO_INTEGER = [TYPE_INTEGER]
    TYPES_TO_STRING = [TYPE_SCALAR_UNIT_SIZE,
                       TYPE_STRING,
                       TYPE_VERSION]
    USE_CONSTRAINTS = False
    
    def __init__(self, enriched_template: str, logger=None):
        self.logger = logger
        self.enriched_template = enriched_template
        self.get_validated_template()
        
    def get_dict(self) -> dict:
        """
        Get the validated template.
        
        Returns:
            dict: The validated template.
        """
        return self.template.copy()
    
    def get_template_serialized(self) -> str:
        """
        Get the validated template serialized as a string.
        
        Returns:
            str: The serialized validated template.
        """
        return json.dumps(self.template, sort_keys=True)
    
    def get_uuid(self) -> str:
        """
        Get the UUID of the template.
        
        Returns:
            str: The UUID of the template if available, otherwise None.
        """
        return self.template.get(self.TEMPL_UUID, None)
    
    def _find_get_input(self, var):
        if isinstance(var, dict):
            for k,v in var.items():
                if isinstance(v,dict) and self.TEMPL_GET_INPUT in v:
                    input_var = v[self.TEMPL_GET_INPUT]
                    input_value = self.template[self.TEMPL_TOPOLOGY_TEMPL][self.TEMLP_INPUTS][input_var]
                    var[k] = input_value
                else:
                    self._find_get_input(v)
        if isinstance(var, list):
            for el in var:
                self._find_get_input(el)
    
    def get_template_name(self) -> None:
        """
        Get the template name from the enriched template.
        
        Returns:
            str: Template name if available, otherwise None.
        """
        self.template[self.TEMPL_TEMPL_NAME] = None 
        if self.TEMPL_METADATA in self.enriched_template and self.TEMPL_TEMPL_NAME in self.enriched_template[self.TEMPL_METADATA]:
            template_name = self.enriched_template[self.TEMPL_METADATA][self.TEMPL_TEMPL_NAME]
            self.template[self.TEMPL_TEMPL_NAME] = template_name
            # self.logger.debug(f"Template name found: {template_name}")
        else:
            self.template[self.TEMPL_TEMPL_NAME] = self.TEMPL_TEMPL_NAME_DEFAULT
            self.logger.debug(f"Template name not found, used the default one: {self.TEMPL_TEMPL_NAME_DEFAULT}")
    
    def init_template(self) -> None:
        """
        Initialize the template with basic information such as version and template name.
                
        Returns:
            None
        """
        self.template = self.enriched_template.copy()
        self.template[self.TEMPL_MSG_VERSION] = self.TEMPL_MSG_VERSION_VALUE
        self.get_template_name()
    
    # Extract parameter type from template 
    def get_param_type(self, param_obj):
        """ 
        Extract the type of a parameter from the parameter object.   
        
        Args:
            param_obj (dict): The parameter object containing type information.     
        Returns:
            type: The type of the parameter (int, str) or None if not specified.
        """
        
        if self.TYPE_FIELD in param_obj:
            param_type = param_obj[self.TYPE_FIELD]
            if param_type in self.TYPES_TO_INTEGER: 
                return int
            elif param_type in self.TYPES_TO_STRING: 
                return str
        return None

    def cast_param(self, param, param_type):
        return param_type(param) if param_type else param

    def get_default_value(self, param_obj: dict) -> bool:
        return self.PARAM_DEFAULT in param_obj

    def get_required_value(self, param_obj: dict) -> bool | None:
        return param_obj.get(self.PARAM_REQUIRED, None)

    def get_error_user_not_in_valids(self, user_parameter, valid_values):
        msg =  f"{user_parameter=}({type(user_parameter)=}) not "
        msg += f"in {valid_values=}({type(valid_values[0])=})"
        return msg

    # Check constraints, default and value type
    def get_param(self, param_obj, user_parameter, use_constraints=False):
        
        # Extract parameter type from template parameter definition
        param_type = self.get_param_type(param_obj)
        
        # If user did not inserted the parameter
        if not user_parameter:
            param_default = self.get_default_value(param_obj)
            param_required = self.get_required_value(param_obj)

            if param_default == False and param_required == False: # Suggestion to not approve
                return None, self.MSG_PARAM_ERROR1 
            elif param_default == False or param_required == True: # Suggestion to not approve
                return None, self.MSG_PARAM_ERROR2 
            else:
                # Consider default value
                value = self.cast_param(param_obj[self.PARAM_DEFAULT], param_type)
                return value, self.MSG_NO_ERR
            
        # If here, user provided a parameter

        # Cast the value to the type stated in the template
        user_parameter = self.cast_param(user_parameter, param_type)

        if use_constraints:
            valid_values = None         # Init valid_values list

            # Collect "valid_values" field
            self.logger.debug(param_obj[self.PARAM_CONSTRAINTS])
            for constr_el in param_obj[self.PARAM_CONSTRAINTS]:
                for constr_k, constr_obj in constr_el.items():
                    if constr_k == self.PARAM_VALID_VALUES:
                        valid_values = [self.cast_param(elem, param_type) for elem in constr_obj]

            if valid_values:
                if user_parameter in valid_values:
                    # If the user parameter belongs to "valid_values" list, returns it
                    return user_parameter, self.MSG_NO_ERR
                else:
                    # If not, report
                    return None, self.get_error_user_not_in_valids(user_parameter, valid_values)
            else:
                return user_parameter, self.MSG_NO_ERR
                # If here, the user parameter is provided and the checking on constraints must be done
                # but no "valid_values" list has been imported. To report.
        else:
            # No constrain 
            return user_parameter, self.MSG_NO_ERR
            
    # Merge user parameters and template defualt and requirements
    def get_validated_template(self):
        
        self.init_template()
        
        # Validate and merge parameters (user and default parameters, constraints and required)
        for param_key, param_obj in self.enriched_template[self.TEMPL_TOPOLOGY_TEMPL][self.TEMLP_INPUTS].items():
            user_param = self.enriched_template[self.TEMPL_USER_PARAMS].get(param_key, None)
            is_param_to_check = param_key not in self.PARAM_TO_NOT_CHECK
            to_constraint = self.USE_CONSTRAINTS and self.PARAM_CONSTRAINTS in param_obj and is_param_to_check
            param, err_msg = self.get_param(param_obj, user_param, to_constraint)
            if err_msg:
                self.logger.error(f"Error during the validation of '{param_key}' parameter. Message: {err_msg}") 
            else:
                self.template[self.TEMPL_TOPOLOGY_TEMPL][self.TEMLP_INPUTS][param_key] = param

        try:
            for _, node_data in self.template[self.TEMPL_TOPOLOGY_TEMPL][self.TEMPL_NODE_TEMPL].items():
                self._find_get_input(node_data)
        except KeyError as e:
            self.logger.error(f"KeyError during template processing: {e}")
        else:
            del self.template['user_parameters'] 