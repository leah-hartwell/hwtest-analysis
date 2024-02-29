class HwCallDef():
    """class to represent a hw call definition"""

    access_types = ['getter', 'setter', 'function']
    name_index = 0
    time_index = 2
    io_index = 4
    generic_output_def = 'hw_out'

    def __init__(self, def_list):
        """initialize a hardware call definition from a list corresponding the a definition entry of raw data"""
        self.def_strings = self.parse_call_strings(def_list[self.name_index])
        self.extract_io_defs(def_list[self.io_index:])
        self.relative_sec = float(def_list[self.time_index])
        self.log_t0 = None  # to be updated by LUT

    def extract_io_defs(self, io_def_list):
        """parse out input and output mapping from hardware"""

        self.has_name = io_def_list[0] == 'name'

        if self.has_name:
            io_def_list = io_def_list[1:]

        output_start = next((i for i, value in enumerate(io_def_list) if 'hw_out' in value), len(io_def_list))
        self.input_mapping = io_def_list[:output_start]
        self.output_mapping = self.parse_output_defs(io_def_list[output_start:])

    def parse_call_strings(self, def_string):
        """gets call info from the defining string; class, accessor and type"""

        # format is ClassName.accessor[_getter, _setter]
        def_names = def_string.split('.')
        try:
            resource_class = def_names[0]
            for access in self.access_types[:-1]:  # see if it is a getter or setter
                type_string = f'_{access}'
                if def_names[1].endswith(type_string):
                    accessor_name = def_names[1][:-len(type_string)]
                    access_type = access
                    break
            else:  # otherwise it must just be a plain function call
                accessor_name = def_names[1]
                access_type = self.access_types[-1]
        except IndexError:  # likely defined with @hw_wrapper so doesn't fit the expected format
            resource_class = ''
            access_type = self.access_types[-1]  # say it's a function
            accessor_name = def_names[0]  # give the provided info as the accessor name

        return {
            'resource_class': resource_class,
            'accessor': accessor_name,
            'type': access_type
        }

    def defines(self, hw_call_list):
        """compares a hw_call list to own definition to see if the defintion matches"""
        call_string = hw_call_list[self.name_index].split('__')[1]  # get def string from correct elemnt of hw_call
        return self.def_strings == self.parse_call_strings(call_string)  # run through def string parser to see if it matches

    @property
    def resource_class(self):
        return self.def_strings['resource_class']

    @property
    def accessor(self):
        return self.def_strings['accessor']

    @property
    def access_type(self):
        return self.def_strings['type']

    @property
    def defining_triple(self):
        return (self.resource_class, self.accessor, self.access_type)

    def __repr__(self) -> str:
        return 'Definition class of ' + self.accessor + ' ' + self.access_type + ' for ' + self.resource_class

    def parse_output_defs(self, output_def_list):
        """parses names of the output defs"""

        outputs = []
        for output_def in output_def_list:
            index_strings = output_def[len(self.generic_output_def):]  # remove hw_out from the start
            if not index_strings:  # if there's nothing left, the return is just a simple value and we can call it hw_out
                outputs.append(self.generic_output_def)
            else:  # otherwise we put the key names enclosed in brackets that are left after hw_out
                outputs.append(index_strings)

        return outputs


class HwCallDefLUT():
    """class to represent a look up table of the definitions seen so far"""

    def __init__(self):
        """initialize a lookup table for a new file"""
        self.reset()

    def reset(self):
        self._lut = []
        self.log_t0 = None

    def add_def(self, new_def):
        """add definition to LUT or replace/update if one has alaready been seen"""

        # set the file start time if the look up table is empty; the first line should always be a definition
        if not self._lut:
            self.log_t0 = new_def.relative_sec

        new_def.log_t0 = self.log_t0  # give the definition the start time of the file

        # figure out if there is already a def with the same def strings and either add or update with new def
        existing_index = next((index for index, class_def in enumerate(self._lut) if class_def.def_strings == new_def.def_strings), None)
        if existing_index:
            self._lut[existing_index] = new_def
        else:
            self._lut.append(new_def)

    def get_definition(self, call_list):
        """returns the HwCallDef that defines the provided call by checking the definitions in the LUT"""
        try:
            return next(call_def for call_def in self._lut if call_def.defines(call_list))
        except KeyError:
            raise Exception(f"No definition found for {call_list[0]}")


class HwCall(HwCallDef):
    """class representation of a hardware call as defined in raw data"""
    origin_index = 1
    duration_index = 3

    metadata_columns = [
        'duration_sec',
        'resource_name',
        'resource_class',
        'accessor',
        'access_type',
        'test_class',
        'test_step',
        'step_call_counter'
    ]

    def __init__(self, hw_call_list, hw_call_def):
        """Initialize a hardware call object from the call_list and corresponding hw_call_def"""
        self._hw_call_def = hw_call_def
        provided_resource_name = hw_call_list[self.name_index].split('__')[0]
        self.resource_name = provided_resource_name if provided_resource_name else 'NotProvided'
        self.parse_call_origin(hw_call_list[self.origin_index])
        self.def_strings = self._hw_call_def.def_strings  # def strings are the same as the hw_call_def

        # get time info relative
        self.epoch_sec = float(hw_call_list[self.time_index])
        self.relative_sec = self.epoch_sec - self._hw_call_def.log_t0
        self.duration_sec = float(hw_call_list[self.duration_index])

        self.parse_io(hw_call_list[self.io_index:])

    def parse_call_origin(self, origin_string):
        split_origin = origin_string.split('->')
        self.test_class = split_origin[0]
        if len(split_origin) > 1:
            self.test_step = split_origin[1]
        else:
            self.test_step = None

    def parse_io(self, io_list):
        """builds dictionary of input and output parameters based on io mapping from defintion"""

        if self._hw_call_def.has_name:
            io_list = io_list[1:]

        def parse_from_mapping(var_defs, var_list):
            """builds dictionary from parameter name mapping and supplied variables"""
            mapped_vars = {}
            for var_def, var in zip(var_defs, var_list):
                mapped_vars.update({var_def: self._convert_to_type(var)})

            return mapped_vars

        num_inputs = len(self._hw_call_def.input_mapping)  # io list needs to be separated into inputs and outputs
        self.inputs = parse_from_mapping(self._hw_call_def.input_mapping, io_list[:num_inputs])
        self.outputs = parse_from_mapping(self._hw_call_def.output_mapping, io_list[num_inputs:])

    @staticmethod
    def _convert_to_type(string_var):
        """convert a string into a float or int if possible"""
        try:  # try to convert to a number
            float_var = float(string_var)
            # only truncate to float if the float can be an integer AND doesn't have a '.' (e.g. you don't want to convert 10.0 into an integer)
            if float_var.is_integer() and '.' not in string_var:
                return int(float_var)
            else:
                return float_var
        except ValueError:  # if it can't be made into a float, it must be a string
            pass

        # return type as None instead of a string
        if string_var == 'None':
            return None
        else:
            return string_var

    @property
    def dictionary(self):
        basic_attributes = ['epoch_sec', 'relative_sec'] + self.metadata_columns

        call_dict = {}
        for attribute in basic_attributes:
            call_dict.update({attribute: getattr(self, attribute, -1)})

        for input_name, input_val in self.inputs.items():
            call_dict.update({f'arg:{input_name}': input_val})

        for output_name, output_val in self.outputs.items():
            dict_key = self.accessor if output_name == self.generic_output_def else (self.accessor + output_name)
            call_dict.update({dict_key: output_val})

        return call_dict

    @property
    def definition(self):
        return self._hw_call_def
