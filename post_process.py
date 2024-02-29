from raw_data_tools.parsing import HwCallDefLUT, HwCallDef, HwCall
from csv import reader
import gzip
import os

try:
    import pandas as pd
except ImportError:
    raise Exception('Pandas is required in order to run the post_processing tool on raw_data. Use the following link to find how to install Pandas on your system: https://pandas.pydata.org/docs/getting_started/install.html')


class FillMethods:
    previous_value = 'ffill'
    next_value = 'bfill'


class RawDataFile(HwCallDefLUT):

    def __init__(self, gz_data_path, max_stored_calls=None):
        super().__init__()
        self._calls_dict = {}
        self._dfs = {}
        self._max_stored_calls = max_stored_calls
        self.load_raw_data_file(gz_data_path)
        self.generate_data_frames()

    def add_call(self, new_call):

        calls = self._calls_dict.get((new_call.resource_name, new_call._hw_call_def.defining_triple), None)
        if calls:
            last_dict = calls[-1]
            new_dict = new_call.dictionary

            step_counter = 0 if last_dict['test_step'] != new_dict['test_step'] else last_dict['step_call_counter'] + 1
            new_dict.update({'step_call_counter': step_counter})
            calls.append(new_dict)
        else:
            new_dict = new_call.dictionary
            new_dict.update({'step_call_counter': 0})
            self._calls_dict.update({(new_call.resource_name, new_call._hw_call_def.defining_triple): [new_dict]})

        if self._max_stored_calls is not None:
            for call_ref, call_list in self._calls_dict.items():
                self._calls_dict[call_ref] = call_list[-self._max_stored_calls:]

    def load_raw_data_file(self, gz_data_path):
        with gzip.open(gz_data_path, 'rt', newline='') as f:
            data_log = reader(f)
            for line in data_log:
                if line[1] == 'hw_args':
                    new_def = HwCallDef(line)
                    self.add_def(new_def)  # add to/update LUT
                else:
                    defining_call = self.get_definition(line)
                    call = HwCall(line, defining_call)
                    self.add_call(call)

    def generate_data_frames(self):
        """returns a dictionary with all the data frames for each call def the LUT has seen since the last reset"""
        df_dict = {}

        for calls in self._calls_dict.values():
            # build the key name to easily reference the data frame later on
            first_entry = calls[0]
            dict_key = f"{first_entry['resource_name']}.{first_entry['accessor']}"
            if first_entry['access_type'] != 'function':
                dict_key += f"_{first_entry['access_type']}"

            # convert stored dict into a data frame
            df_dict[dict_key] = pd.DataFrame(calls)

        self._dfs = df_dict

    def write_csvs(self, save_folder, stream_names=None, file_names=None, include_metadata=False):

        # figure out what we're writing and where
        stream_names = stream_names if stream_names else self.available_streams
        if file_names is None:
            file_names = [f'{stream_name}.csv' for stream_name in stream_names]

        for stream_name, file_name in zip(stream_names, file_names):
            save_path = os.path.join(save_folder, file_name)
            raw_data_to_csv(self.streams[stream_name], save_path, include_metadata)

    def update_postpended_id(self, stream_name, id):
        """postpends the provided string to the column names of arguments and outputs\n
        existing name is separated by a a pipe '||' """

        def update_column_id(current_col_name, id):
            original_name_index = current_col_name.find('||')

            if original_name_index == -1:
                new_col_name = current_col_name + '||' + id
            else:
                new_col_name = current_col_name[:original_name_index] + '||' + id

            return new_col_name

        df = self.streams[stream_name]
        col_mapping = {col_name: update_column_id(col_name, id) for col_name in df.columns if col_name not in ['relative_sec', 'epoch_sec'] + HwCall.metadata_columns}
        df.rename(col_mapping, axis='columns', inplace=True)

    @property
    def streams(self):
        return self._dfs

    @property
    def available_streams(self):
        return list(self.streams)


def combine_raw_data(streams, fill_method=None, primary_stream_index=None):
    combined = pd.concat(streams).sort_values('epoch_sec')

    if fill_method:
        combined = getattr(combined, fill_method)()

    if primary_stream_index is not None:
        primary_df = streams[primary_stream_index]
        resource_name, accessor, access_type = [primary_df.loc[0].loc[id_col] for id_col in ['resource_name', 'accessor', 'access_type']]
        combined = combined[(combined['resource_name'] == resource_name) & (combined['accessor'] == accessor) & (combined['access_type'] == access_type)]

    return combined


def raw_data_to_csv(stream, save_path, include_metadata=False):

    def writeable_columns():
        """returns the list of columns to be written"""
        metadata_columns = HwCall.metadata_columns
        columns = stream.columns
        if include_metadata:
            return columns
        else:
            return [column for column in columns if column not in metadata_columns]

    stream.to_csv(save_path, columns=writeable_columns(), index=False)
