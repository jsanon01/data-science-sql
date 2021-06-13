from io import TextIOWrapper
import re

class CSVPreprocessor(TextIOWrapper):
    def read(self, size):

        # true if the first row is being processed
        trim_cols = self.tell() == 0

        # read data up to size
        data = super().read(size)
        if (trim_cols):

            # first is being processed, trim whitespace from header names
            trim_col_headers = re.compile('(\s*([\w\d_]+)\s*)*')
            end_of_line_index = data.index('\n');
            trimmed_headers = trim_col_headers.sub(r'\2', data[:end_of_line_index])

            # return the trimmed headers
            return trimmed_headers + data[end_of_line_index:]
        else:

            # return the data
            return data
