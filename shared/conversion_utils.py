def get_convert_to_string(col_name):

    # convert 'M' values to None for the given column name
    def convert_to_string(value):
        if value == 'M':
            return None
        else:
            return value

    return convert_to_string


def get_convert_to_float(col_name):

    # convert 'M' values to None for the given column name
    def convert_to_float(value):
        if value == 'M' or value == 'T':
            return None
        else:
            try:
                return float(value)
            except ValueError:
                print('Could not convert col {0} with a value {1} to float'.format(col_name, value))
                raise

    return convert_to_float


def get_convert_to_date(col_name):
    
    # convert 'M' values to None for the given column name
    def convert_to_date(value):
        if value == 'M':
            return None
        else:
            try:
                return float(value)
            except ValueError:
                print('Could not convert col {0} with a value {1} to float'.format(col_name, value))
                raise

    return convert_to_date
