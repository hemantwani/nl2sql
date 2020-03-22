class Lookup_creator:
    def __init__(self):
        pass

    def lookup_(self, df):
        cols = df.columns ##all df columns
        num_cols = df._get_numeric_data().columns  ###columns with numerical values
        categorical_columns = (list(set(cols) - set(num_cols))) ##categorical columns

        lookup_dict = {}
        for category in categorical_columns:
            uniques = df[category].str.lower().unique().tolist()  ## to get all unique values from categorical columns and convert it to list form
            lookup_dict[category] = uniques

        return (lookup_dict)


