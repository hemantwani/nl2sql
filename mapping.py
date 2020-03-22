import numpy as np
from dateutil.parser import parse
from fuzzywuzzy import process

class Mapper:
    def __init__(self, df):
        self.df = df

    def is_date(self, string):
        try:
            parse(string)
            return True
        except:
            return False

    def mappingI(self):
        columns = list(self.df)
        ent_col_corpus = {
        'sub category' : ['product category', 'sub category', 'merchandising', 'merchandise','subcategory'],
        'brand':['brand'],
        'units':['sales units','quantity sold', 'units sold','sales'],
        'revenue':['revenue', 'earnings', 'sales revenue'],
        'unit_lift': ['unit lift', 'unit_lift'],
        'revenue_lift': ['revenue lift', 'rev_lift'],
        'category':['product category', 'category']
        }

        type_object = (list(self.df.select_dtypes(exclude=[np.number])))
        # type_numeric = (list(self.df._get_numeric_data()))
        mapping_dict = {}

        for object in type_object:
            date_check = list(set(self.df[object].apply(self.is_date).tolist()))
            if len(date_check) == 1 and date_check[0]==True:
                mapping_dict['time'] = object
                columns.remove(object)

        for col in columns:
            col_wise_list = []
            for k,v in ent_col_corpus.items():
                res = process.extractOne(col,v)
                col_wise_list.append((k, res[1]))
            max_item = max(col_wise_list, key=lambda item: item[1])
            if max_item[1]>=75:
                mapping_dict[max_item[0]] = col
        return mapping_dict

class Update_Mapper:
    def __init__(self):
        pass

    def parse_pre_config(self, pre_config):
        dict2 = pre_config['message']['data']
        # mapping_dict = ["{}:{}".format(elem['entity'],elem['column']) for elem in dict2]
        mapping_dict = {}
        for elem in dict2:
            mapping_dict[elem['entity']] = elem['column']
        print(mapping_dict)
        return (mapping_dict)
