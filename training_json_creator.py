import pandas as pd
import json
import math

def find_str(s, char):
    index = 0

    if char in s:
        c = char[0]
        for ch in s:
            if ch == c:
                if s[index:index+len(char)] == char:
                    start_index = index
                    end_index = index+len(char)
                    return [start_index, end_index]

            index += 1
    return [0,0]

def float_to_int(char):
    try:
        if char.is_integer() == True:
            val = str(math.ceil(char))
            return val
        return char
    except:
        return char


def common_example_generator(df):
    column_names = (list(df))
    common_examples = []

    for index, row in df.iterrows():
        sent_dict = {}
        text = (row['sentence'])
        intents_levels = [(row['Level 1']), (row['Level 2']), (row['Level 3']), (row['Level 4']), (row['Level 5'])]
        valid_intents = [str(x).lower() for x in intents_levels]
        actual_intent = "+".join(valid_intents)
        entities = []

        for column_name in column_names[6:]:
            col_value = row[column_name]
            if str(col_value).lower() != "nan":
                entity_val = str(float_to_int(col_value))
                position = (find_str(text.lower(), entity_val.lower()))
                dict = {
                    'start': position[0],
                    'end': position[1],
                    'value': str(entity_val).lower(),
                    'entity': column_name
                }
                entities.append(dict)
        sent_dict['text'] = text
        sent_dict['intent'] = actual_intent
        sent_dict['entities'] = entities
        common_examples.append(sent_dict)
    # print (common_examples)
    return common_examples


df = pd.read_excel(r'training_data.xlsx', sheet_name='Sheet2')
print (list(df))
list1 = common_example_generator(df)
rasa_nlu_data = {'rasa_nlu_data':
                    {
                        'common_examples' : list1,
                    }
                }


with open("./Rasa_train/rasa_train2.json", "w") as file:
    json.dump(rasa_nlu_data,file)