from rasa_nlu.training_data import load_data
from rasa_nlu.model import Trainer
from rasa_nlu import config
import pickle, os
from rasa_nlu.model import Interpreter
from mapping import Mapper
import re
from nl2sql import Nl_Sql

# from temp_nlsql import Nl_Sql
from interpreter import Value_Interpretation
import datetime

##import the database
from import_data import Import

imported_data = Import().import_data()
df = imported_data["dataframe"]
cursor = imported_data["cursor"]
conn = imported_data["conn"]
df_cols = list(df)

###import lookup
from lookup import Lookup_creator
lookup_dict = Lookup_creator().lookup_(df)
# print (lookup_dict)

# try:
#     from nltk import pos_tag, word_tokenize
# except:
#     import nltk
#     nltk.download('averaged_perceptron_tagger')
#     nltk.download('punkt')
#     nltk.download('brown')


def date_extraction(inp_str):
    date_extracted = re.search(r"(\b(0?[1-9]|[12]\d|30|31)[^\w\d:](0?[1-9]|1[0-2])[^\w\d:](\d{4}|\d{2})\b)|(\b(0?[1-9]|[12]\d|30|31)(,)?\s+((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)\s+(\d{4}|\d{2})\b)|(\b(0?[1-9]|[12]\d|30|31)(\s+)?(?:st|nd|rd|th)(,)?\s+((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)\s+(\d{4}|\d{2})\b)|(\b(0?[1-9]|[12]\d|30|31)(?:st|nd|rd|th)(\s+)?((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)(')?(\s+)?(\d{4}|\d{2})\b)|(\b(0?[1-9]|[12]\d|30|31)(\s+)?((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)(')?(\s+)?(\d{4}|\d{2})\b)",inp_str)
    format = ["%d" ,"%b", "%Y"]

    if date_extracted == None:
        date_extracted = re.search(r"(\b(0?[1-9]|1[0-2])[^\w\d:](0?[1-9]|[12]\d|30|31)[^\w\d:](\d{4}|\d{2})\b)|(\b((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)\s+(0?[1-9]|[12]\d|30|31)(,)?\s+(\d{4}|\d{2})\b)|(\b((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)\s+(0?[1-9]|[12]\d|30|31)(\s+)?(?:st|nd|rd|th)(,)?\s+(\d{4}|\d{2})\b)|(\b((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)(0?[1-9]|[12]\d|30|31)(?:st|nd|rd|th)(\d{4}|\d{2})\b)",inp_str)
        format = ["%b", "%d", "%Y"]

    if date_extracted == None:
        date_extracted = re.search(r"(\b(\d{4}|\d{2})[^\w\d:](0?[1-9]|1[0-2])[^\w\d:](0?[1-9]|[12]\d|30|31)\b)|(\b(\d{4}|\d{2})(,)?\s+((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)(,)?(\s+)?(0?[1-9]|[12]\d|30|31)\b)|(\b(\d{4}|\d{2})(,)?\s+((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)(,)?(\s+)?(0?[1-9]|[12]\d|30|31)(\s+)?(?:st|nd|rd|th)\b)|(\b(\d{4}|\d{2})((J|j)an(uary)?|(F|f)eb(ruary)?|(M|m)ar(ch)?|(A|a)pr(il)?|(M|m)ay|(J|j)un(e)?|(J|j)ul(y)?|(A|a)ug(ust)?|(S|s)ep(tember)?|(S|s)ept(ember)?|(O|o)ct(ober)?|(N|n)ov(ember)?|(D|d)ec(ember)?)(0?[1-9]|[12]\d|30|31)(?:st|nd|rd|th)\b)",inp_str)
        format = ["%Y", "%b", "%d"]

    if date_extracted != None:
        date = (date_extracted.group(0)).replace("'", " ")
        date = re.sub("[^A-Za-z0-9 ]+", '-', date)
        format1 = " ".join(format)
        format2 = format1.replace("b", "B")
        format3 = "-".join(format).replace('b', 'm')
        format4 = format1.replace("Y", "y")
        format5 = format2.replace("Y", "y")
        format6 = format3.replace("Y", "y")
        result = [date, format1, format2, format3, format4, format5, format6]
    else:
        result = []
    return result

def date_formatwise_conversion(date, format):
    try:
        formated_date = datetime.datetime.strptime(date, format).date()
        formated_date = datetime.datetime.strftime(formated_date,'%d-%m-%Y')
        print ("This is formatted date::{}".format(date))
    except:
        formated_date = None
    return formated_date


def date_checker(inp_str):
    date_extracted = date_extraction(inp_str)
    if date_extracted:
        date = date_extracted[0]
        date_format1, date_format2, date_format3, date_format4, date_format5, date_format6 = date_extracted[1:7]

        date_val = re.sub(r'st|nd|rd|th', '', date).replace("  ", " ")
        print(date, [date_format1], [date_format2], [date_format3], [date_format4], [date_format5], [date_format6])

        date = date_formatwise_conversion(date_val, date_format1)
        if date == None:
            date = date_formatwise_conversion(date_val, date_format2)
        if date == None:
            date = date_formatwise_conversion(date_val, date_format3)
        if date == None:
            date = date_formatwise_conversion(date_val, date_format4)
        if date == None:
            date = date_formatwise_conversion(date_val, date_format5)
        if date == None:
            date = date_formatwise_conversion(date_val, date_format6)
        if date == None:
            date = ''
    else:
        date = ''
    return str(date)

def top_bottom_count(sentence):
    m = re.search('top(\d+)', sentence.lower().replace(" ",""), re.IGNORECASE) or re.search('mid(\d+)', sentence.lower().replace(" ",""), re.IGNORECASE)\
        or re.search('bottom(\d+)', sentence.lower().replace(" ",""), re.IGNORECASE) or re.search('head(\d+)', sentence.lower().replace(" ",""), re.IGNORECASE)\
        or re.search('tail(\d+)', sentence.lower().replace(" ",""), re.IGNORECASE)or re.search('lead(\d+)', sentence.lower().replace(" ",""), re.IGNORECASE)
    if m != None:
        return (m.group(1))
    return ('1')

def periodic_counts(sentence):
    m = re.search('(\d+)wee.', sentence.lower().replace(" ",""), re.IGNORECASE) \
        or re.search('(\d+)mont.', sentence.lower().replace(" ",""), re.IGNORECASE) \
        or re.search('(\d+)quarte.', sentence.lower().replace(" ",""), re.IGNORECASE) \
        or re.search('(\d+)annu.', sentence.lower().replace(" ",""), re.IGNORECASE) \
        or re.search('(\d+)y.', sentence.lower().replace(" ",""), re.IGNORECASE)
    if m != None:
        return (m.group(1))
    return ('1')


def process_analysis(input_sentence, intents,  entities, postags):

    mapping_dict = Mapper(df).mappingI()
    # print ("This is the mapping dict:       {}".format(mapping_dict))
    # print (entities)

    rasa_entities = {}
    for ent in entities:
        if ent["entity"] == ent["value"]:
            rasa_entities[ent["entity"]] = ""
        else:
            rasa_entities[ent["entity"]] = ent["value"]


    # print ("Rasa_entities:{}".format(rasa_entities))
    if "time" in rasa_entities.keys():
        period_count = periodic_counts(input_sentence)
        if period_count != 0:
            rasa_entities["period_count"] = period_count

    ######lookup values
    lookup_entities = {}
    for key, value in lookup_dict.items():
        for word in value:
            if word in input_sentence:
                lookup_entities[key] = word.lower()

    ###compare lookups and rasa_entities
    values1 = rasa_entities.values()
    for k, v in lookup_entities.items():
        if v not in values1:
            rasa_entities[k] = v

    rasa_keys = rasa_entities.keys()
    mapper_keys = mapping_dict.keys()
    entities_to_map = list(set(mapper_keys) & set(rasa_keys))

    for entity in entities_to_map:
        new_key = mapping_dict[entity]
        old_key = entity
        if new_key not in rasa_keys:
            rasa_entities[new_key] = rasa_entities.pop(old_key)

    intents_list = intents.split("+")
    rasa_level1 = intents_list[0]
    rasa_level2 = intents_list[1]
    rasa_level3 = intents_list[2]
    rasa_level4 = intents_list[3]
    rasa_level5 = intents_list[4]

    level2_dict = {}
    if rasa_level2 != "nan":  ##to check for level2 intent and mapping a column to it and adding in a dict
        # print(mapping_dict[rasa_level2])
        level2_dict[mapping_dict[rasa_level2]] = rasa_level2
    final_intents = [rasa_level1, level2_dict, rasa_level3, rasa_level4, rasa_level5]  ##final intent list where level2 is dict of column and value

    if rasa_level3 == "top" or rasa_level3 == "bottom" or rasa_level3 == "mid":
        top_count = top_bottom_count(input_sentence)
        if top_count != 0:
            rasa_entities["count"] = top_count


    # print ("Entities b4 interpreter:    {}".format(rasa_entities))
    final_entities = Value_Interpretation().interpreter(rasa_entities, lookup_dict, df, mapping_dict)
    date_extracted = date_checker(input_sentence)
    if date_extracted:
        final_entities[mapping_dict['time']] = date_extracted
    print("INTENTS:    ", final_intents)
    print("ENTITIES:   ", final_entities)

    nlsql = Nl_Sql(cursor, final_entities, final_intents, postags, df_cols, lookup_dict, mapping_dict)
    gen_result = (nlsql.nl2sql())
    print (gen_result)
    print("\n\n")

    return gen_result


rasa_train_dir = "./Rasa_train/"
if os.path.isfile(os.path.join(rasa_train_dir, "model2.pkl")):
    print ("Loading from already trained_model")
    trained_model = pickle.load(open(os.path.join(rasa_train_dir, 'model2.pkl'), 'rb'))
    print ("Model loaded")
else:
    print ("No model.pkl file... Training the rasa_model")
    training_data = load_data(os.path.join(rasa_train_dir,'rasa_train2.json'))
    trainer = Trainer(config.load(os.path.join(rasa_train_dir,'config.json')))
    trainer.train(training_data)
    trained_model = trainer.persist(rasa_train_dir)
    print ("Training done and Model loaded")
    output_model = open(os.path.join(rasa_train_dir, 'model2.pkl'), 'wb')
    pickle.dump(trained_model, output_model)
    output_model.close()


# where model_directory points to the model folder
interpreter = Interpreter.load(trained_model)

while True:
    print ("\n\n")
    input_sentence = input("Enter:  ").lower()
    interpreter_result = (interpreter.parse(input_sentence))

    intents = (interpreter.parse(input_sentence)['intent']['name'])
    entities = (interpreter.parse(input_sentence)['entities'])
    postags = ''

    if intents!= None:
        # result = process_analysis(input_sentence, intents, entities, postags)
        try:
            result = process_analysis(input_sentence, intents, entities, postags)
        except:
            result = "Sorry for the inconvenience caused but I couldn't answer this query due to lack of my knowledge. I am still learning."
    else:
        result = "Sorry for the inconvenience caused but I couldn't answer this query as your question seems invalid. Please  enter valid question and try again."

    print (result)