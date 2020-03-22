import pandas as pd
import matplotlib.pyplot as plt
import operator
import json
from interpreter import Value_Interpretation


class Nl_Sql:
    def __init__(self, cursor, entities , intents, pos_tags, df_cols, lookup, mapping_dict):
        self.params = entities
        self.intents = intents
        self.pos_tags = pos_tags
        self.cols = df_cols
        self.cursor = cursor
        self.lookup = lookup
        self.mapping_dict = mapping_dict


    def intents_split(self):
        self.level1 = self.intents[0]
        self.level3 = self.intents[2]
        self.level4 = self.intents[3]
        self.level5 = self.intents[4]
        level2 = self.intents[1]
        if level2 != 'nan':
            self.level2 = (list(level2.keys())[0])
        else:
            self.level2 = ""
        if self.level4 != 'nan':
            self.level_4_params = self.level_4_check()

    def level_3_check(self,required_df, sort_factor = None):
        if self.level3 != 'nan':
            if sort_factor == None:
                sort_factor = self.level2
            else:
                sort_factor = sort_factor
            # print (33,sort_factor)
            if self.level3 == "highest":
                final_df = required_df.sort_values(sort_factor, ascending=False).head(1)
            elif self.level3 == "lowest":
                final_df = required_df.sort_values(sort_factor, ascending=True).head(1)
            elif self.level3 == "top":
                count = int(self.params['count'])
                final_df = required_df.sort_values(sort_factor, ascending=False).head(count)
            elif self.level3 == "bottom":
                count = int(self.params['count'])
                final_df = required_df.sort_values(sort_factor, ascending=True).head(count)
            elif self.level3 == "rank":
                final_df = (required_df.rank(ascending=False))
            else:
                final_df = required_df
        else:
            final_df = required_df
        return final_df


    def level_4_check(self):
        if self.level4 == "yoy":
            start_end_date = Value_Interpretation().prev_date_approx(self.lookup, self.mapping_dict)
            prev_approx_date = start_end_date[1]
            if "Start_Week" not in self.params.keys():
                start_week = start_end_date[0]
                self.params["Start_Week"] =  start_week
            start_week = self.params["Start_Week"]
            result = [start_week , prev_approx_date]
            return result


    def level_5_check(self):
        if self.level5 == "comparison":
            plot_gen = "bar"
        elif self.level5 == "composition":
            plot_gen = "pie"
        elif self.level5 == "distribution":
            plot_gen = "histogram"
        elif self.level5 == "relationship":
            plot_gen = "scatter"
        else:
            plot_gen = 0
        return plot_gen



    def nl2sql(self):
        self.intents_split()

        #####column select for sql query
        self.present_entities = ["{}".format(k) for k, v in self.params.items() if v.strip() and k in self.cols] #####in all total entities extracted
        self.filter_lst = ["{}".format(k) for k, v in self.params.items() if k in self.cols]   ####the entities with distinct values

        # self.features_to_groupby = [x for x in self.filter_lst if x not in self.present_entities]  ##### extraction of entities with not distinct values
        # print ("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        # print (self.present_entities)
        # print (self.filter_lst)
        # print ("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

        if self.filter_lst:
            entities_extracted = ",".join(self.filter_lst)
            query = ("SELECT {} ,".format(entities_extracted))
            return (self.further_intent_wise_analysis(query))
        else:
            final_df = {'responser': 'Please provide the essential parameters for analysis. Your question seems to be incomplete, please reframe your query.', 'plot_gen':0}
            return  final_df


    def further_intent_wise_analysis(self, query):
        if "plot" in self.params.keys():
            self.plot_gen = self.params["plot"]
        else:
            self.plot_gen = self.level_5_check()

        #####for level2 select a column name
        if self.level2:
            query = query + "%s " % self.level2
            self.filter_lst.append(self.level2)
        query = query + " FROM sample"

        #####filters for sql query
        filters = ['{}="{}"'.format(k, v) for k, v in self.params.items() if v.strip() and k in self.cols]  ##GENERATING FILTERS BASED ON THE ENTITY KEYS

        ###JOINING THE FILTERS
        conditions = " AND ".join(filters)
        if conditions:
            where_lst = [query, conditions]
            final_q = (" WHERE ".join(where_lst))
        else:
            final_q = query

        if self.level4!= "nan":
            result = self.multiple_query_kpi(final_q)
        else:
            result = self.single_query_no_kpi(final_q)
        return result

    def group_by_variables(self):
        '''
        grp_by = self.present_entities
        if grp_by:
            required_df = (self.my_df.groupby(grp_by).sum()).add_suffix('').reset_index()
        else:
            required_df = (self.my_df.groupby(self.present_entities).sum()).add_suffix('').reset_index()
        '''
        grp_by = self.filter_lst
        grp_by.remove(self.level2)
        if not grp_by:
            return self.present_entities
        else:
            return grp_by


    def multiple_query_kpi(self, final_q):
        start_week = self.level_4_params[0]
        prev_approx_date = self.level_4_params[1]

        final_q2 = final_q.replace(start_week, prev_approx_date)  ####yoy date change
        print ("DF1 Query:", final_q)
        print ("DF2 Query:", final_q2)


        df1 = (self.cursor.execute(final_q).fetchall())
        df2 = (self.cursor.execute(final_q2).fetchall())
        df1.extend(df2)
        print (df1)
        self.my_df = pd.DataFrame(df1 , columns = self.filter_lst)

        grp_by = self.group_by_variables()
        required_df = (self.my_df.groupby(grp_by, sort=True).sum()).add_suffix('').reset_index()


        grp_by.remove("Start_Week")
        if not grp_by:
            return ("Sorry for the inconvenience caused but I couldn't answer this query as your question seems incomplete. Please provide all the required parameters and let me help you in your analysis..")

        elif len(grp_by) == 1:
            distinct_col = grp_by[0]
        else:
            distinct = {}
            for col in grp_by:
                distinct[col] = required_df[col].nunique()
            distinct_col = max(distinct.items(), key=operator.itemgetter(1))[0]

        processed_reqd_df = KPI().yoy_change(required_df, distinct_col, self.level2)
        final_df = self.level_3_check(processed_reqd_df, "profit")

        if final_df.shape[0] > 1:
            final_df = final_df.loc[:, (final_df != final_df.iloc[0]).any()].reset_index(drop=True)
            fin_js = json.loads(final_df.to_json(orient='index'))
            final_df = {"data": [v for k, v in fin_js.items()]}

            if self.plot_gen==0:
                self.plot_gen = "bar"
            process_result = self.df_formatter(final_df, self.plot_gen)
            return (process_result)

        else:
            print (final_df)
            profit = abs(final_df["profit"].values[0])
            growth_rate = abs(final_df["growth_rate"].values[0])

            dict_df = {}
            for col in grp_by:
                dict_df[col] = list(required_df[col].unique())

            sentence = "Hey, I found the result for your query. It seems that you were looking for {}".format(self.level2)
            sentence2 = KPI().growth(profit, growth_rate)
            for key, value in dict_df.items():
                if len(value)==1:
                    sentence += " such that for {} as {},".format(key, value[0])
            sentence += " {} in the range between {} to {}".format("Start_Week", prev_approx_date, start_week)
            final_df = {}
            final_df["response"] = sentence + sentence2
            final_df["plot_gen"] = self.plot_gen

            process_result = json.dumps(final_df).replace('\'', '').replace('\"', '\'')
            return (process_result)


    def single_query_no_kpi(self, final_q):
        print("This is Sql query generated:   ",final_q)
        fetched_value = (self.cursor.execute(final_q).fetchall())  ###sql qUERY BASED ON QUERY STATEMENT AND PARAMEWTERS
        self.my_df = pd.DataFrame(fetched_value, columns = self.filter_lst)

        grp_by = self.group_by_variables()
        required_df = (self.my_df.groupby(grp_by).sum()).add_suffix('').reset_index()
        final_df = self.level_3_check(required_df)
        print ("\n\n##############This is final df###############")
        print (final_df)

        if final_df.shape[0] > 1:

            final_df = final_df.loc[:, (final_df != final_df.iloc[0]).any()].reset_index(drop=True)
            fin_js = json.loads(final_df.to_json(orient='index'))
            final_df = {"data": [v for k, v in fin_js.items()]}

        process_result = self.df_formatter(final_df, self.plot_gen)
        return (process_result)


    def df_formatter(self, df, plot_gen):
        if plot_gen == 0:
            json_df = json.loads(df.to_json())
            fin_df = {}
            for key, value in json_df.items():
                actual_value = (list(value.values())[0])
                fin_df[key] = actual_value

            last_key = list(fin_df.keys())[-1]
            last_val = fin_df[last_key]
            fin_df.pop(last_key)

            sentence = "It seems that you want to know regarding {}".format(last_key)
            sentence2 = " result is {}".format(last_val)
            for key, value in fin_df.items():
                sentence += " for {} as {},".format(key, value)
            final_df = {}
            final_df["response"] = sentence + sentence2
            final_df["plot_gen"] = plot_gen
        else:
            final_df = df
            final_df["response"] = "Here you go. Please have a look at the visualization generated based on your query."
            final_df["plot_gen"] = plot_gen

        json_df_result = json.dumps(final_df).replace('\'', '').replace('\"', '\'')
        return  json_df_result

    def my_plots(self):
        df = pd.DataFrame({'lab': ['A', 'B', 'C'], 'val': [10, 30, 20]})
        ax = df.plot.bar(x='lab', y='val', rot=0)


class KPI:
    def yoy_change(self, required_df, distinct_col, level2):
        new_frame = (required_df.groupby(distinct_col)[level2].apply(list).apply(lambda x: x if (len(x) == 2) else "F").reset_index())
        new_frame = new_frame[new_frame[level2] != "F"]
        new_frame["profit"] = new_frame[level2].apply(lambda x: (x[0] - x[1]))
        new_frame["growth_rate"] = new_frame[level2].apply(lambda x: (x[0] - x[1])/x[1])
        new_frame = new_frame.drop(level2, axis=1)
        return  new_frame

    def growth(self, profit, growth):
        profit_loss_value = profit
        growth_rate = growth

        if growth_rate < 0:
            profit_loss_value = (profit_loss_value*-1)
            growth_rate = (growth_rate * -1)
            profit_or_loss = "loss"
            growth = "decreasing"

        elif growth_rate == 0:
            profit_or_loss = "no profit and no loss"
            growth = "neutral"

        else:
            profit_or_loss = "profit"
            growth = "increasing"

        sentence2 = ". Here, I observed a {} growth at the rate of {} and {} of {}.".format(growth, growth_rate, profit_or_loss, profit_loss_value)
        return sentence2



# what are the sales for mens after shaves category
# which category has the highest revenue  for the past week?
# Top 3 brands in every product category for the latest week
# please tell the sales for garnier last week
# Which category has the highest revenue  for the latest week?
# Brand Performance basis revenue for the latest week