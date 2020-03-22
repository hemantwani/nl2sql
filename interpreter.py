import datetime


class Value_Interpretation():
    def __init__(self):
        pass

    def guess_date(self, string):
        for fmt in ["%d/%m/%Y", "%d-%m-%Y"]:
            try:
                return datetime.datetime.strptime(string, fmt).date()
            except ValueError:
                continue
        raise ValueError(string)

    def date_lists_uniform_format(self, dateslist):
        date_list = []
        for sample in dateslist:
            date = self.guess_date(sample)
            date = date.strftime("%d-%m-%Y")
            date_list.append(date)
        return  date_list

    def interpreter(self, entities, lookup_dict, df, mapping_dict):
        entities_keys = entities.keys()
        num_cols = df._get_numeric_data().columns
        intersect = list(set(entities_keys) & set(list(set(list(df)) - set(num_cols))))
        intersect_keys = [k for k,v in mapping_dict.items() if v in intersect]


        if "time" in intersect_keys:
            key = mapping_dict['time']
            dates_list = self.date_lists_uniform_format(lookup_dict[key])
            latest_value = (sorted(dates_list, key=lambda x: datetime.datetime.strptime(x, '%d-%m-%Y')))[-1]
            date_txt_val = entities[key]
            fin_date = str((self.date_conversion(latest_value,date_txt_val, dates_list)))
            entities[key] = fin_date

        if "brand" in intersect_keys:
            key = mapping_dict['brand']
            val_extracted = entities[key]
            lookup_val = lookup_dict[key]
            if val_extracted not in lookup_val:
                entities[key] = ''

        if "merchandise_category" in intersect_keys:
            key = mapping_dict['merchandise_category']
            val_extracted = entities[key]
            lookup_val = lookup_dict[key]
            if val_extracted not in lookup_val:
                entities[key] = ''

        if "category" in intersect_keys:
            key = mapping_dict['category']
            val_extracted = entities[key]
            lookup_val = lookup_dict[key]
            if val_extracted not in lookup_val:
                entities[key] = ''

        return entities

    def date_conversion(self, latest_value, date_txt_val, dates_list):
        latest_date = datetime.datetime.strptime(latest_value, "%d-%m-%Y").date()

        if "week" in date_txt_val:
            if "past" in date_txt_val or "previous" in date_txt_val or "last" in date_txt_val or "pre" in date_txt_val:
                prev_factor = 1
                latest_date = str(latest_date - datetime.timedelta(prev_factor * 7))
            latest_date = str(datetime.datetime.strptime(str(latest_date), "%Y-%m-%d").strftime("%d-%m-%Y"))
            return latest_date

        elif "month" in date_txt_val:
            if "past" in date_txt_val or "previous" in date_txt_val or "last" in date_txt_val or "pre" in date_txt_val:
                prev_factor = 1
                out2 = (latest_date - datetime.timedelta(prev_factor * 365 / 12))
                latest_date = str(self.nearest_date(dates_list, out2))
                return latest_date
            return str(datetime.datetime.strptime(str(latest_date), "%Y-%m-%d").strftime("%d-%m-%Y"))

        elif "year" in date_txt_val:
            if "past" in date_txt_val or "previous" in date_txt_val or "last" in date_txt_val or "pre" in date_txt_val:
                prev_factor = 1
                out2 = (latest_date - datetime.timedelta(prev_factor * 365 ))
                latest_date = str(self.nearest_date(dates_list, out2))
                return latest_date
            return str(datetime.datetime.strptime(str(latest_date), "%Y-%m-%d").strftime("%d-%m-%Y"))


        elif "quarter" in date_txt_val:
            if "past" in date_txt_val or "previous" in date_txt_val or "last" in date_txt_val or "pre" in date_txt_val:
                prev_quarter = 1
                prev_factor = prev_quarter * 3
                out2 = (latest_date - datetime.timedelta(prev_factor * 365 / 12 ))
                latest_date = str(self.nearest_date(dates_list, out2))
                return latest_date
            return str(datetime.datetime.strptime(str(latest_date), "%Y-%m-%d").strftime("%d-%m-%Y"))

    ######FOR KPI
    def prev_date_approx(self, lookup_dict, mapping_dict):
        col_name = mapping_dict['time']
        date_list = Value_Interpretation().date_lists_uniform_format(lookup_dict[col_name])
        start_date = (sorted(date_list, key=lambda x: datetime.datetime.strptime(x, '%d-%m-%Y')))[-1]

        prevyear = 1
        date1 = datetime.datetime.strptime(start_date, '%d-%m-%Y').date()
        date_interval = (date1 - datetime.timedelta(prevyear * 365))
        prev_approx = str(self.nearest_date(date_list, date_interval))
        start_date = str(start_date)
        start_end_date = [start_date, prev_approx]

        return start_end_date


    def nearest_date(self, items, date):
        nearest = min(items, key=lambda x: abs(datetime.datetime.strptime(x, '%d-%m-%Y').date() - date))
        return nearest