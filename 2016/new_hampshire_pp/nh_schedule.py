"""
start_time,
end_time,
is_only_by_appointment,
is_or_by_appointment,
is_subject_to_change,
start_date,
end_date,
hours_open_id,
id
"""


import pandas as pd
import config
import datetime
import hashlib



class ScheduleTxt(object):
    """

    """


    def __init__(self, base_df):
        self.base_df = base_df
        #print self.base_df

#    def mountain_time(self, county):
#        df = self.build_schedule_txt()

#        if df['county'] in mountain_tz:
#                df['start_date'] = df['start_date'].str.replace('\n', '')


    def get_start_time(self, index, office_name, start_time, start_date, hours_open_id):
        """#"""
        #print index
        start_time = start_time.strip()

        if start_time:
            if len(start_time.replace(' - ', '-')) == 13:
                st = '0' + start_time.replace(' - ', '-')
                #print index, st, hours_open_id
                return st
            else:

                st = start_time.replace(' - ', '-')
                print index, st, hours_open_id
                return st
        else:
            raise ValueError('Missing start_time value at row ' + str(index) + '.')

    def get_end_time(self, index, end_time, hours_open_id):
        """#"""

        end_time = end_time.strip()

        if end_time:
            #print index, end_time
            return end_time.replace(' - ', '-')
        else:
            raise ValueError('Missing end_time value at row ' + str(index) + '.')

    def is_only_by_appointment(self):
        return None

    def is_or_by_appointment(self):
        return None

    def is_subject_to_change(self):
        # create conditional when/if column is present
        return None

    def get_start_date(self, index, start_date):
        """#"""

        return '2016-11-08'

    def get_end_date(self, index, end_date):
        """#"""
        return '2016-11-08'

    def get_hours_open_id(self, hours_open_id):
        """#"""
        #print hours_open_id
        return hours_open_id


    def create_schedule_id(self, index):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length

        if index <= 9:
            index_str = '000' + str(index)

        elif index in range(10, 100):
            index_str = '00' + str(index)

        elif index in range(100, 1000):
            index_str = '0' + str(index)
        else:
            index_str = str(index)

        return 'sch' + str(index_str)

    def build_schedule_txt(self):
        """
        New columns that match the 'schedule.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

        self.base_df['start_time'] = self.base_df.apply(
            lambda row: self.get_start_time(row['index'], row['county'], row['start_time'], row['start_date'],
                                            row['hours_open_id']), axis=1)

        self.base_df['end_time'] = self.base_df.apply(
            lambda row: self.get_end_time(row['county'], row['end_time'], row['hours_open_id']), axis=1)

        self.base_df['is_only_by_appointment'] = self.base_df.apply(
            lambda row: self.is_only_by_appointment(), axis=1)

        self.base_df['is_or_by_appointment'] = self.base_df.apply(
            lambda row: self.is_or_by_appointment(), axis=1)

        self.base_df['is_subject_to_change'] = self.base_df.apply(
            lambda row: self.is_subject_to_change(), axis=1)

        self.base_df['start_date'] = self.base_df.apply(
            lambda row: self.get_start_date(row['index'], row['start_date']), axis=1)

        self.base_df['end_date'] = self.base_df.apply(
            lambda row: self.get_end_date(row['index'], row['end_date']), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.get_hours_open_id(row['hours_open_id']), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_schedule_id(row['index']), axis=1)

        return self.base_df

    def add_utc_offset(self):
        base = self.build_schedule_txt

        pass

    def write_schedule_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        sch = self.build_schedule_txt()
        #print sch

        # Drop base_df columns.
        sch.drop(['index', 'county', 'ocd_division', 'homepage_url', 'phone', 'email', 'street', 'city', 'state',
                  'zip_code', 'address_line', 'directions', 'hours', 'photo_uri', 'is_drop_box',
                  'is_early_voting', 'latitude', 'longitude', 'latlng_source'], inplace=True, axis=1)

        # reorder columns
        cols =['start_time', 'end_time', 'is_only_by_appointment', 'is_or_by_appointment', 'is_subject_to_change',
               'start_date', 'end_date', 'hours_open_id', 'id']

        sch = sch.reindex(columns=cols)
        #print sch

        sch.to_csv(config.output + 'schedule.txt', index=False, encoding='utf-8')  # send to txt file
        sch.to_csv(config.output + 'schedule.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    intermediate_doc = 'intermediate_doc.csv'

    s = 'county,precinct,official_title,ocd_division,homepage_url,phone,email,name,street,city,state,zip_code,start_time,end_time,start_date,end_date,index,address_line,directions,hours,photo_uri,hours_open_id,is_drop_box,is_early_voting,latitude,longitude,latlng_source,id'.split(',')
    print s


    colnames = ['county', 'precinct', 'official_title', 'ocd_division', 'homepage_url', 'phone', 'email', 'name',
                'street', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'index',
                'address_line', 'directions', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box', 'is_early_voting',
                'latitude', 'longitude', 'latlng_source', 'id']




    early_voting_df = pd.read_csv(config.output + intermediate_doc, names=colnames, encoding='ISO-8859-1', skiprows=1, delimiter=',')

    early_voting_df['index'] = early_voting_df.index +1 # offsets zero based index so it starts at 1 for ids
    #print early_voting_df
    #print early_voting_df.hours_open_id

    ScheduleTxt(early_voting_df).write_schedule_txt()
    #ScheduleTxt(early_voting_df).format_for_schedule()