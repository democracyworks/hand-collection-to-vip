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
from minnesota_polling_location import PollingLocationTxt
import datetime
import re

class ScheduleTxt(object):
    """
    Inherits from PollingLocationTxt.

    """


    def __init__(self, base_df, early_voting_true='false', drop_box_true='false'):
        #PollingLocationTxt.__init__(self, base_df, early_voting_true='false', drop_box_true='false')
        #self.base_df = self.export_for_schedule_and_locality()
        self.base_df = base_df

#    def format_for_schedule(self):

#        sch_base_df = self.base_df

        # Drop base_df columns.
#        sch_base_df.drop(['index', 'ocd_division', 'county', 'location_name', 'address_1', 'address_2',
#                          'city', 'state', 'zip', 'id'], inplace=True, axis=1)

       # print self.dedupe(sch_base_df)

#        return self.dedupe(sch_base_df)


    def get_sch_start_time(self, start_time, start_date):
        """Replace AM or PM with ':00' and concatenate with utc offset"""
        diff1 = '11/7'
        diff2 = '11/6'

        if diff1 in start_date or diff2 in start_date:
            utc_offset = config.utc_offset_6
        else:
            utc_offset = config.utc_offset_5

        start_time = tuple(start_time.split('-'))[0]

        start_time = str(datetime.datetime.strptime(start_time, '%I:%M %p'))[11:]
        #print start_time

        return start_time + utc_offset

    def get_end_time(self, end_time, start_date):
        """Replace AM or PM with ':00' and concatenate with utc offset."""

        diff1 = '11/7'
        diff2 = '11/6'

        if diff1 in start_date or diff2 in start_date:
            utc_offset = config.utc_offset_6
        else:
            utc_offset = config.utc_offset_5

        end_time = tuple(end_time.split('-'))[1]

        end_time = str(datetime.datetime.strptime(end_time, '%I:%M %p'))[11:]

        return end_time + utc_offset

    def is_only_by_appointment(self):
        return ''

    def is_or_by_appointment(self):
        return ''

    def is_subject_to_change(self):
        # create conditional when/if column is present
        return ''


    def get_start_date(self, start_date):
        """#"""

        diff1 = '11-07'
        diff2 = '11-06'

        if diff1 in start_date or diff2 in start_date:
            utc_offset = config.utc_offset_6
        else:
            utc_offset = config.utc_offset_5

        start_date = datetime.datetime.strptime(start_date, '%m/%d/%y').strftime('%Y-%m-%d')
        return start_date

    def get_end_date(self, end_date, start_date):
        """#"""

        diff1 = '11-07'
        diff2 = '11-06'

        if diff1 in start_date or diff2 in start_date:
            utc_offset = config.utc_offset_6
        else:
            utc_offset = config.utc_offset_5

        end_date = datetime.datetime.strptime(end_date, '%m/%d/%y').strftime('%Y-%m-%d')
        return end_date

    def get_hours_open_id(self, hours_open_id):
        """#"""
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

        self.base_df['start_time2'] = self.base_df.apply(
            lambda row: self.get_sch_start_time(row['hours'], row['start_date']), axis=1)

        self.base_df['end_time2'] = self.base_df.apply(
            lambda row: self.get_end_time(row['hours'], row['start_date']), axis=1)

        self.base_df['is_only_by_appointment2'] = self.base_df.apply(
            lambda row: self.is_only_by_appointment(), axis=1)

        self.base_df['is_or_by_appointment2'] = self.base_df.apply(
            lambda row: self.is_or_by_appointment(), axis=1)

        self.base_df['is_subject_to_change2'] = self.base_df.apply(
            lambda row: self.is_subject_to_change(), axis=1)

        self.base_df['start_date2'] = self.base_df.apply(
            lambda row: self.get_start_date(row['start_date']), axis=1)

        self.base_df['end_date2'] = self.base_df.apply(
            lambda row: self.get_end_date(row['end_date'], row['start_date']), axis=1)

        self.base_df['hours_open_id2'] = self.base_df.apply(
            lambda row: self.get_hours_open_id(row['hours_open_id']), axis=1)

        self.base_df['id2'] = self.base_df.apply(
            lambda row: self.create_schedule_id(row['index']), axis=1)

        return self.base_df

#    def dedupe(self, dupe):
#        """#"""
#        return dupe.drop_duplicates(subset=['address_line', 'hours'])

    def write_schedule_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        sch = self.build_schedule_txt()


         # start_time2, end_time2, is_only_by_appointment2, is_or_by_appointment2, is_subject_to_change2, start_date2, end_date2, hours_open_id2, id2

        # Drop base_df columns.
        sch.drop(['ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state',
                'zip', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
                'is_or_by_appointment', 'appointment_phone_num', 'is_subject_to_change', 'index',
                'address_line', 'directions', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box',
                'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'polling_location_id', 'dirs'], inplace=True, axis=1)

        # Drop base_df columns.
        #sch.drop(['address_line', 'directions', 'hours', 'photo_uri', 'is_drop_box', 'is_early_voting',
        #          'latitude', 'longitude', 'latlng_source', 'id'], inplace=True, axis=1)

#        sch = self.dedupe(sch)  # 'address_line' and 'hours are used to identfy/remove duplicates
        #print sch

#        sch.drop(['address_line', 'hours'], inplace=True, axis=1)
#        print sch

        sch.rename(columns={'start_time2': 'start_time', 'end_time2': 'end_time',
                            'is_only_by_appointment2': 'is_only_by_appointment',
                            'is_or_by_appointment2': 'is_or_by_appointment',
                            'is_subject_to_change2': 'is_subject_to_change',
                            'start_date2': 'start_date', 'end_date2': 'end_date',
                            'hours_open_id2': 'hours_open_id', 'id2': 'id'}, inplace=True)

        print sch

        sch.to_csv(config.output + 'schedule.txt', index=False, encoding='utf-8')  # send to txt file
        sch.to_csv(config.output + 'schedule.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    ''

    early_voting_true = 'true'  # true or false

    early_voting_file = 'intermediate_doc.csv'

    early_voting_path = config.output + early_voting_file
    colnames = ['ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'dirs', 'city', 'state',
                'zip', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
                'is_or_by_appointment', 'appointment_phone_num', 'is_subject_to_change', 'index', 'address_line', 'directions',
                'hours', 'photo_uri', 'hours_open_id', 'is_drop_box', 'is_early_voting', 'latitude', 'longitude',
                'latlng_source', 'polling_location_id']

    early_voting_df = pd.read_csv(early_voting_path, names=colnames, encoding='utf-8', skiprows=1)

    early_voting_df['index'] = early_voting_df.index + 1 # offsets zero based index so it starts at 1 for ids
    #print early_voting_df

    st = ScheduleTxt(early_voting_df, config.state)
    st.write_schedule_txt()