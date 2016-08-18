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

class ScheduleTxt(PollingLocationTxt):
    """
    Inherits from PollingLocationTxt.

    """


    def __init__(self, base_df, early_voting_true='false', drop_box_true='false'):
        PollingLocationTxt.__init__(self, base_df, early_voting_true='false', drop_box_true='false')
        self.base_df = self.build_polling_location_txt()
        print self.base_df

    def format_for_schedule(self):

        sch_base_df = self.base_df

        # Drop base_df columns.
        sch_base_df.drop(['index', 'ocd_division', 'county', 'location_name', 'address_1', 'address_2',
                          'city', 'state', 'zip', 'id'], inplace=True, axis=1)

       # print self.dedupe(sch_base_df)

        return self.dedupe(sch_base_df)


    def get_sch_start_time(self, start_time):
        """Replace AM or PM with ':00' and concatenate with utc offset"""

        start_time = tuple(start_time.split('-'))[0]

        start_time = str(datetime.datetime.strptime(start_time, '%I:%M %p'))[11:]
        print start_time

        return start_time + config.utc_offset


    def get_end_time(self, end_time):
        """Replace AM or PM with ':00' and concatenate with utc offset."""

        end_time = tuple(end_time.split('-'))[1]

        end_time = str(datetime.datetime.strptime(end_time, '%I:%M %p'))[11:]

        return end_time + config.utc_offset

    def is_only_by_appointment(self):
        return ''

    def is_or_by_appointment(self):
        return ''


    def is_subject_to_change(self):
        # create conditional when/if column is present
        return ''


    def get_start_date(self, start_date):
        start_date = datetime.datetime.strptime(start_date, '%m-%d-%Y').strftime('%Y-%m-%d')
        return start_date + config.utc_offset

    def get_end_date(self, end_date):
        # create conditional when/if column is present
        end_date = datetime.datetime.strptime(end_date, '%m-%d-%Y').strftime('%Y-%m-%d')
        return end_date + config.utc_offset

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
            lambda row: self.get_sch_start_time(row['hours']), axis=1)

        self.base_df['end_time2'] = self.base_df.apply(
            lambda row: self.get_end_time(row['hours']), axis=1)

        self.base_df['is_only_by_appointment2'] = self.base_df.apply(
            lambda row: self.is_only_by_appointment(), axis=1)

        self.base_df['is_or_by_appointment2'] = self.base_df.apply(
            lambda row: self.is_or_by_appointment(), axis=1)

        self.base_df['is_subject_to_change2'] = self.base_df.apply(
            lambda row: self.is_subject_to_change(), axis=1)

        self.base_df['start_date2'] = self.base_df.apply(
            lambda row: self.get_start_date(row['start_date']), axis=1)

        self.base_df['end_date2'] = self.base_df.apply(
            lambda row: self.get_end_date(row['end_date']), axis=1)

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
        sch.drop(['index', 'ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state', 'zip',
                'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment', 'is_or_by_appointment',
                'appointment_phone_num', 'is_subject_to_change', 'directions', 'photo_uri',
                'hours_open_id', 'is_drop_box', 'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'id'], inplace=True, axis=1)

        # Drop base_df columns.
        #sch.drop(['address_line', 'directions', 'hours', 'photo_uri', 'is_drop_box', 'is_early_voting',
        #          'latitude', 'longitude', 'latlng_source', 'id'], inplace=True, axis=1)

        sch = self.dedupe(sch)  # 'address_line' and 'hours are used to identfy/remove duplicates
        #print sch

        sch.drop(['address_line', 'hours'], inplace=True, axis=1)
        #print sch

        sch.rename(columns={'start_time2': 'start_time', 'end_time2': 'end_time',
                            'is_only_by_appointment2': 'is_only_by_appointment',
                            'is_or_by_appointment2': 'is_or_by_appointment',
                            'is_subject_to_change2': 'is_subject_to_change',
                            'start_date2': 'start_date', 'end_date2': 'end_date',
                            'hours_open_id2': 'hours_open_id', 'id2': 'id'}, inplace=True)

        print sch

        sch.to_csv(config.polling_location_output + 'schedule.txt', index=False, encoding='utf-8')  # send to txt file
        sch.to_csv(config.polling_location_output + 'schedule.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':


    early_voting_true = 'true'  # true or false
    #drop_box_true =
    state_file='minnesota_early_voting_info.csv'

    #early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/" + state_file
    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/minnesota/early_voting_input/" + state_file


    colnames = ['ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state', 'zip',
                'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment', 'is_or_by_appointment',
                'appointment_phone_num', 'is_subject_to_change']

    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1
    #print early_voting_df

    ScheduleTxt(early_voting_df).write_schedule_txt()
    #ScheduleTxt(early_voting_df).format_for_schedule()