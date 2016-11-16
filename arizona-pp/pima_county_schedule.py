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
    """Creates VIP 5.1 schedule.txt document.

    """


    def __init__(self, base_df):
        self.base_df = base_df

    def get_start_time(self, index, office_name, start_time):
        """Formats and sets start_time value."""

        if start_time:
            if len(start_time.replace(' - ', '-')) == 13:
                st = '0' + start_time.replace(' - ', '-')
                print st
                return st
            else:
                print start_time
                return start_time.replace(' - ', '-')
        else:
            raise ValueError('Missing start_time value at row ' + str(index) + '.')

    def get_end_time(self, index, end_time):
        """Formats and sets end_time value."""

        if end_time:
            print end_time
            return end_time.replace(' - ', '-')
        else:
            raise ValueError('Missing end_time value at row ' + str(index) + '.')

    def is_only_by_appointment(self):
        return None

    def is_or_by_appointment(self):
        return None

    def is_subject_to_change(self):
        return None

    def get_start_date(self):
        """Sets start_date."""
        return '2016-11-08'

    def get_end_date(self):
        """Sets end_date."""
        return '2016-11-08'

    def get_hours_open_id(self, hours_open_id):
        """Fetches hours_open_id from intermediate_doc.txt and sets value."""
        return hours_open_id

    def create_schedule_id(self, index):
        """Creates sequential id based on row index. Leading zeros are added to maintain consistent id length."""
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
            lambda row: self.get_start_time(row['index'], row['county'], row['start_time']), axis=1)

        self.base_df['end_time'] = self.base_df.apply(
            lambda row: self.get_end_time(row['county'], row['end_time']), axis=1)

        self.base_df['is_only_by_appointment'] = self.base_df.apply(
            lambda row: self.is_only_by_appointment(), axis=1)

        self.base_df['is_or_by_appointment'] = self.base_df.apply(
            lambda row: self.is_or_by_appointment(), axis=1)

        self.base_df['is_subject_to_change'] = self.base_df.apply(
            lambda row: self.is_subject_to_change(), axis=1)

        self.base_df['start_date'] = self.base_df.apply(
            lambda row: self.get_start_date(), axis=1)

        self.base_df['end_date'] = self.base_df.apply(
            lambda row: self.get_end_date(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.get_hours_open_id(row['hours_open_id']), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_schedule_id(row['index']), axis=1)

        return self.base_df

    def write_schedule_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        sch = self.build_schedule_txt()

        # Reorders columns, dropping base_df columns in the process leaving only VIP 5.1 schedule.txt information.
        cols =['start_time', 'end_time', 'is_only_by_appointment', 'is_or_by_appointment', 'is_subject_to_change',
               'start_date', 'end_date', 'hours_open_id', 'id']

        sch = sch.reindex(columns=cols)

        sch.to_csv(config.output + 'schedule.txt', index=False, encoding='utf-8')  # send to txt file
        sch.to_csv(config.output + 'schedule.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':


    intermediate_doc = 'intermediate_doc.csv'

    colnames = ['county', 'official_title', 'types', 'ocd_division', 'division_description', 'homepage_url', 'phone',
                'email', 'directions', 'precinct', 'address', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'notes', 'index',
                'address_line', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box', 'is_early_voting', 'latitude',
                'longitude', 'latlng_source', 'id']

    polling_place_df = pd.read_csv(config.output + intermediate_doc, names=colnames, encoding='ISO-8859-1', skiprows=1, delimiter=',')

    polling_place_df['index'] = polling_place_df.index# offsets zero based index so it starts at 1 for ids
    print polling_place_df.columns

    ScheduleTxt(polling_place_df).write_schedule_txt()