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

class ScheduleTxt(object):
    """

    """


    def __init__(self, base_df):
        self.base_df = base_df

    def get_start_time(self, start_time):
        #return self.convert_to_uct()
        return start_time.replace(' - ', '-')

    def get_end_time(self, end_time):
        """#"""
        return end_time.replace(' - ', '-')


    def is_only_by_appointment(self):
        return None

    def is_or_by_appointment(self):
        return None

    def is_subject_to_change(self):
        # create conditional when/if column is present
        return None

    def format_date(self, start_date):

        start_date = tuple(start_date.split('-'))
        print start_date

        month = start_date[0]
        day = start_date [1]
        year = '2016'

        if len(month) == 1:
            month = '0' + month
        else:
            month = month

        if len(day) == 1:
            day = '0' + day
        else:
            day = day

        return month + '/' + day + '/' + year


    def get_start_date(self, start_date):

        mdy = self.format_date(start_date)
        start_date = datetime.datetime.strptime(mdy, '%m/%d/%Y').strftime('%Y-%m-%d')

        diff1 = '11-07'
        diff2 = '11-06'

        if diff1 in start_date or diff2 in start_date:
            utc_offset = config.utc_offset_5
            return start_date
        else:
            utc_offset = config.utc_offset_4
            return start_date

    def get_end_date(self, end_date, start_date):
        # create conditional when/if column is present
        #end_date = tuple(end_date.split('/'))

        mdy = self.format_date(end_date)
        end_date = datetime.datetime.strptime(mdy, '%m/%d/%Y').strftime('%Y-%m-%d')

        diff1 = '11-07'
        diff2 = '11-06'

        if diff1 in start_date or diff2 in start_date:
            #utc_offset = config.utc_offset_5
            return end_date

        elif diff1 in end_date or diff2 in end_date:
            #utc_offset = config.utc_offset_5
            return end_date

        else:
            #utc_offset = config.utc_offset_4
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

        self.base_df['start_time'] = self.base_df.apply(
            lambda row: self.get_start_time(row['start_time']), axis=1)

        self.base_df['end_time'] = self.base_df.apply(
            lambda row: self.get_end_time(row['end_time']), axis=1)

        self.base_df['is_only_by_appointment'] = self.base_df.apply(
            lambda row: self.is_only_by_appointment(), axis=1)

        self.base_df['is_or_by_appointment'] = self.base_df.apply(
            lambda row: self.is_or_by_appointment(), axis=1)

        self.base_df['is_subject_to_change'] = self.base_df.apply(
            lambda row: self.is_subject_to_change(), axis=1)

        self.base_df['start_date'] = self.base_df.apply(
            lambda row: self.get_start_date(row['start_date']), axis=1)

        self.base_df['end_date'] = self.base_df.apply(
            lambda row: self.get_end_date(row['end_date'], row['start_date']), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.get_hours_open_id(row['hours_open_id']), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_schedule_id(row['index']), axis=1)

        return self.base_df

    def write_schedule_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        sch = self.build_schedule_txt()
        #print sch

        # Drop base_df columns.
        sch.drop(['office_name', 'official_title', 'types', 'ocd_division', 'division_description', 'phone', 'email',
                'street', 'city', 'state', 'zip_code', 'notes',
                'index', 'address_line', 'directions', 'hours', 'photo_uri', 'is_drop_box',
                'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'polling_location_id'], inplace=True, axis=1)

        # reorder columns
        cols =['start_time', 'end_time', 'is_only_by_appointment', 'is_or_by_appointment', 'is_subject_to_change',
               'start_date', 'end_date', 'hours_open_id', 'id']

        sch = sch.reindex(columns=cols)
        print sch

        sch.to_csv(config.output + 'schedule.txt', index=False, encoding='utf-8')  # send to txt file
        sch.to_csv(config.output + 'schedule.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    state_file = 'intermediate_doc.csv'

    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/montana/output/" + state_file

    colnames = ['office_name', 'official_title', 'types', 'ocd_division', 'division_description', 'phone', 'email',
                'street', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'notes',
                'index', 'address_line', 'directions', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box',
                'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'polling_location_id']

    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1, delimiter=',')

    early_voting_df['index'] = early_voting_df.index +1 # offsets zero based index so it starts at 1 for ids
    #print early_voting_df

    ScheduleTxt(early_voting_df).write_schedule_txt()
    #ScheduleTxt(early_voting_df).format_for_schedule()