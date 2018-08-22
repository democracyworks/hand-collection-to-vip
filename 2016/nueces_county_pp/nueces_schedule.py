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


    def get_start_time(self, index, start_time):
        """#"""
        hours_arr = start_time.split("-")
        time = hours_arr[0].strip()
        offset = hours_arr[1].strip()

        if len(time)<8:
            time = "0" + time

        if len(offset) < 5:
            offset = "0" + offset

        return time + "-" + offset


#        diff1 = '11-07'
#        diff2 = '11-06'
#        try:
#            if county in mountain_counties:
#                if diff1 in start_date or diff2 in start_date:
#                    utc_offset = config.mountain_utc_offset_7
#                    return start_time + utc_offset
#                else:
#                    utc_offset = config.mountain_utc_offset_6
#                    return start_time + utc_offset

#            elif diff1 in start_date or diff2 in start_date:
#                    utc_offset = config.central_utc_offset_6
#                    return start_time + utc_offset
#            else:
#                utc_offset = config.central_utc_offset_5
#                return start_time + utc_offset
#        except:
#            pass

        #else:

        #    raise ValueError('Missing or invalid data at row ' + str(index))

        #start = tuple(start_time.split(' - '))[0]

        #utc_offset = tuple(start_time.split(' - '))[1]
        #utc_offset = '0' + utc_offset

        #start_time = start + '-' + utc_offset

        #start_time = start_time.replace(' - ', '-')
        #if len(start_time) == 13:
        #    return '0' + start_time
        #else:

        #print index, start_time
        #return  start_time

    def get_end_time(self, office_name, end_time):
        """#"""

        hours_arr = end_time.split("-")
        time = hours_arr[0].strip()
        offset = hours_arr[1].strip()

        if len(time) < 8:
            time = "0" + time

        if len(offset) < 5:
            offset = "0" + offset

        return time + "-" + offset

    def is_only_by_appointment(self):
        return None

    def is_or_by_appointment(self):
        return None

    def is_subject_to_change(self):
        # create conditional when/if column is present
        return None

#    def format_date(self, start_date):

#        start_date = tuple(start_date.split('/'))

#        month = start_date[0]
#        day = start_date [1]
#        year = start_date[2]

#        if len(month) == 1:
#            month = '0' + month
#        else:
#            month = month

#        if len(day) == 1:
#            day = '0' + day
#        else:
#            day = day

#        return month + '/' + day + '/' + year


    def get_start_date(self, index):
        """#"""
        #print index, start_date

        return '2016-11-08'



        #elif len(start_date) == 8:
        #    print index, start_date
        #    sd = tuple(start_date.split('-'))
        #    m = sd[0]
        #    d = sd[1]
        #    return '2016-' + m + d

            #end_date = datetime.datetime.strptime(start_date, '%m-%d-%y').strftime('%Y-%m-%d')
            #print index, end_date
            #return end_date




    def get_end_date(self, index):
        """#"""

        return '2016-11-08'

    def get_hours_open_id(self, hours_open_id):
        """#"""
        print hours_open_id
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
            lambda row: self.get_start_time(row['index'], row['start_time']), axis=1)

        self.base_df['end_time'] = self.base_df.apply(
            lambda row: self.get_end_time(row['index'], row['end_time']), axis=1)

        self.base_df['is_only_by_appointment'] = self.base_df.apply(
            lambda row: self.is_only_by_appointment(), axis=1)

        self.base_df['is_or_by_appointment'] = self.base_df.apply(
            lambda row: self.is_or_by_appointment(), axis=1)

        self.base_df['is_subject_to_change'] = self.base_df.apply(
            lambda row: self.is_subject_to_change(), axis=1)

        self.base_df['start_date'] = self.base_df.apply(
            lambda row: self.get_start_date(row['index']), axis=1)

        self.base_df['end_date'] = self.base_df.apply(
            lambda row: self.get_end_date(row['index']), axis=1)

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
        sch.drop(['office-name', 'ocd_division', 'directions', 'precinct_name', 'loc_name', 'street', 'city', 'state',
                'zip_code', 'notes',
     'index', 'name', 'address_line', 'hours', 'photo_uri',  'is_drop_box', 'is_early_voting', 'latitude',
     'longitude', 'latlng_source', 'source_id'], inplace=True, axis=1)

        # reorder columns
        cols =['start_time', 'end_time', 'is_only_by_appointment', 'is_or_by_appointment', 'is_subject_to_change',
               'start_date', 'end_date', 'hours_open_id', 'id']

        sch = sch.reindex(columns=cols)
        print sch

        sch.to_csv(config.output + 'schedule.txt', index=False, encoding='utf-8')  # send to txt file
        sch.to_csv(config.output + 'schedule.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    #s ='county ocd_division homepage_url phone email directions location_name address1 address2 city state zip_code start_time end_time start_date end_date is_subject_to_change notes index address_line hours photo_uri hours_open_id is_drop_box is_early_voting latitude longitude latlng_source id'.split(' ')
    #print s

    intermediate_doc = 'intermediate_doc.csv'

    colnames = ['office-name', 'ocd_division', 'directions', 'precinct_name', 'loc_name', 'street', 'city', 'state',
                'zip_code', 'start_time', 'end_time', 'notes',
     'index', 'name', 'address_line', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box', 'is_early_voting', 'latitude',
     'longitude', 'latlng_source', 'source_id']
    print len(colnames)

    early_voting_df = pd.read_csv(config.output + intermediate_doc, names=colnames, encoding='ISO-8859-1', skiprows=1, delimiter=',')

    early_voting_df['index'] = early_voting_df.index +1  # offsets zero based index so it starts at 1 for ids
    print early_voting_df
    print early_voting_df.hours_open_id

    ScheduleTxt(early_voting_df).write_schedule_txt()
    #ScheduleTxt(early_voting_df).format_for_schedule()