"""
polling_location columns:
address_line,
directions,
hours,
photo_uri,
hours_open_id,
is_drop_box,
is_early_voting,
latitude,
longitude,
latlng_source,
id

"""


import pandas as pd
import time
import config
import hashlib
from time import strftime

class PollingLocationTxt(object):
    """

    """

    def __init__(self, base_df, early_voting_true='false', drop_box_true='false'):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true


    def get_address_line(self, index, address_1, address_2, city, zip_code):
        # required: print message for exception
        # TODO: concatenate street, city, state and zip
        if address_1 and not pd.isnull(address_2):
            address = str(address_1) + ' ' + str(address_2)
        elif address_1:
            address = address_1
        else:
            address = ''
            print 'Missing address value at row ' + index + '.'

        if city:
            city = city
        else:
            city = ''
            print 'Missing city value at row ' + index + '.'

        if zip_code:
            # TODO: add zip code validation
            zip_code = str(zip_code)
            # print zip_code
        else:
            zip_code = ''
            print 'Missing zip_code value at row ' + index + '.'
        address_line = str(address) + ', ' + str(city) + ', MN ' + str(zip_code)

        return address_line

    def get_directions(self):
        """#"""
        # no direct relationship to any column
        return ''

    def convert_to_time(self, index, time):
       if not pd.isnull(time):
            time_str = str(int(time))
            if len(time_str) == 4:
                hours = time_str[0:2]
                mins = time_str[2:]
                time_str = hours + ":" + mins
                return time_str
            elif len(time_str) == 3:
                hours = time_str[0]
                mins = time_str[1:]
                time_str = hours + ":" +mins
                return time_str
            else:
                print 'Hours were not in the proper format in line ' + str(index) + '.'
                return ''
       else:
            return ""

    def convert_to_twelve_hour(self, index, time_str):
        if not pd.isnull(time_str):
            d = time.strptime(time_str, "%H:%M")
            formatted_time = time.strftime("%I:%M %p", d)
            return formatted_time
        else:
            return ''


    def get_hours(self, index, start_time, end_time):
        # create conditional when/if column is present
        if not pd.isnull(start_time) and not pd.isnull(end_time):
            start_time = self.convert_to_time(index, start_time)

            end_time = self.convert_to_time(index, end_time)

            formatted_start_str = self.convert_to_twelve_hour(index, start_time)

            formatted_end_str = self.convert_to_twelve_hour(index, end_time)

            hours_str = formatted_start_str + "-" + formatted_end_str

            return hours_str
        else:
            # print 'Hours not presented in the right format in line ' + str(index) +"."
            return ''

    def get_photo_uri(self):
        # create conditional when/if column is present
        return ''

    def create_hours_open_id(self, index, address_1, address_2, city, zip_code):
        """#"""
        # TODO: this is the correct id/index code, correct everywhere

        address_line = self.get_address_line(index, address_1, address_2, city, zip_code)
        #print address_line

        address_line = int(hashlib.sha1(address_line).hexdigest(), 16) % (10 ** 8)

        return 'ho' + str(address_line)

    def is_drop_box(self):
        """#"""
        # TODO: default to false? Make a boolean instance variable passed as a parameter
        return self.drop_box_true

    def is_early_voting(self):
        """#"""
        # TODO: default to True for now. Or Make a boolean instance variable passed as a parameter
        return self.early_voting_true

    def get_latitude(self):
        # create conditional when/if column is present
        return ''

    def get_longitude(self):
        # create conditional when/if column is present
        return ''

    def get_latlng_source(self):
        # create conditional when/if column is present
        return ''

    def create_id(self, index, ocd_division, street, city, state, zip_code):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length

        address_line = self.get_address_line(index, street, city, state, zip_code)

        id =  int(hashlib.sha1(config.state + address_line).hexdigest(), 16) % (10 ** 8)
        return 'poll' + str(id)

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'], row['address_1'], row['address_2'],
                                              row['city'], row['zip']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index'], row['address_1'], row['address_2'],
                                              row['city'], row['zip']), axis=1)

        self.base_df['is_drop_box'] = self.base_df.apply(
            lambda row: self.is_drop_box(), axis=1)

        self.base_df['is_early_voting'] = self.base_df.apply(
            lambda row: self.is_early_voting(), axis=1)

        self.base_df['latitude'] = self.base_df.apply(
            lambda row: self.get_latitude(), axis=1)

        self.base_df['longitude'] = self.base_df.apply(
            lambda row: self.get_longitude(), axis=1)

        self.base_df['latlng_source'] = self.base_df.apply(
            lambda row: self.get_latlng_source(), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['index'], row['ocd_division'], row['address_1'], row['address_2'],
                                       row['city'], row['zip']), axis=1)

        return self.base_df

    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset=['ocd_division','address_line'])

    def export_for_schedule_and_locality(self):
        intermediate_doc = self.build_polling_location_txt()

        print intermediate_doc
        #intermediate_doc = self.dedupe(intermediate_doc)

        intermediate_doc = intermediate_doc.drop_duplicates(subset=['start_time', 'end_time', 'start_date',
                                                                    'end_date', 'address_line'])

        #intermediate_doc = intermediate_doc.drop_duplicates(subset=['address_line', 'hours'])
        print intermediate_doc

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()
        #print plt

        # Drop base_df columns.
     #   plt.drop(['index', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state', 'zip',
     #           'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment', 'is_or_by_appointment',
     #           'is_subject_to_change'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt

        # reorder columns
        cols =['address_line', 'directions', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box', 'is_early_voting',
               'latitude', 'longitude', 'latlng_source', 'id']

        plt = plt.reindex(columns=cols)

        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':
    s = 'ocd_division, county, location_name, address1, address2, dirs, city, state, zip, start_time, end_time, start_date, end_date, is_only_by_appointment, is_or_by_appointment, is_subject_to_change'.split(', ')
    print s
    early_voting_true = 'true'  # true or false
    #drop_box_true =
    #state_file='minnesota_early_voting_info.csv'

    state_file = 'Minnesota Early Voting Information - Clean (4).csv'

    #early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/" + state_file
    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/minnesota/early_voting_input/" + state_file


    colnames = ['ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'dirs', 'city', 'state', 'zip',
                'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment', 'is_or_by_appointment',
                'is_subject_to_change']


    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1

    pl = PollingLocationTxt(early_voting_df, early_voting_true)
    #pl.write_polling_location_txt()
    pl.export_for_schedule_and_locality()
    #pl.export_for_schedule()