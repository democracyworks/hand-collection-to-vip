import pandas as pd
import config
import hashlib
import datetime
import re

class PollingLocationTxt(object):
    """

    """

    def __init__(self, base_df, state, drop_box_true='false'):
        self.base_df = base_df
        self.state_abbreviation = state
        self.drop_box_true = drop_box_true


    def get_address_line(self, index, address1, address2, city, state, zip_code):
        # required: print message for exception

        print index, city

        if not pd.isnull(address1):
            #address = street
            address1 = str(re.sub(r'[^\x00-\x7f]', r' ', address1.strip()))
            #address1 = ' '.join(address1.split())
            print 1, address1
            #print type(address)
        else:
            raise ValueError('Missing street value at row ' + str(index) + '.')
            #address = ''

        if not pd.isnull(address2):
            #address = street
            address2 = ', ' + str(re.sub(r'[^\x00-\x7f]', r' ', address2.strip()))
            #address2 = ' '.join(address2.split())
            #address = address1 + ' ' + address2
            #print address2
            #print type(address)
        else:
            address2 = ''

        if not pd.isnull(city):
            city_name = str(city)
            city_name = ''.join([i if ord(i) < 128 else ' ' for i in str(city_name)])
            print city_name
        else:
            raise ValueError('Missing city value at row ' + str(index) + '.')
            #city_name =''

        if not pd.isnull(zip_code):
            zip = str(zip_code)
            zip = ', ' + str(re.sub(r'[^\x00-\x7f]', r' ', zip.strip()))
            #print zip
            #print type(zip)

        else:
            zip = ''
            #raise ValueError('Missing zip code value at row ' + str(index) + '.')

        #print address
        #print type(address)

        final_line = address1 +  address2 + ", " + city_name + ', ' + config.state_abbreviation_upper + ' ' + zip
        final_line = ' '.join(final_line.split())
        #print index, final_line
        return final_line



    def get_directions(self):
        """#"""
        # no direct relationship to any column
        return ''

    def get_hours(self, index, start_time, end_time):
        """Convert from 24 to 12 hour format."""
        print index, start_time, end_time

        #hour_min_time_format = "%H:%M"
        #hour_min_sec_time_format= "%H:%M:%S"
        start_time = tuple(start_time.split('-'))[0].strip()
        end_time = tuple(end_time.split('-'))[0].strip()

        if len(start_time) == 4:
            start_time = '0' + start_time + ':00'
        elif len(start_time) == 5:
            start_time = start_time + ':00'
        else:
            start_time = start_time

        if len(end_time) == 4:
            end_time = '0' + end_time + ':00'
        elif len(end_time) == 5:
            end_time = end_time + ':00'
        else:
            end_time = end_time

        #if hour_min_sec_time_format:
        #print start_time
            #start_time = tuple(start_time.split('-'))[0].strip()
        print index, start_time
        start_time = datetime.datetime.strptime(start_time, "%H:%M:%S").strftime("%I:%M %p")
        print start_time
        #elif hour_min_time_format:
        #start_time = datetime.datetime.strptime(start_time, "%H:%M").strftime("%I:%M %p")
        #print start_time



        end_time = datetime.datetime.strptime(end_time, "%H:%M:%S").strftime("%I:%M %p")
        print end_time

        return start_time + '-' + end_time

    def get_photo_uri(self):
        # create conditional when/if column is present
        return ''

    def create_hours_open_id(self, index, address1, address2, city, state, zip_code):
        """#"""

        address_line = self.get_address_line(index, address1, address2, city, state, zip_code)
        #print address_line

        address_line = int(hashlib.sha1(address_line).hexdigest(), 16) % (10 ** 8)

        return 'ho' + str(address_line)

    def is_drop_box(self):
        """#"""
        # TODO: default to False? Make a boolean instance variable passed as a parameter
        return self.drop_box_true

    def is_early_voting(self):
        """Returns early_voting parameter set in the congig file"""
        return config.early_voting

    def get_latitude(self):
        # create conditional when/if column is present
        return ''

    def get_longitude(self):
        # create conditional when/if column is present
        return ''

    def get_latlng_source(self):
        # create conditional when/if column is present
        return ''

    def create_id(self, index, ocd_division, address_1, address_2, city, state, zip_code):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length

        address_line = self.get_address_line(index, address_1, address_2, city, state, zip_code)

        id =  int(hashlib.sha1(ocd_division + address_line).hexdigest(), 16) % (10 ** 8)
        id = 'poll' + str(id)
        print 'LOOK', address_line, id
        return id

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'], row['address1'], row['address2'],
                                              row['city'], row['state'], row['zip_code']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index'], row['address1'], row['address2'],
                                              row['city'], row['state'], row['zip_code']), axis=1)

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
            lambda row: self.create_id(row['index'], row['ocd_division'], row['address1'], row['address2'],
                                              row['city'], row['state'], row['zip_code']), axis=1)

        return self.base_df

    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset=['address_line'])

    def export_for_schedule_and_locality(self):
        intermediate_doc = self.build_polling_location_txt()

        print intermediate_doc
        #intermediate_doc = self.dedupe(intermediate_doc)

        intermediate_doc = intermediate_doc.drop_duplicates(subset=['start_time', 'end_time', 'start_date',
                                                                    'end_date', 'address_line'])

        #intermediate_doc.rename(columns={'id': 'polling_location_id'}, inplace=True)

        #intermediate_doc = intermediate_doc.drop_duplicates(subset=['address_line', 'hours'])
        #print intermediate_doc

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')
        return intermediate_doc

    def export_for_locality(self):
        return self.build_polling_location_txt()

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()

        # Drop base_df columns.
        plt.drop(['index', 'office_name', 'ocd_division', 'homepage_url', 'phone', 'email', 'directions', 'location_name', 'address1',
                'address2', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt

        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    s = 'office_name ocd_division homepage_url phone email directions name address1 address2 city state zip start_time end_time start_date end_date is_subject_to_change notes'.split(' ')
    print s

    state = config.state_abbreviation_upper

    #state_file='texas_early_voting_info_clean.csv'

    colnames = ['office_name', 'ocd_division', 'homepage_url', 'phone', 'email', 'directions', 'location_name', 'address1',
                'address2', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date']


    usecols = ['office_name', 'ocd_division', 'homepage_url', 'phone', 'email', 'directions', 'location_name', 'address1',
                'address2', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date']



    early_voting_df = pd.read_csv(config.input + config.state_file, names=colnames,
                                  usecols=usecols,
                                  encoding='ISO-8859-1',
                                  skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1
    print early_voting_df

    pl = PollingLocationTxt(early_voting_df, state)

    #pl.write_polling_location_txt()
    pl.export_for_schedule_and_locality()

