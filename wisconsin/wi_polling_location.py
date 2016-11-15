import pandas as pd
import time
import config
from time import strftime
from datetime import datetime
import hashlib
import re

class PollingLocationTxt(object):
    """

    """

    def __init__(self, base_df, early_voting_true=False, drop_box_true="false"):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true

    def get_address_line(self, index, address1, address2, city, state, zip_code):
        """#"""
        print 'wtf'

        if not pd.isnull(address1):
            address1 = ''.join([i if ord(i) < 128 else ' ' for i in address1.strip()])
            #address = ' '.join(address.split())
        else:
            pass
            #raise ValueError('Missing street value at row ' + str(index) + '.')

        if not pd.isnull(address2):
            address2 = ''.join([i if ord(i) < 128 else ' ' for i in address2.strip()])
            #address2 = ' '.join(address2.split())
        else:
            address2 = ''

        if city:
            city_name = str(city).strip()
        else:
            raise ValueError('Missing city value at row ' + str(index) + '.')

        if zip_code:
            print zip_code
            zip = ''.join([i if ord(i) < 128 else ' ' for i in str(zip_code).strip()])
            print index, zip
        else:
            raise ValueError('Missing zip code value at row ' + str(index) + '.')

        final_line = str(address1) + str(address2) + ", " + city_name + ', ' + config.state_abbreviation_upper + ' ' + zip
        final_line = ' '.join(final_line.split())
        print index, final_line
        return final_line

    def get_directions(self):
        """#"""
        # no direct relationship to any column
        return ''

    def get_start_time(self, time):
        arr = time.split(" ")
        hours = arr[0].split(":")[0] + ":" + arr[0].split(":")[1]
        return hours + " " + "AM"

    def get_end_time(self, time):
        arr = time.split(" ")
        hours = arr[0].split(":")[0] + ":" + arr[0].split(":")[1]
        return hours + " " + "PM"

    def get_hours(self, index, start_time, end_time):
        #d = datetime.strptime("10:30", "%H:%M")
        print start_time
        print end_time
        try:
            start_time = datetime.strptime(start_time.split(' - ')[0], "%H:%M:%S").strftime("%I:%M %p")

            end_time = datetime.strptime(end_time.split(' - ')[0], "%H:%M:%S").strftime("%I:%M %p")

            return start_time + "-" + end_time
        except:
            pass



    def convert_hours(self):
        pass

    def get_photo_uri(self):
        """#"""
        return ''

    def create_hours_open_id(self, index, address1, address2, city, state, zip_code):
        """#"""
        address_line = self.get_address_line(index, address1, address2, city, state, zip_code)

        return 'ho' + str(int(hashlib.sha1(address_line).hexdigest(), 16) % (10 ** 8))

    def is_drop_box(self):
        """Defaults to false."""
        # TODO: default to False? Make a boolean instance variable passed as a parameter
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

    def create_id(self, index, ocd_division, address1, address2, city, state, zip_code):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length
        if not pd.isnull(ocd_division):
            address_line = self.get_address_line(index, address1, address2, city, state, zip_code)
            id = int(hashlib.sha1(ocd_division + address_line).hexdigest(), 16) % (10 ** 8)
            id = 'poll' + str(id)
            return id

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'], row['address1'], row['address2'], row['city'],
                                              row['state'], row['zip_code']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)
        #
        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index'], row['address1'], row['address2'], row['city'],
                                              row['state'], row['zip_code']), axis=1)

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
            lambda row: self.create_id(row['index'], row['ocd_division'],row['address1'], row['address2'],
                                       row['city'], row['state'], row['zip_code']), axis=1)

        return self.base_df

    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset=['address_line'])

    def dedupe_for_sch(self, dupe):
        return dupe.drop_duplicates(subset=['address_line', 'hours', 'start_date'])

    def group_by_address(self, item):
        item = item.groupby('address_line').agg(lambda x: ' '.join(set(x))).reset_index()

    def export_for_schedule_and_locality(self):
        intermediate_doc = self.build_polling_location_txt()

        # print intermediate_doc
        # intermediate_doc = self.dedupe(intermediate_doc)

        intermediate_doc = intermediate_doc.drop_duplicates(subset=['start_time', 'end_time', 'start_date',
                                                                    'end_date', 'address_line'])

        intermediate_doc = intermediate_doc[intermediate_doc.address_line.notnull()]

        intermediate_doc.to_csv(config.output + 'intermediate_doc2.csv', index=False, encoding='utf-8')
        return intermediate_doc

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()

        # Drop base_df columns.
        plt.drop(['index', 'office_name', 'official_name', 'phone', 'email', 'ocd_division', 'phone', 'location_name',
                'address1', 'address2', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date',
                'end_date', 'is_only_by_appointment', 'is_or_by_appointment', 'directions', 'notes'],
                 inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt
        plt = plt[plt.address_line.notnull()]

        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    #s = 'office_name official_name phone email ocd_division phone location_name address1 address2 city state zip_code start_time end_time start_date end_date is_only_by_appointment is_or_by_appointment directions notes'.split(' ')
    #print s

    s = 'county, official_name, phone, email, ocd_division, phone, location_name, address1, address2, city, state, zip_code, start_time, end_time, start_date, end_date, is_only_by_appointment, is_or_by_appointment, directions, notes, index, address_line, hours, photo_uri, hours_open_id, is_drop_box, is_early_voting, latitude, longitude, latlng_source, id'.split(', ')
    print s


    colnames = ['office_name', 'official_name', 'phone', 'email', 'ocd_division', 'phone', 'location_name',
                'address1', 'address2', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date',
                'end_date', 'is_only_by_appointment', 'is_or_by_appointment', 'directions', 'notes']


    #usecols = ['office_name', 'official_name', 'phone', 'email', 'ocd_division', 'phone', 'location_name',
    #            'address1', 'address2', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date',
    #            'end_date', 'is_only_by_appointment', 'is_or_by_appointment', 'directions']


    early_voting_df = pd.read_csv(config.input + config.state_file, names=colnames, sep=',', encoding='ISO-8859-1', skiprows=1, dtype={'zip': str})
    early_voting_df['index'] = early_voting_df.index + 1
    print early_voting_df

    pl = PollingLocationTxt(early_voting_df, config.early_voting)
    #pl.export_for_schedule_and_locality()
    pl.write_polling_location_txt()