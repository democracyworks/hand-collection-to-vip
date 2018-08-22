import pandas as pd
import time
import config
import hashlib
import datetime
import re

class PollingLocationTxt(object):
    """Creates VIP 5.1 polling_location.txt document.

    """

    def __init__(self, base_df, early_voting_true='false', drop_box_true='false'):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true

    def get_address_line(self, index, address1, city, state, zip_code):
        """Assembles the address_line value from data taken from multiple dataframe columns."""

        if not pd.isnull(address1):
            address = str(re.sub(r'[^\x00-\x7f]', r' ', address1.strip()))
            address = ' '.join(address.split())
        else:
            raise ValueError('Missing street value at row ' + str(index) + '.')

        if city:
            print index, city
            city_name = city.strip()
        else:
            raise ValueError('Missing city value at row ' + str(index) + '.')

        if not pd.isnull(zip_code):
            print zip_code
            zip = ''.join([i if ord(i) < 128 else ' ' for i in str(zip_code).strip()])
        else:
            zip = ''
            #raise ValueError('Missing zip code value at row ' + str(index) + '.')

        final_line = str(address1) + ", " + city_name + ', ' + config.state_abbreviation_upper + ' ' + str(zip)

        final_line = ' '.join(final_line.split())
        print final_line
        return final_line

    def convert_zip_code(self, index, zip_code):
        """Standardizes zip code format."""
        if len(str(zip_code)) == 5:
            text = str(zip_code)
        elif len(str(zip_code)) == 4:
            text = "0" + str(zip_code)
        elif len(str(zip_code)) == 10:
            text = str(zip_code)
            text = text.split("-")[0]
        else:
            text = ''
            print 'Zip code not valid for line ' + str(index) + "."
        return text


    def get_directions(self):
        """Sets directions value."""
        return ''

    def get_hours(self, index, start_time, end_time):
        """Converts time from 24 to 12 hour format."""

        print index, end_time
        print start_time, end_time
        start_time = str(start_time)
        start_time = tuple(start_time.split(' - '))[0]
        start_time = datetime.datetime.strptime(start_time, "%H:%M:%S").strftime("%I:%M %p")
        #print start_time

        end_time = str(end_time)
        end_time = tuple(end_time.split(' - '))[0]
        end_time = datetime.datetime.strptime(end_time, "%H:%M:%S").strftime("%I:%M %p")
        #print end_time

        return start_time + '-' + end_time

    def get_photo_uri(self):
        """Not a required field. No data available."""
        return ''

    def create_hours_open_id(self, index, address_1, city, state, zip_code):
        """Creates the hours_open_id by prepending 'ho' to a hash of the address_line created with
        the get_address_line method.
        """

        address_line = self.get_address_line(index, address_1, city, state, zip_code)
        print index, address_line

        hours_open_id = int(hashlib.sha1(address_line).hexdigest(), 16) % (10 ** 8)

        return 'ho' + str(hours_open_id)

    def is_drop_box(self):
        """Sets is_drop_box boolean value. Defaults to false."""
        return self.drop_box_true

    def is_early_voting(self):
        """Sets is_early_voting value. Hard coded as 'false' for polling place states."""
        return 'false'

    def get_latitude(self):
        """Not a required field. No data available."""
        return ''

    def get_longitude(self):
        """Not a required field. No data available."""
        return ''

    def get_latlng_source(self):
        """Not a required field. No data available."""
        return ''

    def create_id(self, index, ocd_division, address_1, city, state, zip_code):
        """Creates polling_location_id by prepending 'poll' to a substring of the hash value of the address_line value
        created with the get_address_line method.
        """

        address_line = self.get_address_line(index, address_1, city, state, zip_code)

        id =  int(hashlib.sha1(str(ocd_division) + address_line).hexdigest(), 16) % (10 ** 8)
        id = 'poll' + str(id)
        return id

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'], row['address1'], row['city'], row['state'],
                                              row['zip_code']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index'], row['address1'], row['city'],
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
            lambda row: self.create_id(row['index'], row['ocd_division'], row['address1'], row['city'], row['state'],
                                       row['zip_code']), axis=1)

        return self.base_df

    def dedupe(self, dupe):
        """Removes duplicate rows when called from the write method."""
        return dupe.drop_duplicates(subset=['id'])

    def export_for_schedule_and_locality(self):
        """Generates output containing information for use in the schedule and locality scripts."""
        intermediate_doc = self.build_polling_location_txt()

        intermediate_doc = intermediate_doc.drop_duplicates(subset=['start_time', 'end_time', 'address_line'])

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')
        return intermediate_doc


    def export_for_locality_precinct(self):
        """Generates output containing information for use in the precinct script."""
        intermediate_doc = self.build_polling_location_txt()

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')
        return intermediate_doc

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()


        # Drop base_df columns.
        plt.drop(['county', 'official_title', 'ocd_division', 'homepage_url', 'phone', 'email', 'directions',
                'precinct', 'name', 'address1', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'index'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt

        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    # Columns names from polling_place data as they'll be referenced in PollingLocationTxt().
    colnames = ['county', 'official_title', 'ocd_division', 'homepage_url', 'phone', 'email', 'directions',
                'precinct', 'name', 'address1', 'city', 'state', 'zip_code', 'start_time', 'end_time']

    # Polling place data is read into a dataframe. The input path and file name are specified in config.py.
    polling_place_df = pd.read_csv(config.input_path + config.state_file, names=colnames, sep=',', encoding='ISO-8859-1', skiprows=1)
    print polling_place_df

    polling_place_df['index'] = polling_place_df.index

    # Creates an instance of PollingLocationTxt(), polling_place_df is passed to the instance.
    pl = PollingLocationTxt(polling_place_df, config.early_voting)

    # These methods generate intermediate_doc.txt and polling_location.txt respectively.
    pl.export_for_schedule_and_locality()
    pl.write_polling_location_txt()


