import pandas as pd
import time
import string
import config
import hashlib
import datetime
import re

class PollingLocationTxt(object):
    """

    """

    def __init__(self, base_df, early_voting_true='false', drop_box_true='false'):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true

    def get_address_line(self, index, address1, city, state, zip_code):
        """#"""

        if not pd.isnull(address1):
            #street =  str(street).strip()
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
            zip = zip.replace(u'\xa0', u' ').encode('utf-8')
            #zip = re.sub(r'[^\x00-\x7f]+', r' ', str(zip_code).strip())
            print index, zip
            #zip = str(zip_code).strip()
        else:
            zip = ''
            #raise ValueError('Missing zip code value at row ' + str(index) + '.')

        final_line = str(address1) + ", " + city_name + ', ' + config.state_abbreviation_upper + ' ' + str(zip)

        #final_line = address + ", " + city_name + ", " + state + " " + zip
        final_line = ' '.join(final_line.split())
        print final_line
        return final_line

    def convert_zip_code(self, index, zip_code):
        #supposed to standardize zip code format
        #TODO: add a zero in front of four digit codes(CSV error) and cut 9 digit codes down to five
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
        """#"""
        return ''

    def get_hours(self, index, start_time, end_time):
        """Convert from 24 to 12 hour format."""

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
        """n/a"""
        return ''

    def create_hours_open_id(self, index, address_1, city, state, zip_code):
        """Creates id from substring of a hash value of address_line."""

        address_line = self.get_address_line(index, address_1, city, state, zip_code)
        print index, address_line

        address_line = int(hashlib.sha1(address_line).hexdigest(), 16) % (10 ** 8)

        return 'ho' + str(address_line)

    def is_drop_box(self):
        """#"""
        # TODO: default to False? Make a boolean instance variable passed as a parameter
        return self.drop_box_true

    def is_early_voting(self):
        """#"""
        # TODO: default to True for now. Or Make a boolean instance variable passed as a parameter
        return self.early_voting_true

    def get_latitude(self):
        """n/a"""
        return ''

    def get_longitude(self):
        """n/a"""
        return ''

    def get_latlng_source(self):
        """n/a"""
        return ''

    def create_id(self, index, ocd_division, address_1, city, state, zip_code):
        """Creates id from substring of a hash value of address_line."""

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
        """#"""
        return dupe.drop_duplicates(subset=['address_line'])

    def export_for_schedule_and_locality(self):
        intermediate_doc = self.build_polling_location_txt()

        print intermediate_doc

        intermediate_doc = intermediate_doc.drop_duplicates(subset=['start_time', 'end_time', 'address_line'])

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')
        return intermediate_doc


    def export_for_locality_precinct(self):
        intermediate_doc = self.build_polling_location_txt()

        print intermediate_doc

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')
        return intermediate_doc

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()

        # Drop base_df columns.
        plt.drop(['office_name', 'ocd_division', 'directions', 'precinct', 'name', 'address1', 'city', 'state',
                'zip_code', 'start_time', 'end_time', 'index'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt

        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    colnames = ['office_name', 'ocd_division', 'directions', 'precinct', 'name', 'address1', 'city', 'state',
                'zip_code', 'start_time', 'end_time', 'index']

    polling_place_df = pd.read_csv(config.input_path + config.state_file, names=colnames, sep=',', encoding='ISO-8859-1', skiprows=1)
    print polling_place_df

    polling_place_df['index'] = polling_place_df.index

    pl = PollingLocationTxt(polling_place_df, config.early_voting)
    pl.export_for_schedule_and_locality()
    pl.write_polling_location_txt()


