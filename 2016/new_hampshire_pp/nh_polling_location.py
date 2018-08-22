import pandas as pd
import time
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

    def get_address_line(self, index, street, city, state, zip_code):
        """#"""

        if not pd.isnull(street):
            #street =  str(street).strip()
            address = str(re.sub(r'[^\x00-\x7f]', r' ', street.strip()))
            address = ' '.join(address.split())
        else:
            raise ValueError('Missing street value at row ' + str(index) + '.')

        if city:
            city_name = city.strip()
        else:
            raise ValueError('Missing city value at row ' + str(index) + '.')

        if zip_code:
            print zip_code
            zip = ''.join([i if ord(i) < 128 else ' ' for i in str(zip_code)])
            #zip = re.sub(r'[^\x00-\x7f]+', r' ', str(zip_code).strip())
            #print index, zip
            #zip = str(zip_code).strip()
            zip = self.convert_zip_code(index, zip)
            print zip
        else:
            raise ValueError('Missing zip code value at row ' + str(index) + '.')

        final_line = address + ", " + city_name + ", " + state + " " + zip
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
        # no direct relationship to any column
        return ''

    def get_hours(self, index, start_time, end_time):
        """Convert from 24 to 12 hour format."""
        print index, start_time
        start_time = tuple(start_time.split(' - '))[0]
        start_time = datetime.datetime.strptime(start_time, "%H:%M:%S").strftime("%I:%M %p")
        #print start_time

        end_time = tuple(end_time.split(' - '))[0]
        end_time = datetime.datetime.strptime(end_time, "%H:%M:%S").strftime("%I:%M %p")
        #print end_time

        return start_time + '-' + end_time

    def get_photo_uri(self):
        # create conditional when/if column is present
        return ''

    def create_hours_open_id(self, index, address_1, address_2, city, zip_code):
        """#"""

        address_line = self.get_address_line(index, address_1, address_2, city, zip_code)
        #print address_line

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
        # create conditional when/if column is present
        return ''

    def get_longitude(self):
        # create conditional when/if column is present
        return ''

    def get_latlng_source(self):
        # create conditional when/if column is present
        return ''

    def create_id(self, index, ocd_division, address_1, address_2, city, zip_code):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length

        address_line = self.get_address_line(index, address_1, address_2, city, zip_code)

        id =  int(hashlib.sha1(ocd_division + address_line).hexdigest(), 16) % (10 ** 8)
        id = 'poll' + str(id)
        return id

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'], row['street'], row['city'], row['state'],
                                              row['zip_code']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index'], row['street'],
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
            lambda row: self.create_id(row['index'], row['ocd_division'], row['street'],
                                              row['city'], row['state'], row['zip_code']), axis=1)

        return self.base_df

    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset=['ocd_division', 'address_line'])

    def export_for_schedule_and_locality(self):
        intermediate_doc = self.build_polling_location_txt()

        print intermediate_doc
        #intermediate_doc = self.dedupe(intermediate_doc)

        # TODO: temporarily dropping ocd_division here to dedupe on multiple values because of multiple '20 Park St' addresses
        # TODO: verify addresses
        intermediate_doc = intermediate_doc.drop_duplicates(subset=['start_time', 'end_time', 'start_date',
                                                                    'end_date', 'ocd_division', 'address_line'])

        #intermediate_doc.rename(columns={'id': 'polling_location_id'}, inplace=True)

        #intermediate_doc = intermediate_doc.drop_duplicates(subset=['address_line', 'hours'])
        #print intermediate_doc

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')
        return intermediate_doc

    def export_for_locality_precinct(self):
        intermediate_doc = self.build_polling_location_txt()

        print intermediate_doc
        # intermediate_doc = self.dedupe(intermediate_doc)

        # intermediate_doc = intermediate_doc.drop_duplicates(subset=['start_time', 'end_time', 'address_line'])

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')
        return intermediate_doc

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()

        # Drop base_df columns.
        plt.drop(['county', 'precinct', 'official_title', 'homepage_url', 'phone', 'email', 'name',
                'street', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'index'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        #print plt

        # TODO: temporarily dropping ocd_division here to dedupe on multiple values because of multiple '20 Park St' addresses
        # TODO: verify addresses
        plt.drop(['ocd_division'], inplace=True, axis=1)
        print plt

        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    #state_file=''
    colnames = ['county', 'precinct', 'official_title', 'ocd_division', 'homepage_url', 'phone', 'email', 'name',
                'street', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date']

    early_voting_df = pd.read_csv(config.input_path + config.state_file, names=colnames, sep=',', encoding='ISO-8859-1', skiprows=1)
    early_voting_df['index'] = early_voting_df.index
    print early_voting_df

    pl = PollingLocationTxt(early_voting_df, config.early_voting)

    pl.export_for_schedule_and_locality()
    pl.write_polling_location_txt()

