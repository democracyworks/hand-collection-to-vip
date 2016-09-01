import pandas as pd
import time
import config
import hashlib
import datetime

class PollingLocationTxt(object):
    """

    """

    def __init__(self, base_df, early_voting_true='false', drop_box_true='false'):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true

    def get_address_line(self, index, location_name, address_1, address_2, city, zip_code):
        # required: print message for exception
        # TODO: concatenate street, city, state and zip
        #if address_1:
        #    adr1 = str(address_1)
        #else:
        #    adr1 = ''

        # 'location', 'address_1', 'address_2', 'city', 'state', 'zip'

        if not pd.isnull(location_name):
            street_address =  str(location_name) + ', ' +str(address_1)
            print street_address
        else:
            street_address = address_1

        if not pd.isnull(address_2):
            street_address =  str(street_address) + ' ' + str(address_2)
        else:
            street_address = street_address

        if city:
            city_name = city
        else:
            city_name =''

        if zip_code:
            zip = str(zip_code)
        else:
            zip = ''

        #address = adr1 + '' +adr2

        final_line = street_address + ", " + city_name + ", SD " + zip
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

    def create_hours_open_id(self, index, location_name, address_1, address_2, city, zip_code):
        """#"""

        address_line = self.get_address_line(index, location_name, address_1, address_2, city, zip_code)
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

    def create_id(self, index, location_name, address_1, address_2, city, zip_code):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length

        address_line = self.get_address_line(index, location_name, address_1, address_2, city, zip_code)

        id =  int(hashlib.sha1(config.state + address_line).hexdigest(), 16) % (10 ** 8)
        id = 'poll' + str(id)
        return id

        #if county:
        #    county_str_part = county.replace(' ', '').lower()[:4]

        #else:
        #    county_str_part = ''
        #    print 'Missing county value at ' + index + '.'

        #if index <= 9:
        #    index_str = '000' + str(index)

        #elif index in range(10,100):
        #    index_str = '00' + str(index)


        #elif index in range(100, 1000):
        #    index_str = '0' + str(index)
        #else:
        #    index_str = str(index)

        #eturn 'poll' + str(index_str)

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'], row['location_name'], row['address_1'],
                                              row['address_2'], row['city'], row['zip_code']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index'], row['location_name'],row['address_1'],
                                              row['address_2'], row['city'], row['zip_code']), axis=1)

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
            lambda row: self.create_id(row['index'], row['ocd_division'], row['address_1'],
                                              row['address_2'], row['city'], row['zip_code']), axis=1)

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

        #intermediate_doc = intermediate_doc.drop_duplicates(subset=['address_line', 'hours'])
        print intermediate_doc

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')
        return intermediate_doc

#    def format(self):
#       plt = self.build_polling_location_txt()

        # Drop base_df columns.
#        plt.drop(['index', 'ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state', 'zip',
#                 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment', 'is_or_by_appointment',
#                 'appointment_phone_num', 'is_subject_to_change'], inplace=True, axis=1)

#        plt = self.dedupe(plt)
#        return plt


    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()


        # Drop base_df columns.
        plt.drop(['index', 'ocd_division', 'homepage_url', 'county', 'location_name', 'address_1', 'address_2', 'city',
                'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
                'is_or_by_appointment', 'appointment_phone', 'is_subject_to_change'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt



        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file



if __name__ == '__main__':


    state_file='ohio_early_voting_info.csv'

    #early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/" + state_file
    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/ohio/early_voting_input/" + state_file

    colnames = ['ocd_division', 'homepage_url', 'county', 'location_name', 'address_1', 'address_2', 'city',
                'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
                'is_or_by_appointment', 'appointment_phone', 'is_subject_to_change']



    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1
    print early_voting_df

    pl = PollingLocationTxt(early_voting_df, config.early_voting)

    pl.write_polling_location_txt()
    #pl.export_for_schedule_and_locality()

