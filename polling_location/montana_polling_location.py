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

class PollingLocationTxt(object):
    """

    """

    def __init__(self, base_df, early_voting_true=False, drop_box_true=False):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true

    def get_address_line(self, index, polling_place_address, city, zip_code):
        # required: print message for exception
        # TODO: concatenate street, city, state and zip

        if polling_place_address:

            address = ' '.join(word[0].upper() + word[1:] for word in str(polling_place_address).lower().split())

        else:
            address = ''
            print 'Missing address value at row ' + index + '.'

        if city:
            city = ' '.join(word[0].upper() + word[1:] for word in city.lower().split())
        else:
            city = ''
            print 'Missing city value at row ' + index + '.'

        if zip_code:
            # TODO: add zip code validation
            zip_code = str(zip_code)
            #print zip_code
        else:
            zip_code = ''
            print 'Missing zip_code value at row ' + index + '.'

        address_line = address + ', ' + city + ', MT ' + zip_code

        return address_line

    def get_directions(self):
        """#"""
        # no direct relationship to any column
        return ''

    def get_hours(self, index, hours):
        # create conditional when/if column is present

        known_str_starts = ('7:00', 'Noon', '* Noon')

        if hours.startswith(known_str_starts) == True:
            polling_place_hours = tuple(str(hours.lower()).replace(" ", "").split('-'))
            #print polling_place_hours

            # start time
            if polling_place_hours[0] in ['noon', '*noon']:
                start_time_str = '12pm'
                    #return time_str

            else:
                start_time_str = polling_place_hours[0][:1] + 'am'
                #print start_time_str

            # end time
            if polling_place_hours[1]:
                end_time_str = polling_place_hours[1][:1] + 'pm'
                print end_time_str

            return start_time_str + '-' + end_time_str

        else:
            print "The value ('" + hours +   "') at row " + str(index) + ' is invalid.'
            #raise ValueError("The value ('" + hours +   "') at row " + str(index) + ' is invalid.')
            #return ''

    def convert_hours(self):
        pass

    def get_photo_uri(self):
        # create conditional when/if column is present
        return ''

    def create_hours_open_id(self):
        """#"""
        pass

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

    def create_id(self, index, county):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length

        if county:
            county_str_part = county.replace(' ', '').lower()[:4]

        else:
            county_str_part = ''
            print 'Missing county value at ' + index + '.'

        if index <= 9:
            index_str = '000' + str(index)

        elif index in range(10,100):
            index_str = '00' + str(index)

        elif index > 100:
            index_str = '0' + str(index)
        else:
            index_str = str(index)

        return county_str_part + str(index_str)

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'], row['polling_place_address'], row['polling_place_city'],
                                              row['polling_place_zip']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['polling_place_hours']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(), axis=1)

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
            lambda row: self.create_id(row['index'], row['county']), axis=1)

        return self.base_df

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()

        # Drop base_df columns.
        plt.drop(['index', 'county', 'precinct_name', 'precinct_number', 'total_number_registered_voters', 'hd', 'sd',
                'polling_place_location', 'polling_place_address', 'polling_place_city', 'polling_place_zip',
                'polling_place_hours', 'late_egistration_location', 'late_egistration_address'], inplace=True, axis=1)

        print plt

        plt.to_csv('polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv('polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':


    early_voting_true = True  # True or False
    #drop_box_true =
    state_file='montana_early_voting _info.csv'

    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/polling_location/polling_location_input/" + state_file

    colnames = ['county', 'precinct_name', 'precinct_number', 'total_number_registered_voters', 'hd', 'sd',
                'polling_place_location', 'polling_place_address', 'polling_place_city', 'polling_place_zip',
                'polling_place_hours', 'late_egistration_location', 'late_egistration_address']
    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)

    early_voting_df['index'] = early_voting_df.index
    #print early_voting_df

    pl = PollingLocationTxt(early_voting_df, early_voting_true)

    pl.write_polling_location_txt()

