import pandas as pd
import time
from time import strftime

class PollingLocationTxt(object):
    """

    """

    def __init__(self, base_df, early_voting_true=False, drop_box_true=False):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true

    # (row['index'], row['address_1'], row['address_2'],
    #  row['city'], row['state'], row['zip']), axis = 1)

    def get_address_line(self, index, address_one, address_two, city, zip_code):
        # required: print message for exception
        # TODO: concatenate street, city, state and zip
        if address_one:
            adr1 = str(address_one)
        else:
            adr1 = ''

        if not pd.isnull(address_two):
            adr2 = str(address_two)
        else:
            adr2 = ''

        if city:
            city_name = city
        else:
            city_name =''

        if zip_code:
            zip = str(zip_code[0:5])
        else:
            zip = ''

        final_line = adr1 + " " + adr2 + " " + city_name + ", KS " + zip
        return final_line

    def get_directions(self):
        """#"""
        # no direct relationship to any column
        return ''

    def convert_to_time(self, index, time):
        #TODO: convert time strings into a time format--ie 1000 to 10:00
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
                # print 'Hours were not in the proper format in line ' + str(index) + '.'
                return ''
       else:
            return ""

    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset='address_line')


    def add_am_and_pm(self, index, time):
        arr = time.split("-")
        str1 = arr[0] + "am"
        str2 = arr[1] + "pm"
        return str1 + "-" + str2


    def get_hours(self, index, hours):
        # create conditional when/if column is present
        hours_arr = hours.split(" ")
        if "(Mountain)" in hours_arr:
            hours_arr.remove("(Mountain)")
        final_str = ''
        for time in hours_arr:
            final_str += self.add_am_and_pm(index, time) + " "
        return final_str




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
            lambda row: self.get_address_line(row['index'], row['address_one'], row['address_two'],
                                              row['city'], row['zip']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['times']), axis=1)

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
        plt.drop(['index', 'county', 'officer', 'email', 'blank', 'phone', 'fax', 'address_one',
                'address_two', 'city', 'state', 'zip', 'times','start_date', 'end_date']
                 , inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt

        txt_file = "/Users/danielgilberg/Development/hand-collection-to-vip/kansas/output/polling_location.txt"
        csv_file = "/Users/danielgilberg/Development/hand-collection-to-vip/kansas/output/polling_location.csv"

        plt.to_csv(txt_file, index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(csv_file, index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':


    early_voting_true = True  # True or False
    #drop_box_true =
    state_file='kansas_early_voting_info.csv'

    early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/" + state_file

    colnames = ['county', 'officer', 'email', 'blank', 'phone', 'fax', 'address_one',
                'address_two', 'city', 'state', 'zip', 'times','start_date', 'end_date']
    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)
    early_voting_df['index'] = early_voting_df.index
    pl = PollingLocationTxt(early_voting_df, early_voting_true)

    # print early_voting_df["address_1"] + early_voting_df["address_2"]
    pl.write_polling_location_txt()
    # print early_voting_df["index"]
