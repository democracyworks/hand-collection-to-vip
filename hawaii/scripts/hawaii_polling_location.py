import pandas as pd
import time
import config
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

    def get_address_line(self, index, address):
        # required: print message for exception
        # TODO: concatenate street, city, state and zip
        if address:
            address_line = address
        else:
            address_line = ''

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
                # print 'Hours were not in the proper format in line ' + str(index) + '.'
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


    def convert_hours(self):
        pass

    def get_photo_uri(self):
        # create conditional when/if column is present
        return ''

    def create_hours_open_id(self, index):
        """#"""
        if index <= 9:
            return 'ho000' + str(index)

        elif index in range(10,100):
            return 'ho00' + str(index)

        elif index > 100:
            return 'ho0' + str(index)
        else:
            return str(index)


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
            lambda row: self.get_address_line(row['index'], row['address']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index']), axis=1)

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

    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset='address_line')

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()

        # Drop base_df columns.
        plt.drop(['county', 'address', 'directions', 'start_time', 'end_time', 'start_date', 'end_date'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt

        plt.to_csv(config.polling_location_output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.polling_location_output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':


    early_voting_true = True  # True or False
    #drop_box_true =
    state_file='hawaii_early_voting_info.csv'

    #early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/" + state_file
    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/hawaii/scripts/early_voting_input/" + state_file

    colnames = ['county', 'address', 'directions', 'start_time', 'end_time', 'start_date', 'end_date']

    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1

    pl = PollingLocationTxt(early_voting_df, early_voting_true)
    pl.write_polling_location_txt()