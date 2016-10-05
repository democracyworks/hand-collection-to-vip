import pandas as pd
import time
import config
from time import strftime
import hashlib

class PollingLocationTxt(object):
    """

    """

    def __init__(self, base_df, early_voting_true=False, drop_box_true="false"):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true

    # (row['index'], row['address_1'], row['address_2'],
    #  row['city'], row['state'], row['zip']), axis = 1)

    def get_address_line(self, index, street, city, zip_code):
        # required: print message for exception
        # TODO: concatenate street, city, state and zip
        if not pd.isnull(street):
            st = street
        else:
            st = ''

        if not pd.isnull(city):
            city = city
        else:
            city = ''

        if not pd.isnull(zip_code):
            # zip = str(zip_code)[0:5]
            zip = zip_code

        else:
            zip = ''

        return st.strip() + ", " + city + ", AR " + str(zip)

    def get_directions(self, dirs):
        """#"""
        # no direct relationship to any column
        return dirs


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
            d = time.strptime(time_str, "%H:%M:%S")
            formatted_time = time.strftime("%I:%M %p", d)
            return formatted_time
        else:
            return ''



    def get_converted_time(self, index, time_str):
        t = str(time_str)
        t = t.split("-")[0].strip()
        return self.convert_to_twelve_hour(index, t)

    def get_hours(self, index, start_time, end_time):
        start = self.get_converted_time(index, start_time)
        end = self.get_converted_time(index, end_time)
        return start + " - " + end



    def convert_hours(self):
        pass

    def get_photo_uri(self):
        # create conditional when/if column is present
        return ''



    def create_hours_open_id(self, index, street, city, zip_code):
        """#"""
        # TODO: this is the correct id/index code, correct everywhere
        address_line = self.get_address_line(index, street, city, zip_code)
        # print address_line

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

    def create_id(self, index, ocd_division, street, city, zip_code):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length
        if not pd.isnull(ocd_division):
            address_line = self.get_address_line(index, street, city, zip_code)
            id = int(hashlib.sha1(ocd_division + address_line).hexdigest(), 16) % (10 ** 8)
            id = 'poll' + str(id)
            return id

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'],  row['address_one'],
                                               row['city'], row['zip']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(row["address_two"]), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)
        #
        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)
        #
        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index'], row['address_one'], row['city'],
                                              row['zip']), axis=1)

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
        #
        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['index'], row['ocd_division'], row['address_one'], row['city'], row['zip']), axis=1)

        return self.base_df

    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset=['address_line'])

    def dedupe_for_sch(self, dupe):
        return dupe.drop_duplicates(subset=['address_line', 'hours', 'start_date'])

    def group_by_address(self, item):
        item = item.groupby('address_line').agg(lambda x: ' '.join(set(x))).reset_index()

    # def export_for_locality(self):
    #     ex_doc = self.build_polling_location_txt()
    #     #print ex_doc
    #
    #     ex_doc = self.dedupe(ex_doc)
    #     # print ex_doc
    #
    #     ex_doc.to_csv(config.output + 'intermediate_pl_for_loc.csv', index=False, encoding='utf-8')
    #
    # def export_for_schedule(self):
    #     ex_doc = self.build_polling_location_txt()
    #
    #     ex_doc = self.dedupe_for_sch(ex_doc)
    #
    #     ex_doc.to_csv(config.output + 'intermediate_pl_for_sch.csv', index=False, encoding='utf-8')

    def export_for_schedule_and_locality(self):
        intermediate_doc = self.build_polling_location_txt()

        # print intermediate_doc
        # intermediate_doc = self.dedupe(intermediate_doc)

        intermediate_doc = intermediate_doc.drop_duplicates(subset=['start_time', 'end_time', 'start_date',
                                                                    'end_date', 'address_line'])

        intermediate_doc.to_csv(config.output + 'intermediate_doc.csv', index=False, encoding='utf-8')

    def write_polling_location_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        plt = self.build_polling_location_txt()

        # Drop base_df columns.
        plt.drop(['office-name', 'title', 'ocd_division', 'description', 'homepage', 'phone', 'email', 'address_one',
                  'address_two', 'loc_name', 'city', 'state', 'zip',
                'start_time', 'end_time', 'start_date', 'end_date', 'notes', 'index'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt

        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':


    early_voting_true = "true"  # True or False
    #drop_box_true =
    state_file='arkansas_early_voting_info.csv'

    #early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/" + state_file
    early_voting_file = config.data_folder + state_file

    colnames = ['office-name', 'title', 'ocd_division', 'description', 'homepage', 'phone', 'email', 'loc_name', 'address_one', 'address_two', 'city', 'state', 'zip',
                'start_time', 'end_time', 'start_date', 'end_date', 'notes']


    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='ISO-8859-1', skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1

    pl = PollingLocationTxt(early_voting_df, early_voting_true)
    pl.export_for_schedule_and_locality()
    pl.write_polling_location_txt()