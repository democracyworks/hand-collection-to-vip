import pandas as pd
import time
import config
from time import strftime
import hashlib
from datetime import datetime

class PollingLocationTxt(object):
    """
    """

    def __init__(self, base_df, early_voting_true=False, drop_box_true="false"):
        self.base_df = base_df
        self.drop_box_true = drop_box_true
        self.early_voting_true = early_voting_true

    # (row['index'], row['address_1'], row['address_2'],
    #  row['city'], row['state'], row['zip']), axis = 1)

    def get_address_line(self, index, name, address_one, address_two, city, zip_code):
        # required: print message for exception
        # TODO: concatenate street, city, state and zip


        adr = ''

        if not pd.isnull(address_one):
            adr += str(address_one)

        if not pd.isnull(address_two):
            adr += " " + str(address_two)

        if not pd.isnull(city):
            city_name = str(city)
        else:
            city_name = ''

        if not pd.isnull(zip_code):
            zip = str(zip_code)[0:5]
        else:
            zip = ''

        return adr.strip() + ", " + city_name + ", CA " + zip

    def get_directions(self, dirs):
        """#"""
        # no direct relationship to any column
        return dirs

    # def get_start_time(self, time):
    #     arr = time.split(" ")
    #     hours = arr[0].split(":")[0] + ":" + arr[0].split(":")[1]
    #     return hours + " " + "AM"
    #
    # def get_end_time(self, time):
    #     arr = time.split(" ")
    #     hours = arr[0].split(":")[0] + ":" + arr[0].split(":")[1]
    #     return hours + " " + "PM"
    def get_time(self, time):
        time = time.split(" - ")[0]
        d = datetime.strptime(time, "%H:%M:%S")
        return d.strftime("%I:%M %p")

    def get_hours(self, index, start_time, end_time):

        start_time = self.get_time(start_time)
        end_time = self.get_time(end_time)
        return start_time + "-" + end_time



    def convert_hours(self):
        pass

    def get_photo_uri(self):
        # create conditional when/if column is present
        return ''



    def create_hours_open_id(self, index, name, address_one, address_two, city, zip_code):
        """#"""
        # TODO: this is the correct id/index code, correct everywhere
        address_line = self.get_address_line(index, name, address_one, address_two, city, zip_code)
        # print address_line

        address_line = int(hashlib.sha1(address_line).hexdigest(), 16) % (10 ** 8)

        return 'ho' + str(address_line)


    def is_drop_box(self, box):
        """#"""
        # TODO: default to False? Make a boolean instance variable passed as a parameter
        if pd.isnull(box):
            return 'false'
        else:
            return str(box).lower()

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

    def create_id(self, index, ocd_division, name, address_one, address_two, city, zip_code):
        """Create id"""
        # concatenate county name, or part of it (first 3/4 letters) with index
        # add leading zeros to maintain consistent id length
        if not pd.isnull(ocd_division):
            address_line = self.get_address_line(index, name, address_one, address_two, city, zip_code)
            id = int(hashlib.sha1(ocd_division + address_line).hexdigest(), 16) % (10 ** 8)
            id = 'poll' + str(id)
            return id

    def build_polling_location_txt(self):
        """
        New columns that match the 'polling_location.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        self.base_df['address_line'] = self.base_df.apply(
            lambda row: self.get_address_line(row['index'], row['name'], row['address_one'],
                                              row['address_two'], row['city'], row['zip']), axis=1)

        self.base_df['directions'] = self.base_df.apply(
            lambda row: self.get_directions(row['dirs']), axis=1)

        self.base_df['hours'] = self.base_df.apply(
            lambda row: self.get_hours(row['index'],row['start_time'], row['end_time']), axis=1)

        self.base_df['photo_uri'] = self.base_df.apply(
            lambda row: self.get_photo_uri(), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.create_hours_open_id(row['index'], row['name'],row['address_one'], row['address_two'], row['city'],
                                              row['zip']), axis=1)

        self.base_df['is_drop_box'] = self.base_df.apply(
            lambda row: self.is_drop_box(row["drop_box"]), axis=1)

        self.base_df['is_early_voting'] = self.base_df.apply(
            lambda row: self.is_early_voting(), axis=1)

        self.base_df['latitude'] = self.base_df.apply(
            lambda row: self.get_latitude(), axis=1)

        self.base_df['longitude'] = self.base_df.apply(
            lambda row: self.get_longitude(), axis=1)

        self.base_df['latlng_source'] = self.base_df.apply(
            lambda row: self.get_latlng_source(), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['index'], row['ocd-division'], row['name'],row['address_one'],
                                       row['address_two'], row['city'], row['zip']), axis=1)

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
        plt.drop(['ocd-division', 'county', 'name', 'address_one', 'address_two', 'city', 'state', 'zip', 'start_time',
                'end_time', 'start_date', 'end_date', 'appt_1', 'appt_2', 'appt_3', 'subject_to_change', 'index', 'dirs'], inplace=True, axis=1)

        plt = self.dedupe(plt)
        print plt

        plt.to_csv(config.output + 'polling_location.txt', index=False, encoding='utf-8')  # send to txt file
        plt.to_csv(config.output + 'polling_location.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':


    early_voting_true = "true"  # True or False
    #drop_box_true =
    state_file='california_early_voting_info.csv'

    #early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/" + state_file
    early_voting_file = config.data_folder + state_file

    colnames = ['ocd-division', 'county', 'name', 'address_one', 'address_two', 'dirs', 'city', 'state', 'zip', 'start_time',
                'end_time', 'start_date', 'end_date', 'appt_1', 'appt_2', 'appt_3', 'subject_to_change', 'drop_box']


    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='ISO-8859-1', skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1

    pl = PollingLocationTxt(early_voting_df, early_voting_true)
    pl.export_for_schedule_and_locality()
    pl.write_polling_location_txt()