"""
Contains class that generates the 'locality.txt' file for any state.

locality.txt contains the following columns:
election_administration_id,
external_identifier_type,
external_identifier_othertype,
external_identifier_value,
name,
polling_location_ids,
state_id,
type,
other_type,
id


"""

import pandas as pd
import config
import re

class LocalityTxt(object):
    """#
    """

    def __init__(self, early_voting_df, state):
        self.base_df = early_voting_df
        self.state = state

    def create_election_administration_id(self, index):
        """Creates election_administration_ids by concatenating a prefix with an 'index_str' based on the Dataframe's
        row index. '0s' are added, if necesary, to maintain a consistent id length. As currently designed the method
        works up to index 9,999"""

        ## TODO: use fips code
        #for key, value in config.fips_dict.iteritems():
        #    if key == config.state.lower():
        #        state_num = value
        #       return 'ea' + str(state_num)
        return ''

    def get_external_identifier_type(self):
        """#"""
        return "ocd-id"

    def get_external_identifier_othertype(self):
        # create conditional when/if column is present
        return ''

    def get_external_identifier_value(self, external_identifier_value):
        """Extracts external identifier (ocd-division)."""

        if external_identifier_value:
            return external_identifier_value
        else:
            return ''

    def create_name(self, index, county, city ):
        """
        Creates a name by concatenating the 'locality' (town name along with town or county designation)
        with an 'index_str' based on the Dataframes row index.'0s' are added, if necesary, to
        maintain a consistent id length.
        """



        # Get locality(town or county), and remove state abbreviation.
        if county and city:
            locality =  county + '_' + city
            return locality + '_' + str(index)
            #print locality
        elif county:
            locality = county
            return locality + '_' + str(index)

        elif city:
            locality = city
            return locality + '_' + str(index)
        else:
            print 'Missing data at row ' + str(index) + '.'



    def create_polling_location_ids(self, polling_location_id):
        """
        Creates polling_location_ids by concatenating 'poll' with an 'index_str' based on the Dataframe's row index.
        '0s' are added, if necesary, to maintain a consistent id length.
        """

        return polling_location_id

 #       if index <= 9:
 #           index_str = '000' + str(index)
 #           return 'poll' + index_str

#        elif index in range(10, 100):
#            index_str = '00' + str(index)
#            return 'poll' + index_str

#        elif index in range(100, 1000):
#            index_str = '0' + str(index)
#            return 'poll' + index_str

#        elif index:
#            index_str = str(index)
#            return 'poll' + index_str

#        else:
#            return ''

    def create_state_id(self):
        """Creates the state_id by matching a key in the state_dict and retrieving
        and modifying its value. A '0' is added, if necessary, to maintain a
        consistent id length.
        """
        for key, value in config.fips_dict.iteritems():
            if key == self.state:
                state_num = value
                if state_num <=9:
                    state_num = '0' + str(state_num)
                else:
                    state_num = str(state_num)

                return 'st' + state_num

    def get_type(self):
        # create conditional when/if column is present
        return 'other'

    def get_other_type(self):
        # create conditional when/if column is present
        return ''


    def create_id(self, index):
        """Creates a sequential id by concatenating 'loc' with an 'index_str' based on the Dataframe's row index.
        '0s' are added, if necesary, to maintain a consistent id length.
        """

        if index <=9:
            index_str = '000' + str(index)
            return 'loc' + index_str

        elif index in range(10,100):
            index_str = '00' + str(index)
            return 'loc' + index_str

        elif index in range(100, 1000):
            index_str = '0' + str(index)
            return 'loc' + index_str

        elif index:
            index_str = str(index)
            return 'loc' + index_str

        else:
            return ''

    def build_locality_txt(self):
        """
        New columns that match the 'locality.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        #self.base_df['election_administration_id'] = self.base_df.apply(
        #    lambda row: self.create_election_administration_id(row['index']), axis=1)

        self.base_df['external_identifier_type'] = self.base_df.apply(
            lambda row: self.get_external_identifier_type(), axis=1)

        self.base_df['external_identifier_othertype'] = self.base_df.apply(
            lambda row: self.get_external_identifier_othertype(), axis=1)

        self.base_df['external_identifier_value'] = self.base_df.apply(
            lambda row: self.get_external_identifier_value(row['ocd_division']), axis=1)

        self.base_df['name'] = self.base_df.apply(
            lambda row: self.create_name(row['index'], row['county'], row['city']), axis=1)

        self.base_df['polling_location_ids'] = self.base_df.apply(
            lambda row: self.create_polling_location_ids(row['polling_location_id']), axis=1)

        self.base_df['state_id'] = self.base_df.apply(
            lambda row: self.create_state_id(), axis=1)

        self.base_df['type'] = self.base_df.apply(
            lambda row: self.get_type(), axis=1)

        self.base_df['other_type'] = self.base_df.apply(
            lambda row: self.get_other_type(), axis=1)

        #self.base_df['id'] = self.base_df.apply(
        #    lambda row: self.create_id(row['index']), axis=1)

        return self.base_df

    def final_build(self):

        loc = self.build_locality_txt()

        # Drop base_df columns.
 #       loc.drop(['ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state',
 #               'zip', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
 #               'is_or_by_appointment', 'appointment_phone_num', 'is_subject_to_change', 'index',
 #               'address_line', 'directions', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box',
 #               'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'polling_location_id'], inplace=True, axis=1)

        loc = loc.groupby('external_identifier_value').agg(lambda x: ' '.join(set(x))).reset_index()

        #loc['election_administration_id'] = loc['election_administration_id'].apply(lambda x: ''.join(x.split(' ')[0]))
        #loc['id'] = loc['id'].apply(lambda x: ''.join(x.split(' ')[0]))

        loc['name'] = loc['name'].apply(lambda x: ''.join(x.split(' ')[0]))

        loc['grouped_index'] = loc.index + 1
        #print loc

        #loc['election_administration_id'] = loc.apply(
        #    lambda row: self.create_election_administration_id(row['grouped_index']), axis=1)
            #lambda row: self.create_election_administration_id(''), axis = 1)

        loc['id'] = loc.apply(
            lambda row: self.create_id(row['grouped_index']), axis=1)

        # reorder columns
        cols =['election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
               'external_identifier_value', 'name', 'polling_location_ids', 'state_id', 'type',
                'other_type', 'grouped_index', 'id']

        final = loc.reindex(columns=cols)

        final.drop(['grouped_index',], inplace=True, axis=1)

        print final

        return final


    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset='external_identifier_value', inplace=True)

    def write_locality_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        loc = self.final_build()

        loc.to_csv(config.locality_output + 'locality.txt', index=False, encoding='utf-8')  # send to txt file
        loc.to_csv(config.locality_output + 'locality.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    early_voting_file = 'intermediate_doc.csv'

    early_voting_path = "/home/acg/democracyworks/hand-collection-to-vip/minnesota/output/" + early_voting_file
    colnames = ['ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state',
                'zip', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
                'is_or_by_appointment', 'appointment_phone_num', 'is_subject_to_change', 'index',
                'address_line', 'directions', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box',
                'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'polling_location_id']

    early_voting_df = pd.read_csv(early_voting_path, names=colnames, encoding='utf-8', skiprows=1)

    early_voting_df['index'] = early_voting_df.index + 1 # offsets zero based index so it starts at 1 for ids
    #print early_voting_df

    lt = LocalityTxt(early_voting_df, config.state)
    lt.write_locality_txt()