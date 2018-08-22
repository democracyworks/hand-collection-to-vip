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
import hashlib
import re
import config
from maine_polling_location import PollingLocationTxt



class LocalityTxt(object):
    """#
    """

    def __init__(self, early_voting_df, state):
        self.base_df = early_voting_df
        self.state = state
        #print self.base_df

    def create_election_administration_id(self, index):
        """Creates election_administration_ids by concatenating a prefix with an 'index_str' based on the Dataframe's
        row index. '0s' are added, if necesary, to maintain a consistent id length. As currently designed the method
        works up to index 9,999"""
        return ''

    def get_external_identifier_type(self):
        """#"""
        return "ocd-id"

    def get_external_identifier_othertype(self):
        # create conditional when/if column is present
        return ''

    def get_external_identifier_value(self, external_identifier_value):
        """Extracts external identifier (ocd-division)."""
        #ocd-division/country:us/state:me/place:woodland

        town = external_identifier_value.split('(')[0]
        town = town.split('/')[0]
        town = town.split('&')[0]
        town = town.lower().replace(" ", "_").strip('_')

        print town
        if external_identifier_value:
            string = "ocd-division/country:us/state:me/place:" + town
            return string
        else:
            return ''

    def create_name(self, index, division_description):
        """
        Creates a name by concatenating the 'locality' (town name along with town or county designation)
        with an 'index_str' based on the Dataframes row index.'0s' are added, if necesary, to
        maintain a consistent id length.
        """
        if division_description:
            locality = str(division_description[:-3].lower().replace(" ", "_"))
            #print locality
        else:
            locality = ''
            print 'Missing data at row ' + str(index) + '.'

        # Add leading '0s' depending on index number.
        if index <= 9:
            index_str = '000' + str(index)

        elif index in range(10,100):
            index_str = '00' + str(index)

        elif index in range(100, 1000):
            index_str = '0' + str(index)
        else:
            index_str = str(index)

        return locality + index_str

    def get_polling_location_ids(self, polling_location_id):
        """Returns empty value so that polling locations are not linked to locality."""
        return ''

    def polling_location_ids_for_precincts(self, polling_location_id):
        """Returns the polling_location_id specifically to pass the data forward to the precinct script."""

        return polling_location_id
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
        """"Set type value"""
        return 'other'

    def get_other_type(self):
        # create conditional when/if column is present
        return ''


    def create_id(self, external_idocdentifier_value):
        """Creates a sequential id by concatenating 'loc' with an 'index_str' based on the Dataframe's row index.
        '0s' are added, if necesary, to maintain a consistent id length.
        """

        l_id = int(hashlib.sha1(str(external_idocdentifier_value).strip()).hexdigest(), 16) % (10 ** 8)

        return 'loc' + str(l_id)

    def build_locality_txt(self):
        """
        New columns that match the 'locality.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        self.base_df['election_administration_id'] = self.base_df.apply(
            lambda row: self.create_election_administration_id(row['index']), axis=1)

        self.base_df['external_identifier_type'] = self.base_df.apply(
            lambda row: self.get_external_identifier_type(), axis=1)

        self.base_df['external_identifier_othertype'] = self.base_df.apply(
            lambda row: self.get_external_identifier_othertype(), axis=1)

        self.base_df['external_identifier_value'] = self.base_df.apply(
            lambda row: self.get_external_identifier_value(row['municipality']), axis=1)

        self.base_df['name'] = self.base_df.apply(
            lambda row: self.create_name(row['index'], row['municipality']), axis=1)

        self.base_df['polling_location_ids'] = self.base_df.apply(
            lambda row: self.get_polling_location_ids(row['id']), axis=1)

        self.base_df['polling_location_ids_for_precincts'] = self.base_df.apply(
            lambda row: self.polling_location_ids_for_precincts(row['id']), axis=1)

        self.base_df['state_id'] = self.base_df.apply(
            lambda row: self.create_state_id(), axis=1)

        self.base_df['type'] = self.base_df.apply(
            lambda row: self.get_type(), axis=1)

        self.base_df['other_type'] = self.base_df.apply(
            lambda row: self.get_other_type(), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['ocd']), axis=1)

        return self.base_df

    def final_build(self):
        """#"""

        loc = self.build_locality_txt()

        print "hi"
        print loc

        loc['id'] = loc.apply(
            lambda row: self.create_id(row['external_identifier_value']), axis=1)

        #print loc

        # reorder columns to VIP format
        cols =['election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
               'external_identifier_value', 'name', 'polling_location_ids', 'state_id', 'type',
                'other_type', 'grouped_index', 'id']

        final = loc.reindex(columns=cols)
        print final

        final.drop(['grouped_index',], inplace=True, axis=1)
        #print final
        return final

    def export_for_precinct(self):
        loc = self.build_locality_txt()
        #loc = loc.drop_duplicates(subset=['id'])

        # reorder columns to VIP format
        cols = ['election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
                'external_identifier_value', 'name', 'polling_location_ids_for_precincts', 'state_id', 'type',
                'other_type', 'id', 'municipality', 'wp']

        final = loc.reindex(columns=cols)

        final.to_csv(config.output + 'locality_for_precinct.txt', index=False, encoding='utf-8')
        #print final
        return final

    def write_locality_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        loc = self.final_build()
        loc = loc.drop_duplicates(subset=['id'])
        #print loc

        loc.to_csv(config.output + 'locality.txt', index=False, encoding='utf-8')  # send to txt file
        loc.to_csv(config.output + 'locality.csv', index=False, encoding='utf-8')  # send to csv file

if __name__ == '__main__':

    columns = ['cty', 'ocd' ,'municipality', 'wp', 'voting_place', 'street', 'start_time', 'end_time', 'start_date', 'end_date']

    polling_place_df = pd.read_csv(config.input_path + config.state_file, names=columns, sep=',', encoding='ISO-8859-1', skiprows=1)
    polling_place_df['index'] = polling_place_df.index

    pl = PollingLocationTxt(polling_place_df, config.state_abbreviation_upper)
    polling_place_df = pl.export_for_locality_precinct()
    #polling_place_df['index'] = polling_place_df.index
    print polling_place_df


    lt = LocalityTxt(polling_place_df, config.state)
    lt.write_locality_txt()
    #lt.export_for_precinct()