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
from pima_county_polling_location import PollingLocationTxt



class LocalityTxt(object):
    """Creates VIP 5.1 locality.txt document.
    """

    def __init__(self, early_voting_df, state):
        self.base_df = early_voting_df
        self.state = state

    def create_election_administration_id(self, index):
        """Not a required field. No data available."""
        return ''

    def get_external_identifier_type(self):
        """Sets external_identifier_type value."""
        return "ocd-id"

    def get_external_identifier_othertype(self):
        """N/A"""
        return ''

    def get_external_identifier_value(self, external_identifier_value):
        """Extracts external identifier (ocd-division)."""

        if external_identifier_value:
            return external_identifier_value
        else:
            return ''

    def create_name(self, index, division_description):
        """
        Creates a name by concatenating the 'locality' (town name along with town or county designation)
        with an 'index_str' based on the dataframe's row index. Leading are added, if necesary, to
        maintain a consistent id length.
        """
        if division_description:
            locality = str(division_description[:-3].lower().replace(" ", "_"))
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
        """Sets polling_location_id value."""

        return polling_location_id

    def create_state_id(self):
        """Prepends 'st' to Pima County's 5 digit fips code."""

        return 'st04019'

    def get_type(self):
        """"Sets type value"""
        return 'other'

    def get_other_type(self):
        """N/A"""
        return ''


    def create_id(self, index):
        """Creates a sequential id by concatenating 'loc' with an 'index_str' based on the dataframe's row index.
        Leading zeroes are added to maintain a consistent id length.
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
        self.base_df['election_administration_id'] = self.base_df.apply(
            lambda row: self.create_election_administration_id(row['index']), axis=1)

        self.base_df['external_identifier_type'] = self.base_df.apply(
            lambda row: self.get_external_identifier_type(), axis=1)

        self.base_df['external_identifier_othertype'] = self.base_df.apply(
            lambda row: self.get_external_identifier_othertype(), axis=1)

        self.base_df['external_identifier_value'] = self.base_df.apply(
            lambda row: self.get_external_identifier_value(row['ocd_division']), axis=1)

#        self.base_df['name'] = self.base_df.apply(
#            lambda row: self.create_name(row['index'], row['division_description']), axis=1)

        self.base_df['polling_location_ids'] = self.base_df.apply(
            lambda row: self.get_polling_location_ids(row['id']), axis=1)

        self.base_df['state_id'] = self.base_df.apply(
            lambda row: self.create_state_id(), axis=1)

        self.base_df['type'] = self.base_df.apply(
            lambda row: self.get_type(), axis=1)

        self.base_df['other_type'] = self.base_df.apply(
            lambda row: self.get_other_type(), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['index']), axis=1)

        return self.base_df


    def groupby_external_identifier(self):
        """Groups rows by the external_identifier_value (county)."""

        loc = self.build_locality_txt()
        print loc.columns
        loc = loc.groupby('external_identifier_value').agg(lambda x: ' '.join(set(x))).reset_index()

        loc['grouped_index'] = loc.index + 1

        # Groupby was dropping the 'name' value. Adding it here instead.
        loc['name'] = loc.apply(
            lambda row: self.create_name(row['grouped_index'], row['county']), axis=1)

        loc['id'] = loc.apply(
            lambda row: self.create_id(row['grouped_index']), axis=1)

        # reorder columns to VIP format
        cols =['election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
               'external_identifier_value', 'name', 'polling_location_ids', 'state_id', 'type',
                'other_type', 'grouped_index', 'id']

        final = loc.reindex(columns=cols)

        final.drop(['grouped_index',], inplace=True, axis=1)
        print final
        return final

    def export_for_precinct(self):
        """"""
        loc = self.build_locality_txt()

        # reorder columns to VIP format
        cols = ['external_identifier_value', 'name', 'polling_location_ids', 'state_id',
                'other_type', 'id', 'address_line', 'zip_code', 'county', 'precinct']

        return loc.reindex(columns=cols)

    def write_locality_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        loc = self.groupby_external_identifier()

        loc.to_csv(config.output + 'locality.txt', index=False, encoding='utf-8')  # send to txt file
        loc.to_csv(config.output + 'locality.csv', index=False, encoding='utf-8')  # send to csv file

if __name__ == '__main__':


    state_file = config.state_file

    colnames = ['county', 'official_title', 'types', 'ocd_division', 'division_description', 'homepage_url', 'phone',
                'email', 'directions', 'precinct', 'address', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'notes']

    early_voting_df = pd.read_csv(config.input_path + state_file, names=colnames, sep=',', encoding='utf-8', skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1

    pl = PollingLocationTxt(early_voting_df, config.state_abbreviation_upper)
    early_voting_df = pl.export_for_schedule_and_locality()
    print early_voting_df


    lt = LocalityTxt(early_voting_df, config.state)
    lt.write_locality_txt()
    #lt.export_for_precinct()