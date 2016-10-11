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
import re
import config
from ga_polling_location import PollingLocationTxt



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

    def get_external_identifier_value(self, ocd_division):
        """Extracts external identifier (ocd-division)."""

        if not pd.isnull(ocd_division):
            return ocd_division
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
        """
        Creates polling_location_ids by concatenating 'poll' with an 'index_str' based on the Dataframe's row index.
        '0s' are added, if necesary, to maintain a consistent id length.
        """

        return polling_location_id

#        if index <= 9:
#            index_str = '000' + str(index)
#            return 'poll' + index_str

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
        """"Set type value"""
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

    def create_merge_key(self, county):
        """For use when a version of the output is exported as a
        dataframe to the precinct script."""
        return county.upper().replace(' COUNTY', '')

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

        self.base_df['merge_key'] = self.base_df.apply(
            lambda row: self.create_merge_key(row['county']), axis=1)

        return self.base_df

#    def group_polling_location_ids(self, frame):
        #frame = self.build_locality_txt()
#        return pd.concat(g for _, g in frame.groupby("external_identifier_value") if len(g) > 1)
        #return frame.groupby('external_identifier_value')

    def final_build(self):
        """#"""

        loc = self.build_locality_txt()
        print loc

        # Drop base_df columns.
 #       loc.drop(['ocd_division', 'email', 'location_name', 'address_1', 'address_2', 'city',
 #               'state', 'zip_code', 'source_start_time', 'source_end_time', 'source_start_date', 'source_end_date',
 #               'is_only_by_appointment', 'is_or_by_appointment', 'appointment_phone', 'is_subject_to_change',
 #               'index'], inplace=True, axis=1)
        #['address_line', 'directions', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box',
        #'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'polling_location_id']

        #loc = loc.groupby('hours_open_id', 'external_identifier_value').agg(lambda x: ' '.join(set(x))).reset_index()
        loc = loc.groupby('external_identifier_value').agg(lambda x: ' '.join(set(x))).reset_index()
        #print loc

        #loc['name'] = loc['name'].apply(lambda x: ''.join(x.split(' ')[0]))

        loc['grouped_index'] = loc.index + 1

        loc['name'] = loc.apply(
            lambda row: self.create_name(row['grouped_index'], row['county']), axis=1)

        loc['id'] = loc.apply(
            lambda row: self.create_id(row['grouped_index']), axis=1)

        #print loc

        # reorder columns to VIP format
        cols =['election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
               'external_identifier_value', 'name', 'polling_location_ids', 'state_id', 'type',
                'other_type', 'grouped_index', 'id']

        final = loc.reindex(columns=cols)

        final.drop(['grouped_index',], inplace=True, axis=1)
        print final
        return final

    def export_for_precinct(self):
        loc = self.build_locality_txt()

        # loc = loc.groupby('hours_open_id', 'external_identifier_value').agg(lambda x: ' '.join(set(x))).reset_index()
        loc = loc.groupby('external_identifier_value').agg(lambda x: ' '.join(set(x))).reset_index()
        print loc

        # loc['name'] = loc['name'].apply(lambda x: ''.join(x.split(' ')[0]))

        loc['grouped_index'] = loc.index + 1

        loc['name'] = loc.apply(
            lambda row: self.create_name(row['grouped_index'], row['county']), axis=1)

        loc['id'] = loc.apply(
            lambda row: self.create_id(row['grouped_index']), axis=1)

#        loc['merge_key'] = loc.apply(
#            lambda row: self.create_merge_key(row['county']), axis=1)

        #print loc

        # reorder columns to VIP format
        cols = ['election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
                'external_identifier_value', 'name', 'polling_location_ids', 'state_id', 'type',
                'other_type', 'grouped_index', 'id', 'merge_key']

        final = loc.reindex(columns=cols)
        print final
        return final

    def write_locality_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        loc = self.final_build()
        #print loc

        loc.to_csv(config.output + 'locality.txt', index=False, encoding='utf-8')  # send to txt file
        loc.to_csv(config.output + 'locality.csv', index=False, encoding='utf-8')  # send to csv file

if __name__ == '__main__':

#    state_file = 'intermediate_doc.csv'

#    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/alt_south_dakota/output/" + state_file

#    colnames = ['ocd_division', 'email', 'county', 'location_name', 'address_1', 'address_2', 'city',
#                'state', 'zip_code', 'source_start_time', 'source_end_time', 'source_start_date', 'source_end_date',
#                'is_only_by_appointment', 'is_or_by_appointment', 'appointment_phone', 'is_subject_to_change',
#                'index', 'address_line', 'directions', 'hours', 'photo_uri', 'hours_open_id', 'is_drop_box',
#                'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'polling_location_id']

#    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1, delimiter=',')

#    early_voting_df['index'] = early_voting_df.index +1 # offsets zero based index so it starts at 1 for ids
    #print early_voting_df

    state_file = config.state_file

    colnames = ['ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state', 'zip_code',
                'start_time', 'end_time', 'start_date', 'end_date']

    usecols = ['ocd_division', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state', 'zip_code',
                'start_time', 'end_time', 'start_date', 'end_date']

    early_voting_df = pd.read_csv(config.input_path + state_file, names=colnames, usecols=usecols, encoding='ISO-8859-1', skiprows=1)
    early_voting_df['index'] = early_voting_df.index

    pl = PollingLocationTxt(early_voting_df, config.state_abbreviation_upper)
    early_voting_df = pl.export_for_schedule_and_locality()
    print early_voting_df


    lt = LocalityTxt(early_voting_df, config.state)
    lt.write_locality_txt()
    #lt.export_for_precinct()