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
        return None

#        if index <= 9:
#            index_str = '000' + str(index)
#            return prefix + index_str

#        elif index in range(10,100):
#            index_str = '00' + str(index)
#            return prefix + index_str

#        elif index in range(100, 1000):
#            index_str = '0' + str(index)
#            return prefix + index_str

#        else:
#            index_str = str(index)
#            return prefix + index_str

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

    def create_name(self, index, division_description):
        """
        Creates a name by concatenating the 'locality' (town name along with town or county designation)
        with an 'index_str' based on the Dataframes row index.'0s' are added, if necesary, to
        maintain a consistent id length.
        """

        # Get locality(town or county), and remove state abbreviation.
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

    def create_polling_location_ids(self, polling_location_id):
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
        self.base_df['election_administration_id'] = self.base_df.apply(
            lambda row: self.create_election_administration_id(row['index']), axis=1)

        self.base_df['external_identifier_type'] = self.base_df.apply(
            lambda row: self.get_external_identifier_type(), axis=1)

        self.base_df['external_identifier_othertype'] = self.base_df.apply(
            lambda row: self.get_external_identifier_othertype(), axis=1)

        self.base_df['external_identifier_value'] = self.base_df.apply(
            lambda row: self.get_external_identifier_value(row['ocd_division']), axis=1)

        self.base_df['name'] = self.base_df.apply(
            lambda row: self.create_name(row['index'], row['division_description']), axis=1)

        self.base_df['polling_location_ids'] = self.base_df.apply(
            lambda row: self.create_polling_location_ids(row['polling_location_id']), axis=1)

        self.base_df['state_id'] = self.base_df.apply(
            lambda row: self.create_state_id(), axis=1)

        self.base_df['type'] = self.base_df.apply(
            lambda row: self.get_type(), axis=1)

        self.base_df['other_type'] = self.base_df.apply(
            lambda row: self.get_other_type(), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['index']), axis=1)

        return self.base_df

#    def group_polling_location_ids(self, frame):
        #frame = self.build_locality_txt()
#        return pd.concat(g for _, g in frame.groupby("external_identifier_value") if len(g) > 1)
        #return frame.groupby('external_identifier_value')

#    def dedupe(self, dupe):
#        """#"""
#        return dupe.drop_duplicates(subset='external_identifier_value')

    def final_build(self):

        loc = self.build_locality_txt()
        #print loc

        # Group by county.
        loc = loc.groupby(['external_identifier_value']).agg(lambda x: ' '.join(set(x))).reset_index()
        #print loc

        loc['name'] = loc['name'].apply(lambda x: ''.join(x.split(' ')[0]))

        loc['grouped_index'] = loc.index + 1

        loc['id'] = loc.apply(
            lambda row: self.create_id(row['grouped_index']), axis=1)

        # reorder columns
        cols =['election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
               'external_identifier_value', 'name', 'polling_location_ids', 'state_id', 'type',
                'other_type', 'grouped_index', 'id']

        final = loc.reindex(columns=cols)
        #print final

        final.drop(['grouped_index'], inplace=True, axis=1)
        return final

    def write_locality_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        loc = self.final_build()
        #print loc

        loc.to_csv(config.output + 'locality.txt', index=False, encoding='utf-8')  # send to txt file
        loc.to_csv(config.output + 'locality.csv', index=False, encoding='utf-8')  # send to csv file

if __name__ == '__main__':

    state_file = 'intermediate_doc.csv'

    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/new_jersey/output/" + state_file

    colnames = ['office_name', 'official_title', 'ocd_division', 'division_description', 'homepage_url', 'phone',
                'email', 'street', 'directions', 'city', 'state', 'zip', 'start_time', 'end_time', 'start_date',
                'end_date', 'must_apply_for_mail_ballot', 'notes', 'index', 'address_line', 'hours', 'photo_uri',
                'hours_open_id', 'is_drop_box', 'is_early_voting', 'latitude', 'longitude', 'latlng_source', 'polling_location_id']

    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1, delimiter=',')

    early_voting_df['index'] = early_voting_df.index +1 # offsets zero based index so it starts at 1 for ids
    #print early_voting_df

    lt = LocalityTxt(early_voting_df, config.state)
    lt.write_locality_txt()