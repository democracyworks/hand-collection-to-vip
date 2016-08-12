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


state_dict = {'wyoming': 50, 'colorado': 6, 'washington': 47, 'hawaii': 11, 'tennessee': 42, 'wisconsin': 49,
              'nevada': 28, 'maine': 19, 'north dakota': 34, 'mississippi': 24, 'south dakota': 41,
              'new jersey': 30, 'oklahoma': 36, 'delaware': 8, 'minnesota': 23, 'north carolina': 33,
              'illinois': 13, 'new york': 32, 'arkansas': 4, 'indiana': 14, 'maryland': 20, 'louisiana': 18,
              'idaho': 12, 'south  carolina': 40, 'arizona': 3, 'iowa': 15, 'west virginia': 48, 'michigan': 22,
              'kansas': 16, 'utah': 44, 'virginia': 46, 'oregon': 37, 'connecticut': 7, 'montana': 26,
              'california': 5, 'massachusetts': 21, 'rhode island': 39, 'vermont': 45, 'georgia': 10,
              'pennsylvania': 38, 'florida': 9, 'alaska': 2, 'kentucky': 17, 'nebraska': 27, 'new hampshire': 29,
              'texas': 43, 'missouri': 25, 'ohio': 35, 'alabama': 1, 'new mexico': 31}


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
        prefix = 'ea'

        if index <= 9:
            index_str = '000' + str(index)
            return prefix + index_str

        elif index in range(10,100):
            index_str = '00' + str(index)
            return prefix + index_str

        elif index >= 100:
            index_str = '0' + str(index)
            return prefix + index_str

        else:
            index_str = str(index)
            return prefix + index_str

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
            locality = division_description[:-3].lower().replace(" ", "_")
            #print locality
        else:
            locality = ''
            print 'Missing data at row ' + str(index) + '.'

        # Add leading '0s' depending on index number.
        if index <= 9:
            index_str = '000' + str(index)

        elif index in range(10,100):
            index_str = '00' + str(index)

        elif index >= 100:
            index_str = '0' + str(index)
        else:
            index_str = str(index)

        return locality + index_str

    def create_polling_location_ids(self, index):
        """
        Creates polling_location_ids by concatenating 'poll' with an 'index_str' based on the Dataframe's row index.
        '0s' are added, if necesary, to maintain a consistent id length.
        """

        if index <= 9:
            index_str = '000' + str(index)
            return 'poll' + index_str

        elif index in range(10, 100):
            index_str = '00' + str(index)
            return 'poll' + index_str

        elif index >= 100:
            index_str = '0' + str(index)
            return 'poll' + index_str
        elif index:
            index_str = str(index)
            return 'poll' + index_str

        else:
            return ''

    def create_state_id(self):
        """Creates the state_id by matching a key in the state_dict and retrieving
        and modifying its value. A '0' is added, if necessary, to maintain a
        consistent id length.
        """
        for key, value in state_dict.iteritems():
            if key == self.state:
                state_num = value
                if state_num <=9:
                    state_num = '0' + str(state_num)
                else:
                    state_num = str(state_num)

                return 'st' + state_num

    def get_type(self):
        # create conditional when/if column is present
        return ''

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

        elif index >= 100:
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
            #lambda row: self.get_external_identifier_type(), axis=1)
            # TODO: temporarily providing empty string
            lambda row: '', axis = 1)

        self.base_df['external_identifier_othertype'] = self.base_df.apply(
            lambda row: self.get_external_identifier_othertype(), axis=1)

        self.base_df['external_identifier_value'] = self.base_df.apply(
            lambda row: self.get_external_identifier_value(row['ocd_division']), axis=1)

        self.base_df['name'] = self.base_df.apply(
            lambda row: self.create_name(row['index'], row['division_description']), axis=1)

        self.base_df['polling_location_ids'] = self.base_df.apply(
            #lambda row: self.create_polling_location_ids(row['index']), axis=1)
            # TODO: temporarily providing empty string
            lambda row: '', axis=1)

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

    def dedupe(self, dupe):
        """#"""
        return dupe.drop_duplicates(subset='external_identifier_value')

    def write_locality_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        loc = self.build_locality_txt()
        #print loc

        # Drop base_df columns.
        loc.drop(['office_name', 'official_title', 'ocd_division', 'division_description', 'homepage_url', 'phone', 'email',
                'street', 'directions', 'city', 'state', 'zip', 'start_time', 'end_time', 'start_date', 'end_date',
                'must_apply_for_mail_ballot', 'notes'], inplace=True, axis=1)

        #loc = self.dedupe(loc)
        print loc
        #a = self.group_polling_location_ids(loc)
        #print type(a)
        #print a

        loc.to_csv(config.locality_output + 'locality.txt', index=False, encoding='utf-8')  # send to txt file
        loc.to_csv(config.locality_output + 'locality.csv', index=False, encoding='utf-8')  # send to csv file

if __name__ == '__main__':

    state_file = 'new_jersey_early_voting_info.csv'

    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/new_jersey/scripts/early_voting_input/" + state_file

    colnames = ['office_name', 'official_title', 'ocd_division', 'division_description', 'homepage_url', 'phone', 'email',
                'street', 'directions', 'city', 'state', 'zip', 'start_time', 'end_time', 'start_date', 'end_date',
                'must_apply_for_mail_ballot', 'notes']
    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1, delimiter=';')

    early_voting_df['index'] = early_voting_df.index +1 # offsets zero based index so it starts at 1 for ids
    #print early_voting_df

    lt = LocalityTxt(early_voting_df, config.state)
    lt.write_locality_txt()