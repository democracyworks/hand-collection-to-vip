"""
Contains class that generates the 'precinct.txt' file.

ballot_style_id,
electoral_district_ids,
external_identifier_type,
external_identifier_othertype,
external_identifier_value,
is_mail_only,
locality_id,
name,
number,
polling_location_ids,
precinct_split_name,
ward,
id




"""

import pandas as pd
import config

# voter file


voter_file = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\texas_pp\source\\" + config.voter_file


def main():

    use = ['vf_source_state', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township', 'vf_ward', 'vf_precinct_id',
           'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full', 'vf_reg_cass_city', 'vf_reg_cass_state',
           'vf_reg_cass_zip', 'vf_reg_cass_zip4', 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional',
           'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
           'vf_reg_cass_apt_num', 'van_precinctid']



    # state voter file import
    df = pd.read_csv(voter_file, sep=',', names=use,
                                 encoding='ISO-8859-1', skiprows=1, iterator=True,
                                 chunksize=1000,
                                 dtype='str'
                                )

    voter_file_df = pd.concat(df, ignore_index=True)
    voter_file_df['index'] = voter_file_df.index

    pr = PrecinctTxt(voter_file_df)
    pr.write_precinct_txt()
    #pr.export_for_street_segments()

class PrecinctTxt(object):
    """#
    """

    def __init__(self, merged_df):
        self.base_df = merged_df
        #print self.base_df

    def ballot_style_id(self):
        """#"""
        return ''

    def electoral_district_ids(self):
        """#"""
        return ''

    def get_external_identifier_type(self):
        """#"""
        return "ocd-id"

    def get_external_identifier_othertype(self):
        """#"""
        return ''

    def get_external_identifier_value(self):
        """Extracts external identifier (ocd-division)."""
        return 'ocd-division/country:us/state:tx/county:tarrant'

    def is_mail_only(self):
        """#"""
        pass

    def locality_id(self):
        """Returns the value from the 'id_y' column. Columns appended with '_y' in the merged dataframe
         are from locality.txt"""
        return ''

    def name(self, vf_precinct_name):
        """#"""
        return vf_precinct_name

    def number(self):
        """#"""
        return ''

    def get_other_type(self):
        # create conditional when/if column is present
        return ''
    def polling_location_ids(self):
        return ''

    def create_precinct_split_name(self, van_precinctid):
        """#"""
        return ''
        #return 'split' + str(van_precinctid) + '-' + str(index)

    def ward(self, vf_ward):
        """#"""
        return vf_ward

    def create_id(self, van_precinctid):
        """Uses id from voter file."""

        return 'pre' + str(van_precinctid)

    def build_precinct_txt(self):
        """
        New columns that match the 'locality.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

        self.base_df['ballot_style_id'] = self.base_df.apply(
            lambda row: self.ballot_style_id(), axis=1)

        self.base_df['electoral_district_ids'] = self.base_df.apply(
            lambda row: self.electoral_district_ids(), axis=1)

        self.base_df['external_identifier_type'] = self.base_df.apply(
            lambda row: self.get_external_identifier_type(), axis=1)

        self.base_df['external_identifier_othertype'] = self.base_df.apply(
            lambda row: self.get_external_identifier_othertype(), axis=1)

        self.base_df['external_identifier_value'] = self.base_df.apply(
            lambda row: self.get_external_identifier_value(), axis=1)

        self.base_df['is_mail_only'] = self.base_df.apply(
            lambda row: self.is_mail_only(), axis=1)

        self.base_df['locality_id'] = self.base_df.apply(
            lambda row: self.locality_id(), axis=1)

        self.base_df['name'] = self.base_df.apply(
            lambda row: self.name(row['vf_precinct_name']), axis=1)

        self.base_df['number'] = self.base_df.apply(
            lambda row: self.number(), axis=1)

        self.base_df['polling_location_ids'] = self.base_df.apply(
            lambda row: self.polling_location_ids(), axis=1)

        self.base_df['precinct_split_name'] = self.base_df.apply(
            lambda row: self.create_precinct_split_name(row['van_precinctid']), axis=1)

        self.base_df['ward'] = self.base_df.apply(
            lambda row: self.ward(row['vf_ward']), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['van_precinctid']), axis=1)

        return self.base_df

    def write_precinct_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        pt = self.build_precinct_txt()

        prt = pt.drop_duplicates(subset=['id'])

        cols = ['ballot_style_id', 'electoral_district_ids', 'external_identifier_type', 'external_identifier_othertype',
                'external_identifier_value', 'is_mail_only', 'locality_id', 'name', 'number', 'polling_location_ids',
                'precinct_split_name', 'ward', 'id']

        prt = prt.reindex(columns=cols)
        #print prt

        prt.to_csv(config.output + 'precinct.txt', index=False, encoding='utf-8')  # send to csv file



if __name__ == '__main__':

    main()