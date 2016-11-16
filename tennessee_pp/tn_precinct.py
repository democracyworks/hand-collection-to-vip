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
from tn_locality import LocalityTxt
from tn_polling_location import PollingLocationTxt

# voter file

f = 'tsmart_google_geo_20160817_TN_20160429.txt'

voter_file = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\tennessee_pp\source\\" + f


class MergePollingPlacesVF(object):

    def __init__(self):
        pass

    def create_polling_place_df(self):
        # build locality dataframe at run time
        colnames = ['county', 'official_title', 'ocd_division', 'homepage_url', 'phone', 'email', 'precinct',
                    'location_name', 'address1', 'directions', 'city', 'state', 'zip_code', 'start_time', 'end_time',
                    '']

        polling_place_df = pd.read_csv(config.input_path + config.state_file,
                                       names=colnames, sep=',',
                                       encoding='ISO-8859-1',
                                       skiprows=1,
                                       dtype='str'
                                       )
        polling_place_df['index'] = polling_place_df.index + 1

        pl = PollingLocationTxt(polling_place_df)
        pp_df = pl.export_for_locality_precinct()
        del polling_place_df

        lt = LocalityTxt(pp_df, config.state)
        lt_df = lt.export_for_precinct()

        lt_df['merge_key'] = lt_df.apply(
            lambda row: self.create_merge_key(row['county'], row['precinct']), axis=1)
        print lt_df
        del pp_df

        return lt_df

    def create_voter_file_df(self):
        use = ['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
               'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
               'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4', 'vf_reg_cass_street_num',
               'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional',
               'vf_reg_cass_unit_designator', 'vf_reg_cass_apt_num', 'van_precinctid']

        # state voter file import
        df = pd.read_csv(voter_file, sep='\t', names=config.voter_file_columns, usecols=use,
                         encoding='ISO-8859-1', skiprows=1, iterator=True,
                         chunksize=1000,
                         dtype='str',
                         )

        voter_file_df = pd.concat(df, ignore_index=True)

        # Drops duplicate rows based on county name and precinct name leaving a smaller dataframe with the necessary data for precinct.txt.
        voter_file_df = voter_file_df.drop_duplicates(subset=['vf_county_name', 'vf_precinct_name'])

        voter_file_df = voter_file_df[voter_file_df['van_precinctid'].isin(['']) == False]

        voter_file_df['merge_key'] = voter_file_df['vf_county_name'] + '-' + voter_file_df['vf_precinct_name']
        # tn_voter_file_df = df
        return voter_file_df

    def create_merge_key(self, county, precinct):
        """Creates the value used to merge the polling place dataframe with the voter file dataframe.

        The conditionals modify hand collected precinct names on a per county basis
        to match the equivalent name in the voter file.

        """
        county = county.upper().replace(' COUNTY', '')
        print precinct
        try:
            if county == 'DAVIDSON':
                if len(precinct) == 1:
                    return county + '-0' + precinct
                else:
                    mk = county + '-' + precinct.replace('-', '')
                    print mk
                    return mk
            elif county == 'SHELBY':
                mk = county + '-' + precinct.split('-')[0]
                print mk
                return mk
            elif county == 'FRANKLIN':
                return county + '-0' + precinct
            elif county == 'KNOX':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                elif len(precinct) == 2:
                    return county + '-00' + precinct
                elif len(precinct) == 3:
                    return county + '-0' + precinct
                elif (c.isalpha() for c in precinct):
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'SUMNER':
                if len(precinct) == 3:
                    return county + '-0' + precinct.replace('-', '0')
                else:
                    return county + '-' + precinct.replace('-', '0')
            elif county == 'RUTHERFORD':
                if len(precinct) == 1:
                    return county + '-0' + precinct.replace('-', '0')
            elif county == 'DEKALB':
                return county + '-0' + precinct
            elif county == 'MADISON':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                elif len(precinct) == 2:
                    return county + '-00' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'HAMILTON':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                elif len(precinct) == 2:
                    return county + '-00' + precinct
                elif len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'BLOUNT':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                elif len(precinct) == 2:
                    return county + '-00' + precinct
                elif len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'HARDEMAN':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                elif len(precinct) == 2:
                    return county + '-00' + precinct
                elif len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'DICKSON':
                precinct = precinct.replace('-', '')
                if len(precinct) == 2:
                    return county + '-00' + precinct
                else:
                    return county + '-0' + precinct
            elif county == 'TROUSDALE':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'TIPTON':
                if len(precinct) == 1:
                    return county + '-000' + precinct.split('-')[1]
                elif len(precinct) == 3:
                    return county + '-0' + precinct.split('-')[1]
                else:
                    return county + '-00' + precinct.split('-')[1]
            elif county == 'MONTGOMERY':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                elif len(precinct) == 2:
                    return county + '-00' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'MARION':
                precinct = str(precinct).split('-')[1]
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct

            elif county == 'LINCOLN':
                return county + '-0' + precinct
            elif county == 'WILLIAMSON':
                if len(precinct) == 2:
                    return county + '-00' + precinct
                else:
                    return county + '-0' + precinct
            elif county == 'WAYNE':
                return county + '-0' + precinct.split('-')[1]
            elif county == 'FAYETTE':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'BRADLEY':
                return county + '-0' + precinct
            elif county == 'BENTON':
                return county + '-000' + precinct
            elif county == 'CHEATHAM':
                return county + '-0' + precinct.replace(' - ', '-')
            elif county == 'CHESTER':
                return county + '-0' + precinct.split(' ')[0]
            elif county == 'CROCKETT':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'MACON':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'SMITH':
                return county + '-000' + precinct
            elif county == 'LEWIS':
                return county + '-0' + precinct
            elif county == 'LAKE':
                return county + '-000' + precinct
            elif county == 'MOORE':
                return county + '-000' + precinct
            elif county == 'MONROE':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'JEFFERSON':
                return county + '-0' + precinct + '01'
            elif county == 'CARTER':
                return county + '-0' + precinct
            elif county == 'COFFEE':
                if len(precinct) == 3:
                    return county + '-0' + precinct.replace('-', '0')
                else:
                    return county + '-' + precinct.replace('-', '0')
            elif county == 'CLAY':
                return county + '-000' + precinct
            elif county == 'HENDERSON':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'HENRY':
                return county + '-0' + precinct
            elif county == 'CARROLL':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'HAMBLEN':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'HARDIN':
                return county + '-00' + precinct
            elif county == 'HAYWOOD':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'HAWKINS':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'CANNON':
                return county + '-00' + precinct
            elif county == 'HUMPHREYS':
                return county + '-000' + precinct
            elif county == 'LAWRENCE':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'OVERTON':
                return county + '-0' + precinct
            elif county == 'WHITE':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'MCNAIRY':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'BLEDSOE':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'DYER':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'DECATUR':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'GIBSON':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-00' + precinct
            elif county == 'GRUNDY':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'WASHINGTON':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                elif len(precinct) == 2:
                    return county + '-00' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'JACKSON':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'WARREN':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'SCOTT':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'PERRY':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'SCOTT':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'ROBERTSON':
                if len(precinct) == 3:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'RHEA':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-' + precinct

            elif county == 'POLK':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'MARSHALL':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-' + precinct

            elif county == 'HOUSTON':
                if len(precinct) == 1:
                    return county + '-000' + precinct
                else:
                    return county + '-' + precinct

            else:
                mk = county + '-' + precinct
                return mk
        except:
            pass

    def merge(self):
        pp = self.create_polling_place_df()
        vf = self.create_voter_file_df()

        merged_df = pd.merge(vf, pp, how='outer', on=['merge_key'])
        print merged_df
        return merged_df


class PrecinctTxt(object):
    """#
    """

    def __init__(self, merged_df):
        self.base_df = merged_df

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

    def get_external_identifier_value(self, external_identifier_value):
        """Extracts external identifier (ocd-division)."""
        return external_identifier_value

    def is_mail_only(self):
        """#"""
        return ''

    def locality_id(self, locality_id):
        """Set localit_id value."""
        return locality_id

    def name(self, vf_precinct_name):
        """#"""
        return vf_precinct_name

    def number(self):
        """#"""
        return ''

    def get_other_type(self):
        # create conditional when/if column is present
        return ''

    def polling_location_ids(self, polling_location_ids_for_precincts):
        return polling_location_ids_for_precincts

    def create_precinct_split_name(self, van_precinctid):
        """#"""
        return ''

    def ward(self, vf_ward):
        """Sets ward value."""
        return vf_ward

    def create_id(self, van_precinctid):
        """Uses id from voter file."""

        return 'pre' + str(van_precinctid)

    def build_precinct_txt(self):
        """
        New columns that match the 'precinct.txt' template are added to the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        m = self.base_df

        self.base_df['ballot_style_id'] = self.base_df.apply(
            lambda row: self.ballot_style_id(), axis=1)

        self.base_df['electoral_district_ids'] = self.base_df.apply(
            lambda row: self.electoral_district_ids(), axis=1)

        self.base_df['external_identifier_type'] = self.base_df.apply(
            lambda row: self.get_external_identifier_type(), axis=1)

        self.base_df['external_identifier_othertype'] = self.base_df.apply(
            lambda row: self.get_external_identifier_othertype(), axis=1)

        self.base_df['external_identifier_value'] = self.base_df.apply(
            lambda row: self.get_external_identifier_value(row['external_identifier_value']), axis=1)

        self.base_df['is_mail_only'] = self.base_df.apply(
            lambda row: self.is_mail_only(), axis=1)

        self.base_df['locality_id'] = self.base_df.apply(
            lambda row: self.locality_id(row['id']), axis=1)

        self.base_df['name'] = self.base_df.apply(
            lambda row: self.name(row['vf_precinct_name']), axis=1)

        self.base_df['number'] = self.base_df.apply(
            lambda row: self.number(), axis=1)

        self.base_df['polling_location_ids'] = self.base_df.apply(
            lambda row: self.polling_location_ids(row['polling_location_ids_for_precincts']), axis=1)

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
        pt = pt[pt['id'].isin(['prenan']) == False]

        prt = pt.drop_duplicates(subset=['id'])

        cols = ['ballot_style_id', 'electoral_district_ids', 'external_identifier_type', 'external_identifier_othertype',
                'external_identifier_value', 'is_mail_only', 'locality_id', 'name', 'number', 'polling_location_ids',
                'precinct_split_name', 'ward', 'id']

        prt = prt.reindex(columns=cols)
        print prt
        prt.to_csv(config.output + 'precinct.txt', index=False, encoding='utf-8')  # send to csv file


def main():
    m = MergePollingPlacesVF()
    merged_df = m.merge()

    pr = PrecinctTxt(merged_df)
    pr.write_precinct_txt()


if __name__ == '__main__':

    main()