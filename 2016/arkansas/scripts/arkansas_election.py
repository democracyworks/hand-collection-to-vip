"""

id,
date,
name,
election_type,
state_id,
is_statewide,
registration_info,
absentee_ballot_info,
results_uri,
polling_hours,
has_election_day_registration,
registration_deadline,
absentee_request_deadline,
hours_open_id


"""

import datetime
import csv
import config
from arkansas_polling_location import PollingLocationTxt
import pandas as pd


class ElectionTxt(object):

    def __init__(self, base_df, state_feed):
        self.base_df = base_df
        self.state_feed = state_feed
        #print self.base_df
        #print state_feed


    def create_election_id(self, index):
        """Leading zeroes are added, if necessary, to maintain a
        consistent id length.


        """
        if index <= 9:
            index_str = '000' + str(index)

        elif index in range(10, 100):
            index_str = '00' + str(index)

        elif index in range(100, 1000):
            index_str = '0' + str(index)
        else:
            index_str = str(index)

        return 'e' + str(index_str)

    def get_date(self):
        """#"""
        return '2016-11-08'

    def get_name(self):
        """#"""
        return "2016 General Election"

    def get_election_name(self):
        return "2016 General"

    def get_election_type(self):
        """#"""
        return 'federal'

    #def get_state_id(self):
     #   """#"""
        # get state name, lower()

      #  pass

    def create_state_id(self):
        """Creates the state_id by matching a key in the state_dict and retrieving
        and modifying its value. A '0' is added, if necessary, to maintain a
        consistent id length.
        """
        # TODO: use fips code
        for key, value in config.fips_dict.iteritems():
            if key == config.state.lower():
                state_num = value
                if state_num <=9:
                    state_num = '0' + str(state_num)
                else:
                    state_num = str(state_num)

                return 'st' + state_num

    def is_statewide(self):
        """#"""
        return 'true'

    def registration_info(self):
        """#"""
        return ''

    def absentee_ballot_info(self):
        """#"""
        return ''

    def results_uri(self):
        """#"""
        return ''

    def polling_hours(self, hours):
        """Takes hours from polling_location."""
        return hours

    def has_election_day_registration(self):
        """#"""
        return 'false'

    def registration_deadline(self, index):
        """Grab registration_deadline from state_feed document."""

        for index, row in self.state_feed.iterrows():
            if row['office_name'] == config.state:
                return row['registration_deadline']
        else:
            print 'Missing value at row '  + str(index) + '.'
            return ''


    def absentee_request_deadline(self, index):
        """Grab ballot_request_deadline_display from state_feed document."""

        for index, row in self.state_feed.iterrows():
            if row['office_name'] == config.state:
                return row['ballot_request_deadline']
        else:
            print 'Missing value at row '  + str(index) + '.'
            return ''

    def hours_open_id(self):
        """#"""
        return ''

    def build_election_txt(self):
        """
        New columns that match the 'schedule.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_election_id(row['index']), axis=1)

        self.base_df['date'] = self.base_df.apply(
            lambda row: self.get_date(), axis=1)

        self.base_df['name'] = self.base_df.apply(
            lambda row: self.get_name(), axis=1)

        self.base_df['election_type'] = self.base_df.apply(
            lambda row: self.get_election_type(), axis=1)

        self.base_df['state_id'] = self.base_df.apply(
            lambda row: self.create_state_id(), axis=1)

        self.base_df['is_statewide'] = self.base_df.apply(
            lambda row: self.is_statewide(), axis=1)

        self.base_df['registration_info'] = self.base_df.apply(
            lambda row: self.registration_info(), axis=1)

        self.base_df['absentee_ballot_info'] = self.base_df.apply(
            lambda row: self.absentee_ballot_info(), axis=1)

        self.base_df['results_uri'] = self.base_df.apply(
            lambda row: self.results_uri(), axis=1)

        self.base_df['polling_hours'] = self.base_df.apply(
            lambda row: self.polling_hours(row['hours']), axis=1)

        self.base_df['has_election_day_registration'] = self.base_df.apply(
            lambda row: self.has_election_day_registration(), axis=1)
        #
        self.base_df['registration_deadline'] = self.base_df.apply(
            lambda row: self.registration_deadline(row['index']), axis=1)

        self.base_df['absentee_request_deadline'] = self.base_df.apply(
            lambda row: self.absentee_request_deadline(row['index']), axis=1)

        self.base_df['hours_open_id'] = self.base_df.apply(
            lambda row: self.hours_open_id(), axis=1)

        #print self.base_df
        return self.base_df


    def write(self):

        et = self.build_election_txt()

        et.drop(['ocd_division', 'email', 'county', 'source_name', 'address_one', 'address_two', 'city', 'state', 'zip',
                'start_time', 'end_time', 'start_date', 'end_date', 'by_appointment_one', 'by_appointment_two', 'appointmnents',
                'subject_to_change', 'index', 'address_line', 'directions',
                'hours', 'photo_uri', 'is_drop_box', 'is_early_voting', 'lat', 'long', 'latlng', 'source_id'], inplace=True, axis=1)

        cols = ["id", "date", "name", "election_type", "state_id", "is_statewide", "registration_info",
                'absentee_ballot_info', 'results_uri', "polling_hours", 'has_election_day_registration', 'registration_deadline',
                'absentee_request_deadline', 'hours_open_id']

        et = et.reindex(columns=cols)

        print et


        et.to_csv(config.output + 'election.txt', index=False, encoding='utf-8')  # send to txt file
        et.to_csv(config.output + 'election.csv', index=False, encoding='utf-8')  # send to csv file

#    def write_election_txt(self):
#        output_path = "/home/acg/democracyworks/hand-collection-to-vip/minnesota/output/election.txt"

#        try:
##            f = open(output_path, 'ab')
#            fieldnames = ['id', 'date', 'name', 'election_type', 'state_id', 'is_statewide',
#                          'registration_info', 'absentee_ballot_info', 'results_uri',
#                          'polling_hours', 'has_election_day_registration', 'registration_deadline',
#                          'absentee_request_deadline', 'hours_open_id']
#            writer = csv.DictWriter(f, fieldnames=fieldnames)
#            writer.writeheader()
#            writer.writerow({'id': self.create_id(),
#                             'date': self.get_date(),
#                             'name':  self.get_name(),
#                            'election_type': self.get_election_type(),
#                             'state_id': self.create_state_id(),
#                             'is_statewide': self.is_statewide(),
#                             'registration_info': '',
#                             'absentee_ballot_info': '',
#                             'results_uri': self.results_uri(),
#                             'polling_hours': '',
#                             'has_election_day_registration': self.has_election_day_registration(),
#                             'registration_deadline': self.registration_deadline(),
#                             'absentee_request_deadline': self.absentee_request_deadline(),
#                             'hours_open_id': self.hours_open_id()
#                            })
#        finally:
#            f.close()

if __name__ == '__main__':

    state_feed_file = 'state_feed_info.csv'
    early_voting_file = 'arkansas_early_voting_info.csv'

    early_voting_path =config.output + "intermediate_doc.csv"
    #early_voting_path = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/kansas_early_voting_info.csv"
    colnames = ['ocd_division', 'email', 'county', 'source_name', 'address_one', 'address_two', 'city', 'state', 'zip',
                'start_time', 'end_time', 'start_date', 'end_date', 'by_appointment_one', 'by_appointment_two', 'appointmnents',
                'subject_to_change', 'index', 'address_line', 'directions',
                'hours', 'photo_uri', 'hours_open_id', 'is_drop_box', 'is_early_voting', 'lat', 'long', 'latlng', 'source_id']
    early_voting_df = pd.read_csv(early_voting_path, names=colnames, encoding='utf-8', skiprows=1)

    early_voting_df['index'] = early_voting_df.index + 1


    state_feed_path = "/Users/danielgilberg/Development/hand-collection-to-vip/arkansas/input/" + state_feed_file
    colnames = ['office_name', 'ocd_division', 'same_day_reg', 'election_date', 'election_name', 'registration_deadline',
                "registration_deadline_display", 'ballot_request_deadline', 'ballot_request_deadline_display']
    state_feed_df = pd.read_csv(state_feed_path, names=colnames, encoding='utf-8', skiprows=1)
    state_feed_df['index'] = state_feed_df.index + 1

    # print state_feed_df


    et = ElectionTxt(early_voting_df, state_feed_df)
    et.write()