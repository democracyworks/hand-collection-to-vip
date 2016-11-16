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


def main():
    input = 'state_feed_info.csv'

    early_voting_file = config.input_path + input

    with open(early_voting_file, 'rb') as f:
        f = csv.reader(f, delimiter=',')
        row_count = 0

        state_feed = {}
        for row in f:
            # print row
            if row[0].lower() == config.state:
                state_feed['office_name'] = row[0]
                state_feed['ocd_division'] = row[1]
                state_feed['same_day_reg'] = row[2]
                state_feed['election_date'] = row[3]
                state_feed['election_name'] = row[4]
                state_feed['registration_deadline'] = row[5]
                state_feed['registration_deadline_display'] = row[6]
                state_feed['ballot_request_deadline'] = row[7]
                state_feed['ballot_request_deadline_display'] = row[8]

                # print state_feed

                et = ElectionTxt(state_feed)
                et.write_election_txt()
                et.absentee_request_deadline()


class ElectionTxt(object):

    def __init__(self, state_feed):
        self.state_feed = state_feed


    def create_id(self):
        """Creates the id by matching a key in the state_dict and retrieving
        and modifying its value. A '0' is added, if necessary, to maintain a
        consistent id length.
        """
        return 'e0001'
        # TODO: use fips code

    def get_date(self):
        """#"""
        return '2016-11-08'

    def get_name(self):
        """#"""
        return '2016 General Election'

    def get_election_type(self):
        """#"""
        return 'federal'

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

    def polling_hours(self):
        """#"""
        # TODO: take from early voting doc
        pass

    def has_election_day_registration(self):
        """#"""
        return 'false'

    def registration_deadline(self):
        """Uses the state_feed dict's registration_deadline value."""

        if self.state_feed.get('registration_deadline'):
            deadline = self.state_feed.get('registration_deadline')
            #print deadline
            return deadline
            #return datetime.datetime.strptime(deadline, '%Y-%m-%d').strftime('%-m/%d/%Y')
        else:
            #raise ValueError('Registration deadline value is not valid.')
            return ''

    def absentee_request_deadline(self):
        """Uses the state_feed dict's ballot_request_deadline value."""

        if self.state_feed.get('ballot_request_deadline'):
            deadline = self.state_feed.get('ballot_request_deadline')
            #print deadline
            return deadline
            #return datetime.datetime.strptime(deadline, '%Y-%m-%d').strftime('%-m/%d/%Y')
        else:
            #raise ValueError('Absentee_request deadline value is not valid.')
            return ''

    def hours_open_id(self):
        """#"""
        return ''

    def write_election_txt(self):
        #output_path = "/home/acg/democracyworks/hand-collection-to-vip/texas/output/election.txt"
        output = config.output + 'election.txt'

        try:
            f = open(output, 'ab')
            fieldnames = ['id', 'date', 'name', 'election_type', 'state_id', 'is_statewide',
                          'registration_info', 'absentee_ballot_info', 'results_uri',
                          'polling_hours', 'has_election_day_registration', 'registration_deadline',
                          'absentee_request_deadline', 'hours_open_id']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({'id': self.create_id(),
                             'date': self.get_date(),
                             'name':  self.get_name(),
                             'election_type': self.get_election_type(),
                             'state_id': self.create_state_id(),
                             'is_statewide': self.is_statewide(),
                             'registration_info': '',
                             'absentee_ballot_info': '',
                             'results_uri': self.results_uri(),
                             'polling_hours': '',
                             'has_election_day_registration': self.has_election_day_registration(),
                             'registration_deadline': self.registration_deadline(),
                             'absentee_request_deadline': self.absentee_request_deadline(),
                             'hours_open_id': self.hours_open_id()
                            })
        finally:
            f.close()


if __name__ == '__main__':
    main()