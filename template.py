import pandas as pd

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

    def vip_method(self, index):
        """#"""
        pass

    def build_method(self):
        """
        New columns that match the 'locality.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        self.base_df['vip_column'] = self.base_df.apply(
            lambda row: self.vip_column_method(row['index']), axis=1)

        return self.base_df

    def write_method(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        loc = self.build_locality_txt()
        # print loc

        # Drop base_df columns.
        loc.drop(
            ['index', 'office-name', 'official-title', 'types', 'ocd-division', 'division-description', 'homepage-url',
             'phone', 'email', 'street', 'city', 'state', 'zip', 'mailing-street', 'mailing-city',
             'mailing-state', 'mailing-zip'], inplace=True, axis=1)

        print loc

        loc.to_csv('locality.txt', index=False, encoding='utf-8')  # send to txt file
        loc.to_csv('locality.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':
    state = 'maine'  # keep it lower case

    early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/locality/early_voting_location_files/" + state + '_early_voting_locations.csv'
    colnames = ['office-name', 'official-title', 'types', 'ocd-division', 'division-description', 'homepage-url',
                'phone',
                'email', 'street', 'city', 'state', 'zip', 'mailing-street', 'mailing-city', 'mailing-state',
                'mailing-zip']
    early_voting_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)

    early_voting_df['index'] = early_voting_df.index + 1  # offsets zero based index so it starts at 1 for ids

    lt = LocalityTxt(early_voting_df, state)
    lt.write_locality_txt()

