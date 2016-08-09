import pandas as pd
import time
from datetime import date
import csv



class SourceTxt(object):
    @classmethod
    def write_source_txt(cls, arr):
        #This method is going to populate the source CSV with all the necessary data
        output_path = "/Users/danielgilberg/Development/hand-collection-to-vip/test.csv"

        try:
            f = open(output_path, 'ab')
            fieldnames = ["date_time", 'description', 'name', 'organization_uri', 'terms_of_use_uri', 'vip_id', 'version', 'id']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in arr:
                writer.writerow({'date_time': item.get_date_time(),
                             'description': item.get_description(),
                             'name':  item.get_name(),
                             'organization_uri': item.get_organization_uri(),
                             'terms_of_use_uri': item.get_terms_of_use_uri(),
                             'vip_id': item.get_vip_id(),
                             'version': item.get_version(),
                             'id': item.get_id()
                            })
        finally:
            f.close()

    def __init__(self, state):
        self.state = state
        # self.base_df = base_df
        x = 1

    def get_date_time(self):
        # generate date-time, import date_time module--follows the format provided in the documentation
        date_string = date.today().strftime("%Y-%m-%d")
        time_string = time.strftime("%H:%M:%S")
        return date_string + "T" + time_string


    def get_vip_id(self):
        #the vip_id is the FIPS code for the state, which can be found online
        states = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
                  "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
                  "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi",
                  "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
                  "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
                  "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]
        codes = ["01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", "18", "19",
                 "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34","35",
                 "36", "37", "38", "39", "40", "41","42", "44", "45", "46", "47", "48", "49", "50", "51", "53",
                 "54", "55", "56"]
        fips_dict= dict(zip(states, codes))
        if self.state in fips_dict:
            return fips_dict[self.state]
        else:
            return ''

    def get_version(self):
        #current version was 5.1 for all instances
        return "5.1"

    def get_description(self):
        return ''

    def get_organization_uri(self):
        #was instructed to leave this field blank
        return ''

    def get_terms_of_use_uri(self):
        #was instructed to leave this field blank
        return ''

    def get_name(self):
        #name is Democracy works for all instances
        return "Democracy Works"

    def get_id(self):
        return ''

#    def write_sourcetxt(self):
#        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

#        loc = self.build_sourcetxt_method()
        #print loc

        # Drop base_df columns.
#        loc.drop(['date_time', 'description', 'names', 'organization_uri', 'terms_of_use_uri', 'vip_id', 'version',

    #date_time, description, name, organization_uri, terms_of_use_uri, vip_id, version, id
    # def write_source_txt(cls, arr):
    #     output_path = "/Users/danielgilberg/Development/hand-collection-to-vip/test.csv"
    #
    #     try:
    #         f = open(output_path, 'ab')
    #         fieldnames = ["date_time", 'description', 'name', 'organization_uri', 'terms_of_use_uri', 'vip_id', 'version', 'id']
    #         writer = csv.DictWriter(f, fieldnames=fieldnames)
    #         writer.writeheader()
    #         for item in arr:
    #             writer.writerow({'date_time': item.get_date_time(),
    #                          'description': item.get_vip_id(),
    #                          'name':  item.get_name(),
    #                          'organization_uri': item.get_organization_uri(),
    #                          'terms_of_use_uri': item.get_terms_of_use_uri(),
    #                          'vip_id': item.get_vip_id(),
    #                          'version': item.get_version(),
    #                          'id': item.get_id()
    #                         })
    #     finally:
    #         f.close()

if __name__ == '__main__':

    arr = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
                  "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
                  "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi",
                  "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
                  "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
                  "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]
    sources = []
    for i in arr:
        lt = SourceTxt(i)
        sources.append(lt)

    SourceTxt.write_source_txt(sources)