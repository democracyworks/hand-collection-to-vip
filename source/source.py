import pandas as pd
import time
from datetime import date
import csv



class SourceTxt(object):

    def __init__(self, state):
        self.state = state

    @classmethod
    def write_source_txt(cls, arr):
        #This method is going to populate the source CSV with all the necessary data
        output_path = "/Users/danielgilberg/Development/hand-collection-to-vip/test.csv"

        with open(output_path, 'ab') as f:
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



    def get_date_time(self):
        # generate date-time, import date_time module--follows the format provided in the documentation
        date_string = date.today().strftime("%Y-%m-%d")
        time_string = time.strftime("%H:%M:%S")
        return date_string + "T" + time_string


    def get_vip_id(self):
        #the vip_id is the FIPS code for the state, which is available in the dictionary below
        fips_dict = {'Mississippi': '28', 'Oklahoma': '40', 'Delaware': '10', 'Minnesota': '27', 'Illinois': '17', 'Arkansas': '05', 'New Mexico': '35',
                     'Indiana': '18', 'Maryland': '24', 'Louisiana': '22', 'Idaho': '16', 'Wyoming': '56', 'Tennessee': '47', 'Arizona': '04', 'Iowa': '19',
                     'Michigan': '26', 'Kansas': '20', 'Utah': '49', 'Virginia': '51', 'Oregon': '41', 'Connecticut': '09', 'Montana': '30', 'California': '06',
                     'Massachusetts': '25', 'West Virginia': '54', 'South Carolina': '45', 'New Hampshire': '33', 'Wisconsin': '55', 'Vermont': '50', 'Georgia': '13',
                     'North Dakota': '38', 'Pennsylvania': '42', 'Florida': '12', 'Alaska': '02', 'Kentucky': '21', 'Hawaii': '15', 'Nebraska': '31',
                     'Missouri': '29', 'Ohio': '39', 'Alabama': '01', 'New York': '36', 'South Dakota': '46', 'Colorado': '08', 'New Jersey': '34',
                     'Washington': '53', 'North Carolina': '37', 'District of Columbia': '11', 'Texas': '48', 'Nevada': '32', 'Maine': '23', 'Rhode Island': '44'}
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

    arr = ["Alabama"]
    sources = []
    for i in arr:
        lt = SourceTxt(i)
        sources.append(lt)


    SourceTxt.write_source_txt(sources)