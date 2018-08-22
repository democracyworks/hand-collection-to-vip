

state = 'new hampshire'
state_upper = 'New Hampshire'

state_abbreviation = 'nh'
state_abbreviation_upper = 'NH'

utc_offset = ''

early_voting = 'false'

state_file = 'New Hampshire Polling Places-11-2.csv'

state_feed = ''

input_path = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\new_hampshire_pp\input\\"

#output = '/home/acg/democracyworks/hand-collection-to-vip/new_hampshire_pp/output/'

output = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\new_hampshire_pp\output\\"

#source_output = "/home/acg/democracyworks/hand-collection-to-vip/dev_scripts/output/source.txt"


polling_place_columns = ['county', 'precinct', 'official_title', 'ocd_division', 'homepage_url', 'phone', 'email', 'name',
                         'street', 'city', 'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date']

fips_dict = {'wyoming': '56', 'colorado': '08', 'washington': '53', 'hawaii': '15', 'tennessee': '47', 'iowa': '19',
             'nevada': '32', 'maine': '23', 'north dakota': '38', 'mississippi': '28', 'south dakota': '46',
             'new jersey': '34', 'oklahoma': '40', 'delaware': '10', 'minnesota': '27', 'north carolina': '37',
             'illinois': '17', 'new york': '36', 'arkansas': '05', 'indiana': '18', 'maryland': '24', 'louisiana': '22',
             'texas': '48', 'district of columbia': '11', 'arizona': '04', 'wisconsin': '55', 'michigan': '26',
             'kansas': '20', 'utah': '49', 'virginia': '51', 'oregon': '41', 'connecticut': '09', 'montana': '30',
             'california': '06', 'new mexico': '35', 'rhode island': '44', 'vermont_pp': '50', 'georgia': '13',
             'pennsylvania': '42', 'florida': '12', 'alaska': '02', 'kentucky': '21', 'missouri': '29',
             'nebraska': '31', 'new hampshire': '33', 'idaho': '16', 'west virginia': '54', 'south carolina': '45',
             'ohio': '39', 'alabama': '01', 'massachusetts': '25'}

# for use with precinct and street_segment scripts
voter_file_columns = ['voterbase_id', 'tsmart_exact_track', 'tsmart_exact_address_track', 'vf_voterfile_update_date',
            'vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
            'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_city_council',
            'vf_municipal_district', 'vf_school_district', 'vf_judicial_district', 'reg_latitude', 'reg_longitude',
            'reg_level', 'reg_census_id', 'reg_dma', 'reg_dma_name', 'reg_place', 'reg_place_name',
            'vf_reg_address_1', 'vf_reg_address_2', 'vf_reg_city', 'vf_reg_state', 'vf_reg_zip', 'vf_reg_zip4',
            'vf_reg_cass_address_full', 'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip',
            'vf_reg_cass_zip4', 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name',
            'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
            'vf_reg_cass_apt_num', 'reg_address_usps_address_code', 'reg_address_carrier_route',
            'reg_address_dpv_confirm_code', 'reg_address_dpv_footnote', 'vf_mail_street', 'vf_mail_city',
            'vf_mail_state', 'vf_mail_zip5', 'vf_mail_zip4', 'vf_mail_house_number', 'vf_mail_pre_direction',
            'vf_mail_street_name', 'vf_mail_street_type', 'vf_mail_post_direction', 'vf_mail_apt_type',
            'vf_mail_apt_num', 'van_precinctid']
