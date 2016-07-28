import pandas as pd
import re
import requests
import time

# Set the input path
input_path = 'C:/Problem3_Data'
country_input = input_path + '/Problem 3 Input Data - Country Map.txt'
raw_input = input_path + '/Problem 3 Input Data.txt'
cleaned_data = input_path + '/cleansed.txt'
country_map_names = ['CountryCode', 'CountryName']

# place the country mapping data into a dataframe
country_map = pd.read_table(country_input, sep='|', header=0, engine='python', names=country_map_names)


def filter_action(frame, col_name, target_val):
    """Print out a specific row's contents"""
    print(frame.loc[frame[col_name] == target_val])


# Verification of formatting issues with a known pipe-existing field (Tanzania)
filter_action(country_map, 'CountryCode', 'TZ')
# The input data frame contains pipes '|' within the field 'Country Name'.  Replace them with ','
country_map['CountryName'] = country_map['CountryName'].str.replace('|', ',')
# Verification of changing the entry
filter_action(country_map, 'CountryCode', 'TZ')
# Remove null keys from the data frame
country_map = country_map[pd.notnull(country_map['CountryCode'])]
# verify that the lookup table has unique keys
ref_row_count = len(country_map)
unique_row_count = len(country_map.groupby('CountryCode'))
if ref_row_count == unique_row_count:
    print("Row count of raw reference table: %d, unique row count: %d.  Match pass." %
          (ref_row_count, unique_row_count))
else:
    print("Row counts do not match.  Raw counts: %d, unique counts: %d." %
          (ref_row_count, unique_row_count))

# save the cleaned file to disk.
country_map.to_csv('C:/Problem3_Data/CountryMap.csv', sep='|', encoding='utf-8')

# The data source contains improperly formatted unicode characters.
# Example: row 97523: 'BIENK#|98|#WKA'|'PL' which is not a unicode definition for what it should
# be (U+00F3).  There are 23 total times this appears in this data set.
raw_input_data = open(raw_input)
new_file = open(cleaned_data, 'w')
for line in raw_input_data:
    line = re.sub(r'#\|98\|#', 'O', line.rstrip())
    line = re.sub(r'\'', '', line.rstrip())
    line = re.sub(r'[\d#.()+]', '', line.rstrip())
    line = re.sub(r'-', ' ', line.rstrip())
    line = re.sub(r', ', ' ', line.rstrip())
    line = re.sub(r' ,', ' ', line.rstrip())
    line = re.sub(r'^,', '', line.rstrip())
    line = re.sub(r',\|', '|', line.rstrip())
    line = re.sub(r'\"', '', line.rstrip())
    line = re.sub(r'\s+', ' ', line.rstrip())
    new_file.write(line + '\n')
new_file.close()

# Set the field names for
input_names = ['CityName', 'CountryCode']

# Now open the cleansed data set in a data frame.
input_data = pd.read_table(cleaned_data, sep='|', header=0, engine='python', names=input_names,
                           skipinitialspace=True)

# this was used to get the unique values of CountryCode in the larger data set
# in order to verify that no invalid country codes were present:
#
# unique_country_code = pd.DataFrame({'count': input_data.groupby(by='CountryCode').size()}).reset_index()
# print("Unique Countries in raw data: ", len(unique_country_code))
# for index, row in unique_country_code.iterrows():
#     print(row['CountryCode'])
#
# Note: The country code 'AN' is no longer associated with Netherlands Antilles as
# this country was dissolved in 2010.

# Verify that the data has been cleaned properly
print(input_data[:10])

# get a unique list and the count of all of the cities and countries for geo-loc purposes
unique_cities = pd.DataFrame({'count': input_data.groupby(
    by=('CityName', 'CountryCode')).size()}).reset_index()
# sort the data frame by the largest counts
unique_sort = unique_cities.sort_values(by='count', ascending=0)
print(unique_sort[:10])

g_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
g_key = 'AIzaSyA5TX9wDRR60A1wrv_pwRJyleBz3NZ25g0'


def geo_loc(city, country):
    """Query the google maps API and retrieve unique city / country data"""
    address_1 = g_url + 'address=' + city
    address_2 = '&components=country:' + country + '&key='
    query_url = address_1 + address_2 + g_key
    # submit and get the response
    try:
        response = requests.get(query_url)
        try:
            results = response.json()['results']
            # retrieve information about the match
            x_comp = results[0]['address_components']
            x_city_name = x_comp[0]['long_name'].upper()
            x_county = x_comp[1]['long_name'].upper()
            x_state_name = x_comp[2]['long_name'].upper()
            x_state_abbr = x_comp[2]['short_name'].upper()
            x_country_full = x_comp[3]['long_name'].upper()
            x_country_iso = x_comp[3]['short_name'].upper()
            # retrieve the lat/long
            x_geo = results[0]['geometry']['location']
            g_lat = x_geo['lat']
            g_long = x_geo['lng']
            return (x_city_name, x_county, x_state_name, x_state_abbr, x_country_full, x_country_iso,
                    g_lat, g_long)
        except IndexError:
            x_city_name = ''
            x_county = ''
            x_state_name = ''
            x_state_abbr = ''
            x_country_full = ''
            x_country_iso = ''
            g_lat = None
            g_long = None
            return (x_city_name, x_county, x_state_name, x_state_abbr, x_country_full, x_country_iso,
                    g_lat, g_long)
            pass
    except requests.exceptions.Timeout:
        time.sleep(5)
        geo_loc(city, country)
    except requests.exceptions.RequestException as e:
        print(e)
        x_city_name = ''
        x_county = ''
        x_state_name = ''
        x_state_abbr = ''
        x_country_full = ''
        x_country_iso = ''
        g_lat = None
        g_long = None
        return (x_city_name, x_county, x_state_name, x_state_abbr, x_country_full, x_country_iso,
                g_lat, g_long)
        pass

# setup the empty lists to store the queried data
g_city = []
g_county = []
g_state = []
g_state_abbr = []
g_country = []
g_country_iso = []
g_lat = []
g_lng = []

# iterate through the unique cities / countries data frame and retrieve geo-location data
for index, row in unique_sort.iterrows():
    city = row['CityName']
    countryc = row['CountryCode']
    r_city, r_county, r_state, r_state_a, r_country, r_iso, r_lat, r_lng = geo_loc(city, countryc)
    g_city.append(r_city)
    g_county.append(r_county)
    g_state.append(r_state)
    g_state_abbr.append(r_state_a)
    g_country.append(r_country)
    g_country_iso.append(r_iso)
    g_lat.append(r_lat)
    g_lng.append(r_lng)
    time.sleep(0.1)

# add the data to the data frame.
unique_sort['lat'] = g_lat
unique_sort['lng'] = g_lng
unique_sort['g_city'] = g_city
unique_sort['g_county'] = g_county
unique_sort['g_state'] = g_state
unique_sort['g_state_abbr'] = g_state_abbr
unique_sort['g_country'] = g_country
unique_sort['g_country_code'] = g_country_iso

# merge the geo-location data back to the original data frame.
unique_sort.to_csv('C:/Problem3_Data/GeoRaw.csv', sep='|', encoding='utf-8')
geo_merge = pd.merge(input_data, unique_sort, how='left', on=['CityName', 'CountryCode'])
geo_merge.to_csv('C:/Problem3_Data/GeoMerge.csv', sep='|', encoding='utf-8')

# there is a strict limit on the number of enquiries that can be returned with this get command
# from the google api and as such, would take either a license, or several (8 days) to
# fill in the data completely, as there are 17663 unique entries.

# place the country mapping data into a dataframe
country_map = pd.read_csv('C:/Problem3_Data/CountryMap.csv', sep='|', header=0,
                          index_col=0, engine='python')
# place the geo_data into a dataframe
geo_data = pd.read_csv('C:/Problem3_Data/GeoMerge.csv', sep='|', header=0,
                       index_col=0, engine='python', skipinitialspace=True)
# drop the count field as it is not needed.
geo_input = geo_data.drop('count', axis=1)
# merge the reference table data
final_output = pd.merge(geo_input, country_map, how='left', on='CountryCode')
# reorder the fields
final_output = final_output[['CityName', 'CountryCode', 'CountryName',
                             'g_city', 'g_county', 'g_state', 'g_state_abbr',
                             'g_country', 'g_country_code', 'lat', 'lng']]


# apply a match success evaluation field
def match_check(row):
    """Checks to see if the geoloc from Google's api matches the raw input"""
    if row['CountryCode'] == row['g_country_code']:
        match = 1
    else:
        match = 0
    return match

# create the match success check field and apply it to the data frame.
final_output['Match_Success'] = final_output.apply(match_check, axis=1)
# rename the fields
final_output.columns = ['Input_City', 'Input_CountryCode', 'Output_CountryName',
                        'g_CityName', 'g_County', 'g_State', 'g_State_Abbreviation',
                        'g_CountryName', 'g_CountryCode', 'g_latitude', 'g_longitude',
                        'g_Match_Success']
# verification of correct formatting
print(final_output[:10])
# save the output file
final_output.to_csv('C:/Problem3_Data/FinalOutput.csv', sep='|', header=0, index=False,
                    encoding='utf-8')


