import sys
import os
from os import system

# upgrade pandas and pytd client
# os.system(f"{sys.executable} -m pip install -U pandas==1.4.2")
# os.system(f"{sys.executable} -m pip install -U pytd==1.4.3")

import pandas as pd
import pytd
import pytd.pandas_td as td
import json
import requests


def main():
    # read in env vars
    td_api_key = os.environ['TD_API_KEY']
    td_api_server = os.environ['TD_API_SERVER']
    profiles_database = os.environ['PROFILES_DATABASE']
    profiles_table = os.environ['PROFILES_TABLE']
    enriched_profiles_table = os.environ['ENRICHED_PROFILES_TABLE']
    profile_id_column = os.environ['PROFILE_ID_COL'] if 'PROFILE_ID_COL' in os.environ \
                                                            and os.environ['PROFILE_ID_COL'] else ''
    reach_api_host = os.environ['REACH_API_HOST']
    reach_api_outputs = [output.strip() for output in os.environ.get('REACH_API_OUTPUTS', '').split(',')]
    reach_api_key = os.environ['REACH_API_KEY']
    reach_api_name = os.environ['REACH_API_NAME']

    # creating schema mapping , customizable for client
    api_search_names, td_profile_columns = create_profile_api_map(profile_id_column)

    # initialise client and con
    con = td.connect(apikey=td_api_key, endpoint=td_api_server)
    client = pytd.Client(apikey=td_api_key, endpoint=td_api_server, database=profiles_database)
    offset = 0
    limit = 10000
    batch = 0


    while True:
        # now get all profiles for enrichment
        batch += 1
        td_profiles = get_td_profiles(client, profiles_table, enriched_profiles_table, td_profile_columns,
                                      profile_id_column, limit, offset)

        if not td_profiles['data']:
            break

        # initialise empty df
        enriched_data = pd.DataFrame()
        offset += limit
        rec_ctr = 0

        for row in td_profiles['data']:
            # map td profile record data to REACH API search params
            api_search_record = {api_name: row[idx] for idx, api_name in enumerate(api_search_names) if row[idx]}
            rec_ctr += 1

            # combine td profile data with td profile column names into a dictionary, then place in dataframe
            output_df = pd.json_normalize(dict(zip(td_profile_columns, row)))

            try:
                print('Querying REACH API with record: {0}, batch: {1}'.format(rec_ctr, batch))
                response = reach_append(reach_api_host, reach_api_name, reach_api_outputs, reach_api_key,
                                        api_search_record)
                body = json.loads(response)['versium']

                if 'errors' in body:
                    print("Received error response from REACH API: {0}".format(body['errors']))
                elif len(body['results']) > 0:
                    # prefix keys with versium_ to avoid name collisions
                    versium_result = {f"Versium {key}": val for key, val in body['results'][0].items()}
                    append_df = pd.json_normalize(versium_result)
                    output_df = output_df.join(append_df)
            except Exception as e:
                print("An error occurred during the append process: {0}".format(e))
                pass

            enriched_data = enriched_data.append(output_df)

        # send data back into treasure data table
        con.load_table_from_dataframe(enriched_data, '{0}.{1}'.format(profiles_database, enriched_profiles_table),
                                      writer='bulk_import', if_exists='append')


def create_profile_api_map(id_column):
    api_environ_map = {'first': 'PROFILE_FIRST_COL', 'last': 'PROFILE_LAST_COL',
                       'email': 'PROFILE_EMAIL_COL', 'phone': 'PROFILE_PHONE_COL',
                       'address': 'PROFILE_ADDRESS_COL', 'city': 'PROFILE_CITY_COL',
                       'state': 'PROFILE_STATE_COL', 'zip': 'PROFILE_ZIP_COL', 'country': 'PROFILE_COUNTRY_COL',
                       'business': 'PROFILE_BUSINESS_COL', 'domain': 'PROFILE_DOMAIN_COL', 'ip': 'PROFILE_IP_COL'}
    api_names = []
    profile_columns = []

    for api_name, profile_name in api_environ_map.items():
        if os.environ.get(profile_name, None):
            api_names.append(api_name)
            profile_columns.append(os.environ[profile_name])

    # it's possible that PROFILE_ID_COL is also one of the api columns, so check if it has already been added
    if id_column not in profile_columns and id_column in os.environ and os.environ[id_column]:
        profile_columns.append(os.environ[id_column])

    return api_names, profile_columns


# Get contact data from treasure data master segment
def get_td_profiles(client, profiles_table, enriched_table, lookup_columns, id_column, limit=10000, offset=0):
    # if id_column provided, exclude records that have already been enriched
    if id_column:
        sql = "SELECT {0} FROM {1} WHERE {2} NOT IN (SELECT {3} FROM {4}) OFFSET {5} LIMIT {6}" \
            .format(','.join(lookup_columns), profiles_table, id_column, id_column, enriched_table, offset, limit)
    else:
        sql = "SELECT {0} FROM {1} OFFSET {2} LIMIT {3}".format(','.join(lookup_columns), profiles_table, offset, limit)

    return client.query(query=sql)


# Query REACH API for enrichment data
def reach_append(host, api_name, api_outputs, api_key, search_record):
    url = "{0}{1}?".format(
        host, api_name)

    for output in api_outputs:
        if output:
            url += "&output[]={0}".format(output)

    params = {**search_record, 'rcfg_include_liveramp': 1}
    headers = {
        'Content-Type': 'application/json',
        'X-Accept': 'json',
        'x-versium-api-key': api_key
    }
    response = requests.post(url, params=params, headers=headers)

    return response.text


# Run main
if __name__ == '__main__':
    main()
