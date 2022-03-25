# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import sys
import os
from os import system

# upgrade pandas and pytd client
# os.system(f"{sys.executable} -m pip install -U pandas==1.0.3")
# os.system(f"{sys.executable} -m pip install -U pytd==1.0.0")

import pandas as pd
import pytd
import pytd.pandas_td as td
import json
import requests


def main():
    # read in env vars
    TD_API_KEY = os.environ['TD_API_KEY']
    TD_API_SERVER = os.environ['TD_API_SERVER']
    PROFILES_DATABASE = os.environ['PROFILES_DATABASE']
    PROFILES_TABLE = os.environ['PROFILES_TABLE']
    ENRICHED_PROFILES_TABLE = os.environ['ENRICHED_PROFILES_TABLE']
    REACH_API_HOST = os.environ['REACH_API_HOST']
    REACH_API_OUTPUTS = list(map(lambda output: output.strip(), os.environ['REACH_API_OUTPUTS'].split(','))) \
        if 'REACH_API_OUTPUTS' in os.environ else []
    REACH_API_KEY = os.environ['REACH_API_KEY']
    REACH_API_NAME = os.environ['REACH_API_NAME']

    # creating schema mapping , customizable for client
    profile_schema = {'FNAME_COL': os.environ['SRC_FNAME_COL'], 'LNAME_COL': os.environ['SRC_LNAME_COL'],
                      'EMAIL_COL': os.environ['SRC_EMAIL_COL'], 'PHONE_COL': os.environ['SRC_PHONE_COL'],
                      'ADDR1_COL': os.environ['SRC_ADDR1_COL'], 'CITY_COL': os.environ['SRC_CITY_COL'],
                      'STATE_COL': os.environ['SRC_STATE_COL'], 'ZIP_COL': os.environ['SRC_ZIP_COL']}
    profile_api_map = {
        'first': os.environ['SRC_FNAME_COL'],
        'last': os.environ['SRC_FNAME_COL'],
        'email': os.environ['SRC_EMAIL_COL'],
        'phone': os.environ['SRC_PHONE_COL'],
        'address': os.environ['SRC_ADDR1_COL'],
        'city': os.environ['SRC_CITY_COL'],
        'state': os.environ['SRC_STATE_COL'],
        'zip': os.environ['SRC_ZIP_COL']
    }

    # initialise client and con
    con = td.connect(apikey=TD_API_KEY, endpoint=TD_API_SERVER)
    client = pytd.Client(apikey=TD_API_KEY, endpoint=TD_API_SERVER, database=PROFILES_DATABASE)
    offset = 0
    limit = 10

    while True:
        # now get all profiles for enrichment
        td_profiles = get_td_profiles(client, PROFILES_TABLE, profile_schema, limit=limit, offset=offset)

        if len(td_profiles['data']) < 1:
            break

        # initialise empty df
        enriched_data = pd.DataFrame()
        offset += limit

        for row in td_profiles['data']:
            # map profile column names to api names
            api_search_record = {}

            for api_name in profile_api_map:
                profile_key_name = profile_api_map[api_name]
                if profile_key_name in row:
                    api_search_record.update({api_name: row[profile_key_name]})

            result = reach_append(REACH_API_HOST, REACH_API_NAME, REACH_API_OUTPUTS, REACH_API_KEY, api_search_record)

            try:
                d = json.loads(result)['versium']['results'][0]
                df = pd.json_normalize(d)
                enriched_data = enriched_data.append(df)
            except:
                pass

        # send data back into treasure data table
        con.load_table_from_dataframe(enriched_data, '{0}.{1}'.format(PROFILES_DATABASE, ENRICHED_PROFILES_TABLE),
                                      writer='bulk_import', if_exists='append')


# Get contact data from treasure data master segment
def get_td_profiles(client, profiles_table, lookup_columns, limit=10000, offset=0):
    sql = "SELECT {0} FROM {1} OFFSET {2} LIMIT {3}".format(','.join(lookup_columns.values()), profiles_table,
                                                            offset, limit)
    print(sql)

    return client.query(query=sql)


def reach_append(host, api_name, api_outputs, api_key, search_record):
    url = "{0}{1}?".format(
        host, api_name)

    for output in api_outputs:
        url += "&output[]={0}".format(output)

    headers = {
        'Content-Type': 'application/json',
        'X-Accept': 'json',
        'x-versium-api-key': api_key
    }
    print(url)
    print(search_record)
    response = requests.request("POST", url, data=search_record, headers=headers)
    print(response.text)
    return response.text


# Run main
if __name__ == '__main__':
    main()
