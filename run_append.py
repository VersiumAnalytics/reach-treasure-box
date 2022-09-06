# Import builtins
import asyncio
import logging
import os
import subprocess
import sys
import time
import warnings
import pkg_resources
import urllib

from collections import namedtuple

# ********************************PACKAGE SETUP***************************************

__requires__ = ["pandas==1.4.2", "pytd>=1.4.0", "aiohttp==3.8.1"]

# Setup logging and warnings
log_level = os.environ.get("LOG_LEVEL", "ERROR").upper()
if log_level == "DEBUG":
    formatter = '[%(asctime)s - %(name)10s - %(filename)15s:%(lineno)5s - %(funcName)15s() - %(levelname)8s] - %(message)s'
else:
    formatter = None

if log_level == "OFF":
    log_level = logging.CRITICAL + 1
else:
    log_level = getattr(logging, log_level)
logging.basicConfig(level=log_level, format=formatter)
logger = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)


# For treasure box, we need to install packages here if they are not already installed. Then import them.
# This breaks Python style guidelines but is the best solution for a custom Python script in a Treasure Data workflow.
def install(dist):
    logger.info(f"Requirement {dist} not found in the current environment. Installing {dist}.")
    python = sys.executable
    # Setting stdout to 2 redirects stdout to stderr.
    subprocess.check_call([python, '-m', 'pip', 'install', '-U', str(dist)], stdout=2)

    # Need to create a new environment to scan for the newly installed package
    env = pkg_resources.Environment()
    # Get the different package distributions (versions) found on the system path return the first (newest)
    if env[dist.key]:
        return env[dist.key][0]

    return None


try:
    requirements = pkg_resources.parse_requirements(__requires__)
    pkg_resources.working_set.resolve(requirements, installer=install, replace_conflicting=True)
except pkg_resources.VersionConflict as e:
    logger.error(f"Could not install required distribution `{e.req}` because it conflicts with the currently installed distribution `{e.dist}`.")
    raise

# ************************************************************************************


import aiohttp
import pandas as pd
import pytd
import pytd.pandas_td as td

QueryResult = namedtuple("QueryResult", ["result", "success", "match"], defaults=(None, False, False))

DEFAULT_RESPONSE = QueryResult(None, False, False)
QUERIES_PER_SECOND_HARD_CAP = 100
BATCH_SIZE = 10000


# Create unique exception type for query related errors
class QueryError(RuntimeError):
    pass


class RateLimiter(object):
    """ Limits the number of calls to a function within a timeframe. Also limits the number of total active function calls.

        Parameters
        ----------
        max_calls : int
            Maximum number of function calls to make within a time period.
        period : float
            Length of time period in seconds.
        n_connections : int
            Maximum number of total active function calls to allow. Once this limit is reached, no more function calls will be made until an
            active call returns.
        n_retry : int
            Number of times to retry a function call until it succeeds.
        retry_wait_time : float
            Number of seconds to wait between retries. The wait time will increase by a factor of `retry_wait_time` everytime the call
            fails. For example, if `retry_wait_time`=3, then it will wait 0 seconds on the first retry, 3 seconds on the second retry,
            6 seconds on the third retry, etc.
    """

    def __init__(self, max_calls=20, period=1, n_connections=100, n_retry=3, retry_wait_time=3):
        if (max_calls // period) > QUERIES_PER_SECOND_HARD_CAP:
            raise ValueError(f"The maximum queries per second allowed is {QUERIES_PER_SECOND_HARD_CAP}. Instead got "
                             f"{max_calls / period:.2f} queries per second.")

        self.max_calls = max_calls
        self.period = period
        self.n_retry = n_retry
        self.retry_wait_time = retry_wait_time

        self.clock = time.monotonic
        self.last_reset = 0
        self.num_calls = 0
        self.sem = asyncio.Semaphore(n_connections)

    def __call__(self, func):

        async def wrapper(*args, **kwargs):
            # Semaphore will block more than {self.n_connections} from happening at once.
            self.last_reset = self.clock()
            async with self.sem:
                for i in range(self.n_retry + 1):
                    while self.num_calls >= self.max_calls:
                        await asyncio.sleep(self.__period_remaining())

                    try:
                        self.num_calls += 1
                        result = await func(*args, attempts_left=self.n_retry - i, **kwargs)
                        return result

                    # Doesn't matter what the exception is, we will still try again
                    except QueryError:
                        if self.n_retry - i > 0:
                            await asyncio.sleep(self.retry_wait_time * i)
                # Failed to perform the query after n_retry attempts. Return empty response
                return DEFAULT_RESPONSE

        return wrapper

    def __period_remaining(self):
        elapsed = self.clock() - self.last_reset
        period_remaining = self.period - elapsed
        if period_remaining <= 0:
            self.num_calls = 0
            self.last_reset = self.clock()
            period_remaining = self.period
        return period_remaining


def create_profile_api_map(id_column):
    api_environ_map = {"first": "PROFILE_FIRST_COL", "last": "PROFILE_LAST_COL",
                       "email": "PROFILE_EMAIL_COL", "phone": "PROFILE_PHONE_COL",
                       "address": "PROFILE_ADDRESS_COL", "city": "PROFILE_CITY_COL",
                       "state": "PROFILE_STATE_COL", "zip": "PROFILE_ZIP_COL", "country": "PROFILE_COUNTRY_COL",
                       "business": "PROFILE_BUSINESS_COL", "domain": "PROFILE_DOMAIN_COL", "ip": "PROFILE_IP_COL"}
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
            .format(",".join(lookup_columns), profiles_table, id_column, id_column, enriched_table, offset, limit)
    else:
        sql = "SELECT {0} FROM {1} OFFSET {2} LIMIT {3}".format(",".join(lookup_columns), profiles_table, offset, limit)
    logger.debug(f"Querying td client with query: {sql}")
    return client.query(query=sql)


async def _fetch(session, url, record, params, headers=None, attempts_left=3):
    """Perform the api append for a single record.
    """
    params.update(record)

    try:

        async with session.post(url, params=params, headers=headers) as response:
            response.raise_for_status()  # raise error if bad http status
            body = await response.json(content_type=None)  # get json representation of response
            body = body["versium"]
            if "errors" in body:
                raise QueryError(f"Received error response from REACH API: {body['errors']}")
            elif body["results"]:
                result = QueryResult({f"Versium {key}": val for key, val in body["results"][0].items()},
                                     success=True,
                                     match=True)
            else:
                logger.debug(f"API call successful but there were no matches for record:\n\t{record}")
                result = QueryResult(result={}, success=True, match=False)

            return result

    except Exception as e:
        if isinstance(e, aiohttp.ClientResponseError):  # Log some extra info if it's a ClientResponseError
            logger.error(f"Error during url fetch: {e.message}\n\tURL: {e.request_info.real_url}"
                         f"\n\tResponse Status: {e.status}\n\tHeaders: {e.headers}\n\tAttempts Left: {attempts_left:d}")
        else:
            status = getattr(response, "status", "UNKNOWN")
            logger.error(f"Error during url fetch: {e}\n\tURL: {url}?{urllib.parse.urlencode(params)}"
                         f"\n\tResponse Status: {status}\n\tAttempts Left: {attempts_left:d}")
        # Use a different error class so that we only catch errors from making the http request
        raise QueryError("Failed to fetch url.")


async def _create_tasks(url, records, *, params=None, headers=None, read_timeout=15, queries_per_second=20, n_connections=100,
                        n_retry=3, retry_wait_time=3):
    """Setup rate limiting and client sessions to asynchronously call the api for each record
    """
    if params is None:
        params = {}
    
    tasks = []
    limit = RateLimiter(queries_per_second, 1, n_connections, n_retry=n_retry, retry_wait_time=retry_wait_time)
    # Wrap our fetch call with the limiter to ensure we don't exceed number of connections or queries per second.
    limited_fetch = limit(_fetch)

    # Reuses connections instead of closing them out.
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=n_connections, limit_per_host=n_connections),
                                     read_timeout=read_timeout) as session:
        for record in records:
            task = asyncio.ensure_future(
                limited_fetch(session, url=url, record=record, params=params, headers=headers))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        return await responses


def reach_append(host, api_name, api_outputs, api_key, search_records, **kwargs):
    """Query REACH API for enrichment data
    """

    url = host + api_name
    
    # Filter out empty outputs
    api_outputs = [out for out in api_outputs if out]
    params = {"output[]": api_outputs, 'rcfg_include_liveramp': 1}
    
    headers = {
        "Content-Type": "application/json",
        "X-Accept": "json",
        "x-versium-api-key": "<omitted>"
    }
    
    logger.debug(f"Started querying {len(search_records)} records.\n"
                 f"Request headers are: {headers}.\n"
                 f"Request params are: {params}.\n"
                 f"Additional arguments: {kwargs}")

    # Assigned here to avoid logging the real api key.
    headers["x-versium-api-key"] = api_key

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(_create_tasks(url, search_records, params=params, headers=headers, **kwargs))
    try:
        responses = loop.run_until_complete(future)

    except KeyboardInterrupt:
        # Canceling pending tasks and stopping the loop.
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
        loop.stop()

        raise KeyboardInterrupt
    
    return responses


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

    n_retry = int(os.environ.get("NUM_RETRY", 3))
    read_timeout = int(os.environ.get("TIMEOUT", 20))
    queries_per_second = int(os.environ.get("QPS", 20))

    # creating schema mapping , customizable for client
    api_search_names, td_profile_columns = create_profile_api_map(profile_id_column)

    # initialise client and con
    con = td.connect(apikey=td_api_key, endpoint=td_api_server)
    client = pytd.Client(apikey=td_api_key, endpoint=td_api_server, database=profiles_database)
    offset = 0
    limit = BATCH_SIZE
    batch = 0

    while True:
        # now get all profiles for enrichment
        batch += 1
        td_profiles = get_td_profiles(client, profiles_table, enriched_profiles_table, td_profile_columns,
                                      profile_id_column, limit, offset)

        if not td_profiles['data']:
            break

        # initialise empty df
        offset += limit
        rec_ctr = 0

        api_search_records = []
        for row in td_profiles['data']:
            # map td profile record data to REACH API search params
            rec = {api_name: row[idx] for idx, api_name in enumerate(api_search_names) if row[idx]}
            api_search_records.append(rec)
            rec_ctr += 1

        try:
            logger.info(f"Querying REACH API with {rec_ctr} records; batch: {batch}")

            start_time = time.monotonic()
            responses = reach_append(reach_api_host, reach_api_name, reach_api_outputs, reach_api_key, api_search_records, n_retry=n_retry,
                                     queries_per_second=queries_per_second, read_timeout=read_timeout)
            end_time = time.monotonic()

            results, successes, matches = zip(*responses)

            output_df = pd.DataFrame(td_profiles['data'], columns=td_profile_columns)

            append_df = pd.DataFrame(results)

            # Calculate success rate, match rate, and queries per second
            num_queries = len(responses)
            success_rate = sum(successes) / num_queries
            match_rate = sum(matches) / num_queries

            num_seconds = end_time - start_time
            effective_qps = num_queries / num_seconds
            logger.info(f"Batch {batch} Results:\n"
                        f"\tQueried {num_queries} records in {num_seconds:.1f} seconds for an average of "
                        f"{effective_qps:.2f} queries per second.\n"
                        f"\tMatch Rate: {match_rate:.2%}.\n"
                        f"\tRequest Success Rate: {success_rate:.2%}.")

            output_df = output_df.join(append_df, rsuffix="_append")

            # send data back into treasure data table
            con.load_table_from_dataframe(output_df, f"{profiles_database}.{enriched_profiles_table}", writer='bulk_import',
                                          if_exists='append')

        except Exception as e:
            logger.error(f"An error occurred during the append process in batch {batch}: {e}")


# Run main
if __name__ == '__main__':
    main()
