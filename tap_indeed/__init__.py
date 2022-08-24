#!/usr/bin/env python3
import os
import json
import backoff
import requests

import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform

REQUIRED_CONFIG_KEYS = ["start_date", "client_id", "client_secret"]
LOGGER = singer.get_logger()
URLS = {
    "get_token": "https://apis.indeed.com/oauth/v2/tokens",
    "list_employers": "https://secure.indeed.com/v2/api/appinfo",
    "ads": "https://apis.indeed.com/ads/v1/stats",
    "list_campaigns": "https://apis.indeed.com/ads/v1/campaigns",
    "campaigns": "https://apis.indeed.com/ads/v1/campaigns/{campaign_id}/stats"
}


class IndeedError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


def get_token(config, employer_id):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    data = {
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret']
    }

    if employer_id:
        data["scope"] = 'employer.advertising.campaign_report.read employer.advertising.campaign.read'
        data["employer"] = employer_id
    else:
        data["scope"] = 'employer_access'

    url = URLS["get_token"]
    response = requests.post(url, headers=headers, data=data)
    return response.json()


def refresh_access_token_if_expired(config, employer_id):
    # if [expires_at not exist] or if [exist and less than current time] then it will update the token
    if config["employers"][employer_id].get('expires_at') is None or \
            config["employers"][employer_id].get('expires_at') < datetime.utcnow():
        res = get_token(config, employer_id)
        config["employers"][employer_id]['access_token'] = res["access_token"]
        config["employers"][employer_id]["expires_at"] = datetime.utcnow() + timedelta(seconds=res["expires_in"])
        return True
    return False


def get_key_properties(stream_id):
    keys = {
        "ads": ["Ad_ID", "Date"],
        "campaigns": ["Id", "Date"],
    }
    return keys[stream_id]


def get_bookmark(stream_id):
    bookmarks = {
        "ads": "Date",
        "campaigns": "Date"
    }
    return bookmarks[stream_id]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def create_metadata_for_report(stream_id, schema, key_properties):
    replication_key = get_bookmark(stream_id)
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available",
                                             "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": [replication_key]
                                             }}]

    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    for key in schema.properties:
        # hence, when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type and schema.properties.get(key).properties:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop], "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).properties])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(stream_id, schema, get_key_properties(stream_id))
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


@backoff.on_exception(backoff.expo, IndeedError, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def request_data(url, request_type, headers, payload, config, employer_id=None):
    if request_type == 'POST':
        response = requests.post(url, headers=headers, data=payload)
    else:
        response = requests.get(url, headers=headers, params=payload)

    if response.status_code == 429:
        raise IndeedError(response.text)
    elif response.status_code == 401:
        # If token expired, it'll fetch new token and retry again.
        refresh_access_token_if_expired(config, employer_id)
        headers.update({'Authorization': f'Bearer {config["employers"][employer_id]["access_token"]}'})
        raise IndeedError("Token Expired, Refreshing...")
    elif response.status_code not in [200, 202]:
        raise Exception(response.text)
    return response


def get_paginated_data(url, data_key, headers, params, config, employer_id):
    """
    Retrieve data in pagination if next page url is available in response.
    """

    all_items = []
    next_page_url = url
    while True:
        response = request_data(
            next_page_url,
            "GET",
            headers,
            payload=params,
            config=config,
            employer_id=employer_id
        ).json()

        all_items += response["data"][data_key]
        next_page_location = [link["href"].split("?")[1] for link in response["meta"]["links"] if link["rel"] == "next"]

        if not next_page_location:
            break
        next_page_url = url + "?" + next_page_location[0]
    return all_items


def get_list_campaign_ids(config, employer_id):
    """
    List all campaigns under given employer account
    """

    headers = {'Authorization': 'Bearer ' + config["employers"][employer_id]['access_token']}
    params = {
        'perPage': 1000
    }

    all_campaigns = get_paginated_data(
        url=URLS["list_campaigns"],
        data_key="Campaigns",
        headers=headers,
        params=params,
        config=config,
        employer_id=employer_id
    )

    return [c["Id"] for c in all_campaigns]


def datetime_to_str(dt):
    return dt.strftime('%Y-%m-%d')


def get_next_date(_date: str):
    return datetime_to_str(datetime.strptime(_date, '%Y-%m-%d') + timedelta(days=1))


def sync_ads(config, state, stream, employer_id):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    # {bookmarks: {stream_id: {<employer_id>: <date>}}}
    start_date = singer.get_bookmark(state, stream.tap_stream_id, employer_id, config["start_date"])

    bookmark = start_date
    stats = pd.DataFrame()
    today = datetime_to_str(datetime.utcnow().date())

    headers = {'Authorization': 'Bearer ' + config["employers"][employer_id]['access_token']}
    while True:
        next_date = get_next_date(bookmark)
        params = {
            'startDate': bookmark,
            'endDate': next_date
        }
        LOGGER.info("Querying Ads States, employer_id: %s,  Date: %s", employer_id, bookmark)

        # Pagination is not supported for the ads states
        tmp = request_data(URLS[stream.tap_stream_id], "GET", headers, payload=params, config=config, employer_id=employer_id).json()
        data_url = tmp['meta']['rootLocation'] + tmp['data']['location']

        tap_data = request_data(data_url, "GET", headers, payload=params, config=config, employer_id=employer_id)
        tap_data = pd.read_csv(StringIO(tap_data.text))

        if len(tap_data) != 0:
            tap_data = pd.merge(
                tap_data,
                tap_data['Spend (currency)'].str.split(' ', expand=True).rename(columns={0: 'Spend', 1: 'Currency'}),
                left_index=True,
                right_index=True
            )

            tap_data['Spend'] = pd.to_numeric(tap_data['Spend'])
            tap_data['Date'] = bookmark
            tap_data['CompanyName'] = config["employers"][employer_id]['name']

            stats = pd.concat([
                stats,
                tap_data]
            )
            records = stats.reset_index(drop=True)
            records.columns = records.columns.str.replace(" ", "_")
            records = records.to_dict("records")

            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    transformed_data = transform(row, schema, metadata=mdata)

                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()

            # if there is data, then only we will print state
            if records:
                state = singer.write_bookmark(state, stream.tap_stream_id, employer_id, bookmark)
                singer.write_state(state)

        if bookmark <= today:
            bookmark = next_date
        if bookmark > today:
            break


def sync_campaigns(config, state, stream, employer_id):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    today = datetime_to_str(datetime.utcnow().date())

    # {bookmarks: {stream_id: {<employer_id>: <date>}}}
    start_date = singer.get_bookmark(state, stream.tap_stream_id, employer_id, config["start_date"])
    headers = {'Authorization': 'Bearer ' + config["employers"][employer_id]['access_token']}
    for cid in get_list_campaign_ids(config, employer_id):
        bookmark = start_date
        while True:
            next_date = get_next_date(bookmark)
            params = {
                'startDate': bookmark,
                'endDate': next_date,
                'perPage': 1000
            }
            LOGGER.info("Querying Campaign States, employer_id: %s, Campaign_Id: %s, Date: %s", employer_id, cid, bookmark)
            all_states = get_paginated_data(
                url=URLS[stream.tap_stream_id].format(campaign_id=cid),
                data_key="entries",
                headers=headers,
                params=params,
                config=config,
                employer_id=employer_id
            )

            records = pd.DataFrame(all_states)
            records.columns = records.columns.str.replace(" ", "_")
            records = records.to_dict("records")
            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    transformed_data = transform(row, schema, metadata=mdata)

                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()

            if bookmark <= today:
                bookmark = next_date
            if bookmark > today:
                break

    # if there is data, then only we will print state
    state = singer.write_bookmark(state, stream.tap_stream_id, employer_id, today)
    singer.write_state(state)


def sync(config, state, catalog):
    # Get access token to list all employer_ids(employers)
    res = get_token(config, employer_id=None)
    headers = {'Authorization': f'Bearer {res["access_token"]}'}
    employers = request_data(URLS["list_employers"], "POST", headers=headers, payload={}, config=config)

    # get access token for individual employer_ids
    config["employers"] = {}
    for emp in employers.json()['employers']:
        res = get_token(config, employer_id=emp['id'])
        config["employers"][emp['id']] = {
            'name': emp['name'],
            'access_token': res['access_token'],
            "expires_at": datetime.utcnow() + timedelta(seconds=res["expires_in"])
        }

    # Loop over all employer accounts
    for emp in employers.json()['employers']:
        # Loop over selected streams in catalog
        for stream in catalog.get_selected_streams(state):
            LOGGER.info("Syncing stream:" + stream.tap_stream_id)

            if stream.tap_stream_id == "ads":
                sync_ads(config, state, stream, employer_id=emp["id"])
            elif stream.tap_stream_id == "campaigns":
                sync_campaigns(config, state, stream, employer_id=emp["id"])
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()

