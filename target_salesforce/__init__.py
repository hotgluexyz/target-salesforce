#!/usr/bin/env python3
import json
import requests

import singer
import singer.utils as singer_utils
from target_salesforce.salesforce import Salesforce
from target_salesforce.salesforce.rest import Rest
from glob import glob

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "refresh_token",
    "client_id",
    "client_secret",
    "start_date",
    "api_type",
    "select_fields_by_default",
]

CONFIG = {
    "refresh_token": None,
    "client_id": None,
    "client_secret": None,
    "start_date": None,
}

PRIORITY_LIST = ["Account", "Contact"]

def load_json(path):
    with open(path) as f:
        return json.load(f)


def sf_connect(CONFIG):
    # Authenticate into Salesforce API
    sf = None
    try:
        sf = Salesforce(
            refresh_token=CONFIG["refresh_token"],
            sf_client_id=CONFIG["client_id"],
            sf_client_secret=CONFIG["client_secret"],
            quota_percent_total=CONFIG.get("quota_percent_total"),
            quota_percent_per_run=CONFIG.get("quota_percent_per_run"),
            is_sandbox=CONFIG.get("is_sandbox"),
            select_fields_by_default=CONFIG.get("select_fields_by_default"),
            default_start_date=CONFIG.get("start_date"),
            api_type=CONFIG.get("api_type"),
        )
        sf.login()
    finally:
        if sf:
            if sf.rest_requests_attempted > 0:
                LOGGER.debug(
                    "This job used %s REST requests towards the Salesforce quota.",
                    sf.rest_requests_attempted,
                )
            if sf.jobs_completed > 0:
                LOGGER.debug(
                    "Replication used %s Bulk API jobs towards the Salesforce quota.",
                    sf.jobs_completed,
                )
            if sf.login_timer:
                sf.login_timer.cancel()
    return sf


def sort_files(payloads_files, priority_list):
    priority_files = []
    for priority in priority_list:
        file = [file for file in payloads_files if priority in file]
        if file:
            priority_files.append(payloads_files.pop(payloads_files.index(file[0])))
    
    regular_files = [f for f in payloads_files if not f in priority_files]
    payloads_files = priority_files + regular_files
    return payloads_files


def generate_ids(client, item):
    for k, v in item.items():
        if type(v)==dict:
            sobject = k.replace("Id", "")
            field, external_id = list(v.items())[0]
            query = f"SELECT Id FROM {sobject} WHERE {field} = '{external_id}'"
            params = {"q": query}
            url = "{}/services/data/v41.0/queryAll".format(client.instance_url)
            headers = client._get_standard_headers()
            rest = Rest(client)
            res = next(rest._sync_records(url, headers, params))
            item[k] = res["Id"]
    return item


def upload_target(client, payload_file, sobject):
    # Upload Payloads
    fname = payload_file.split('/')[-1]
    LOGGER.info(f"Found {fname}, processing...")
    payload = load_json(payload_file)

    external_id = [f["name"] for f in sobject['fields'] if f['externalId']]
    external_id = external_id[0] if external_id else None

    LOGGER.info(f"Uploading {len(payload)} {sobject['name']}(s) to SalesForce")
    for item in payload:
        if external_id in item.keys():
            item = generate_ids(client, item)
            rest = Rest(client)
            query = f"SELECT Id, {external_id} FROM {sobject['name']} WHERE {external_id} = '{item.get(external_id)}' AND IsDeleted=false"
            params = {"q": query}
            url = "{}/services/data/v41.0/queryAll".format(client.instance_url)
            headers = client._get_standard_headers()
            res = next(rest._sync_records(url, headers, params), False)
            if res:
                payload_str = json.dumps(item)
                endpoint = "/".join(res['attributes']['url'].split("/")[4:])
                client.update_record(endpoint, payload_str)
                continue
        payload_str = json.dumps(item)
        LOGGER.debug(f"PAYLOAD: {payload_str}")
        client.create_record(sobject["name"], payload_str)


def main():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    sf = sf_connect(CONFIG)

    payloads_files = glob(f"{CONFIG.get('input_path', '.')}/*.json")
    payloads_files = [f for f in payloads_files if "config.json" not in f]
    payloads_files = sort_files(payloads_files, PRIORITY_LIST)

    for payload in payloads_files:
        try:
            payload_name = payload.split('/')[-1][:-5]
            sobject = sf.describe(payload_name)
            upload_target(sf, payload, sobject)
        except requests.exceptions.HTTPError as e:
            if '404' in str(e)[:3]:
                LOGGER.warning(f"{payload} do not have a valid Salesforce Sobject name.")
            elif '400' in str(e)[:3]:
                LOGGER.warning(f"{payload} invalid payload.")
            else:
                raise e

if __name__ == "__main__":
    main()
