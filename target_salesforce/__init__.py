#!/usr/bin/env python3
import json
import os

import singer
import singer.utils as singer_utils
from target_salesforce.salesforce import Salesforce
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

VALID_SOBJECTS = [
    "Tasks",
    "Contacts"
]

CONFIG = {
    "refresh_token": None,
    "client_id": None,
    "client_secret": None,
    "start_date": None,
}


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


def upload_target(client, payload_file, sobject):
    # Upload Tasks
    if os.path.exists(payload_file):
        fname = payload_file.split('/')[-1]
        LOGGER.info(f"Found {fname}, processing...")
        payload = load_json(payload_file)

    LOGGER.info(f"Uploading {len(payload)} task(s) to SalesForce")
    for item in payload:
        payload_str = json.dumps(item)
        LOGGER.debug(f"PAYLOAD: {payload_str}")
        client.create_record(sobject, payload_str)


def main():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    sf = sf_connect(CONFIG)

    payloads_files = glob(f"{CONFIG.get('input_path')}/*.json")
    for sobject in VALID_SOBJECTS:
        for payload in payloads_files:
            payload_name = payload.split('/')[-1][:-5]
            if payload_name.lower()==sobject.lower():
                upload_target(sf, payload, sobject)


if __name__ == "__main__":
    main()
