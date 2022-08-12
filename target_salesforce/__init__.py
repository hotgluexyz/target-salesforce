#!/usr/bin/env python3
import json
import sys
from glob import glob

import requests
import singer
import singer.utils as singer_utils
from singer import metadata

import target_salesforce
from target_salesforce.salesforce import Salesforce
from target_salesforce.salesforce.bulk import Bulk
from target_salesforce.salesforce.exceptions import \
    TapSalesforceBulkAPIDisabledException
from target_salesforce.salesforce.rest import Rest

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

FORCED_FULL_TABLE = {
    'BackgroundOperationResult', # Does not support ordering by CreatedDate
    'LoginEvent', # Does not support ordering by CreatedDate
}


def get_replication_key(sobject_name, fields):
    if sobject_name in FORCED_FULL_TABLE:
        return None

    fields_list = [f['name'] for f in fields]

    if 'SystemModstamp' in fields_list:
        return 'SystemModstamp'
    elif 'LastModifiedDate' in fields_list:
        return 'LastModifiedDate'
    elif 'CreatedDate' in fields_list:
        return 'CreatedDate'
    elif 'LoginTime' in fields_list and sobject_name == 'LoginHistory':
        return 'LoginTime'
    return None


def create_property_schema(field, mdata):
    field_name = field['name']

    if field_name == "Id":
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'automatic')
    else:
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'available')

    property_schema, mdata = target_salesforce.salesforce.field_to_property_schema(field, mdata)

    return (property_schema, mdata)


def generate_schema(fields, sf, sobject_name, replication_key):
    unsupported_fields = set()
    mdata = metadata.new()
    properties = {}

    # Loop over the object's fields
    for f in fields:
        field_name = f['name']

        property_schema, mdata = create_property_schema(
            f, mdata)

        # Compound Address fields and geolocations cannot be queried by the Bulk API
        if f['type'] in ("address", "location") and sf.api_type == target_salesforce.salesforce.BULK_API_TYPE:
            unsupported_fields.add(
                (field_name, 'cannot query compound address fields or geolocations with bulk API'))

        # we haven't been able to observe any records with a json field, so we
        # are marking it as unavailable until we have an example to work with
        if f['type'] == "json":
            unsupported_fields.add(
                (field_name, 'do not currently support json fields - please contact support'))

        # Blacklisted fields are dependent on the api_type being used
        field_pair = (sobject_name, field_name)
        if field_pair in sf.get_blacklisted_fields():
            unsupported_fields.add(
                (field_name, sf.get_blacklisted_fields()[field_pair]))

        inclusion = metadata.get(
            mdata, ('properties', field_name), 'inclusion')

        if sf.select_fields_by_default and inclusion != 'unsupported':
            mdata = metadata.write(
                mdata, ('properties', field_name), 'selected-by-default', True)

        properties[field_name] = property_schema

    if replication_key:
        mdata = metadata.write(
            mdata, ('properties', replication_key), 'inclusion', 'automatic')

    # There are cases where compound fields are referenced by the associated
    # subfields but are not actually present in the field list
    field_name_set = {f['name'] for f in fields}
    filtered_unsupported_fields = [f for f in unsupported_fields if f[0] in field_name_set]
    missing_unsupported_field_names = [f[0] for f in unsupported_fields if f[0] not in field_name_set]

    if missing_unsupported_field_names:
        LOGGER.info("Ignoring the following unsupported fields for object %s as they are missing from the field list: %s",
                    sobject_name,
                    ', '.join(sorted(missing_unsupported_field_names)))

    if filtered_unsupported_fields:
        LOGGER.info("Not syncing the following unsupported fields for object %s: %s",
                    sobject_name,
                    ', '.join(sorted([k for k, _ in filtered_unsupported_fields])))

    # Any property added to unsupported_fields has metadata generated and
    # removed
    for prop, description in filtered_unsupported_fields:
        if metadata.get(mdata, ('properties', prop),
                        'selected-by-default'):
            metadata.delete(
                mdata, ('properties', prop), 'selected-by-default')

        mdata = metadata.write(
            mdata, ('properties', prop), 'unsupported-description', description)
        mdata = metadata.write(
            mdata, ('properties', prop), 'inclusion', 'unsupported')

    if replication_key:
        mdata = metadata.write(
            mdata, (), 'valid-replication-keys', [replication_key])
    else:
        mdata = metadata.write(
            mdata,
            (),
            'forced-replication-method',
            {
                'replication-method': 'FULL_TABLE',
                'reason': 'No replication keys found from the Salesforce API'})

    mdata = metadata.write(mdata, (), 'table-key-properties', ['Id'])

    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': properties
    }

    entry = {
        'stream': sobject_name,
        'tap_stream_id': sobject_name,
        'schema': schema,
        'metadata': metadata.to_list(mdata)
    }

    return entry


# pylint: disable=too-many-branches,too-many-statements
def do_discover(sf):
    """Describes a Salesforce instance's objects and generates a JSON schema for each field."""
    global_description = sf.describe()

    objects_to_discover = {o['name'] for o in global_description['sobjects']}

    sf_custom_setting_objects = []
    object_to_tag_references = {}

    # For each SF Object describe it, loop its fields and build a schema
    entries = []

    # Check if the user has BULK API enabled
    if sf.api_type == 'BULK' and not Bulk(sf).has_permissions():
        raise TapSalesforceBulkAPIDisabledException('This client does not have Bulk API permissions, received "API_DISABLED_FOR_ORG" error code')

    for sobject_name in sorted(objects_to_discover):

        # Skip blacklisted SF objects depending on the api_type in use
        # ChangeEvent objects are not queryable via Bulk or REST (undocumented)
        if sobject_name in sf.get_blacklisted_objects() \
           or sobject_name.endswith("ChangeEvent"):
            continue

        sobject_description = sf.describe(sobject_name)

        # Cache customSetting and Tag objects to check for blacklisting after
        # all objects have been described
        if sobject_description.get("customSetting"):
            sf_custom_setting_objects.append(sobject_name)
        elif sobject_name.endswith("__Tag"):
            relationship_field = next(
                (f for f in sobject_description["fields"] if f.get("relationshipName") == "Item"),
                None)
            if relationship_field:
                # Map {"Object":"Object__Tag"}
                object_to_tag_references[relationship_field["referenceTo"]
                                         [0]] = sobject_name

        fields = sobject_description['fields']
        replication_key = get_replication_key(sobject_name, fields)

        # Salesforce Objects are skipped when they do not have an Id field
        if not [f["name"] for f in fields if f["name"]=="Id"]:
            LOGGER.info(
                "Skipping Salesforce Object %s, as it has no Id field",
                sobject_name)
            continue

        entry = generate_schema(fields, sf, sobject_name, replication_key)
        entries.append(entry)

    # For each custom setting field, remove its associated tag from entries
    # See Blacklisting.md for more information
    unsupported_tag_objects = [object_to_tag_references[f]
                               for f in sf_custom_setting_objects if f in object_to_tag_references]
    if unsupported_tag_objects:
        LOGGER.info( #pylint:disable=logging-not-lazy
            "Skipping the following Tag objects, Tags on Custom Settings Salesforce objects " +
            "are not supported by the Bulk API:")
        LOGGER.info(unsupported_tag_objects)
        entries = [e for e in entries if e['stream']
                   not in unsupported_tag_objects]

    result = {'streams': entries}
    json.dump(result, sys.stdout, indent=4)


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

    external_ids = [f["name"] for f in sobject['fields'] if f['externalId']]

    LOGGER.info(f"Uploading {len(payload)} {sobject['name']}(s) to SalesForce")
    for item in payload:
        external_id = [i for i in item.keys() if i in external_ids]
        if external_id:
            update_item = item.copy()
            update_item = generate_ids(client, update_item)
            rest = Rest(client)
            for eid in external_id:
                query = f"SELECT Id, {eid} FROM {sobject['name']} WHERE {eid} = '{update_item.get(eid)}' AND IsDeleted=false"
                params = {"q": query}
                url = "{}/services/data/v41.0/queryAll".format(client.instance_url)
                headers = client._get_standard_headers()
                res = next(rest._sync_records(url, headers, params), False)
                if res:
                    break
            if res:
                payload_str = json.dumps(item)
                endpoint = "/".join(res['attributes']['url'].split("/")[4:])
                res = client.update_record(endpoint, payload_str)
                if res.status_code==404:
                    LOGGER.warning(f"{sobject['name']} do not have a valid Salesforce Sobject name.")
                elif res.status_code==400:
                    LOGGER.warning(f"{payload} invalid payload: {res.json()[0].get('message')}")
                elif res.status_code>300:
                    LOGGER.warning(f"{payload} invalid payload.")
                continue
        payload_str = json.dumps(item)
        LOGGER.debug(f"PAYLOAD: {payload_str}")
        res = client.create_record(sobject["name"], payload_str)
        if res.status_code==404:
            LOGGER.warning(f"{sobject['name']} do not have a valid Salesforce Sobject name.")
        elif res.status_code==400:
            LOGGER.warning(f"{payload} invalid payload: {res.json()[0].get('message')}")
        elif res.status_code!=200:
            LOGGER.warning(f"{payload} invalid payload.")


def main():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    sf = sf_connect(CONFIG)

    if args.discover:
        do_discover(sf)
    else:
        payloads_files = glob(f"{CONFIG.get('input_path', '.')}/*.json")
        payloads_files = [f for f in payloads_files if "config.json" not in f]
        payloads_files = sort_files(payloads_files, PRIORITY_LIST)

        for payload in payloads_files:
            payload_name = payload.split('/')[-1][:-5]
            sobject = sf.describe(payload_name)
            if isinstance(sobject, dict) and sobject.get("label"):
                upload_target(sf, payload, sobject)

if __name__ == "__main__":
    main()
