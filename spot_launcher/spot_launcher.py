#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto.ec2
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping
import time
import copy
import argparse
import sys
import pprint
import os
import yaml

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_PATH, '../configs')

def launch_from_config(conn, instance_config_name, config_file_name):
    spot_requests_config = get_config(config_file_name)
    config = spot_requests_config[instance_config_name]
    mapping = create_mapping(config)

    print 'Launching %s instances'%(instance_config_name)
    print 'Instance parameters:'
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(config)

    spot_req = conn.request_spot_instances(
        config['price'],
        config['ami_id'],
        count=config['count'],
        type=config['type'],
        key_name=config['key_name'],
        instance_type=config['instance_type'],
        placement_group=config['placement_group'],
        security_group_ids=config['security_groups'],
        subnet_id=config['subnet_id'],
        instance_profile_name=config['instance_profile_name'],
        block_device_map=mapping
    )

    request_ids = [req.id for req in spot_req]
    print 'Waiting for fulfillment'
    instance_ids = wait_for_fulfillment(conn, request_ids, 
            copy.deepcopy(request_ids))

    if 'tags' in config:
        tag_instances(conn, instance_ids, config['tags'])

    return instance_ids

def get_config(config_file_name):
    config_file = open(os.path.join(CONFIG_PATH, config_file_name))
    config_dict = yaml.load(config_file.read())
    return config_dict

def create_mapping(config):
    if 'mapping' not in config:
        return None
    mapping = BlockDeviceMapping()
    for ephemeral_name, device_path in config['mapping'].iteritems():
        ephemeral = BlockDeviceType()
        ephemeral.ephemeral_name = ephemeral_name
        mapping[device_path] = ephemeral
    return mapping

def wait_for_fulfillment(conn, request_ids, pending_request_ids):
    """Loop through all pending request ids waiting for them to be fulfilled.
    If a request is fulfilled, remove it from pending_request_ids.
    If there are still pending requests, sleep and check again in 10 seconds.
    Only return when all spot requests have been fulfilled."""

    instance_ids = []
    failed_ids = []
    time.sleep(10)
    pending_statuses = set(['pending-evaluation', 'pending-fulfillment'])
    while len(pending_request_ids) > 0:
        results = conn.get_all_spot_instance_requests(
            request_ids=pending_request_ids)

        for result in results:
            if result.status.code == 'fulfilled':
                pending_request_ids.pop(pending_request_ids.index(result.id))
                print '\nspot request %s fulfilled!'%result.id
                instance_ids.append(result.instance_id)
            elif result.status.code not in pending_statuses:
                pending_request_ids.pop(pending_request_ids.index(result.id))
                print '\nspot request %s could not be fulfilled. ' \
                      'Status code: %s'%(result.id, result.status.code)
                failed_ids.append(result.id)

        if len(pending_request_ids) > 0:
            sys.stdout.write('.')
            sys.stdout.flush()
        time.sleep(10)
    
    if len(failed_ids) > 0:
        print 'The following spot requests ' \
              'have failed: %s'%(', '.join(failed_ids))
    else:
        print 'All spot requests fulfilled!'

    return instance_ids

def tag_instances(conn, instance_ids, tags):
    instances = conn.get_only_instances(instance_ids=instance_ids)
    for instance in instances:
        for key, value in tags.iteritems():
            instance.add_tag(key=key, value=value)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('instance', type=str, 
        help='Instance config name to launch')
    parser.add_argument('-r', '--region', type=str, default='us-east-1',
        help='EC2 region name')
    parser.add_argument('-c', '--config-file', type=str, default='spot_requests.yml',
        help='Spot requests config file name')
    args = parser.parse_args()

    conn = boto.ec2.connect_to_region(args.region)
    config_file_name = args.config_file
    instance_config_name = args.instance
    launch_from_config(conn, instance_config_name, config_file_name)

if __name__ == '__main__':
    main()
