###############################################################################
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2025 Tom Kralidis
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

from glob import glob
import json
import logging
import sys

from jsondiff import diff
from pywcmp.wcmp2.ets import WMOCoreMetadataProfileTestSuite2

LOGGER = logging.getLogger(__name__)


def prepare_record(data: dict) -> dict:
    """
    Prepare a record for parity checking

    :param record: `dict` of record

    :returns: `dict` of prepared record
    """

    keep_links = []

    LOGGER.debug('Pruning generated_by')
    _ = data.pop('generated_by', None)

    LOGGER.debug('Pruning wmo:topicHierarchy')
    _ = data['properties'].pop('wmo:topicHierarchy', None)

    LOGGER.debug('Pruning wmo:topicHierarchy')
    _ = data['properties'].pop('centre-id', None)

    LOGGER.debug('Pruning links')
    for link in data['links']:
        if link.get('rel', '') == 'license':
            keep_links.append(link)
#        elif link.get('href').startswith('mqtt'):
#            keep_links.append(link)
        else:
            LOGGER.debug('Removing link {link}')
    data['links'] = keep_links

    return data


if len(sys.argv) < 2:
    print(f'Usage: {sys.argv[0]} <centre-id>')
    sys.exit(1)

centre_id = sys.argv[1]
iut = None
other_gdcs = {}

for gdc in glob('*-global-discovery-catalogue'):
    if gdc.startswith(centre_id):
        iut = centre_id
    else:
        other_gdcs[gdc] = {}
        for wcmp2 in glob(f'{gdc}/*.json'):
            with open(wcmp2) as fh:
                data = json.load(fh)
                other_gdcs[gdc][data['id']] = data

if iut is None or not other_gdcs:
    msg = 'Nothing to compare!'
    LOGGER.warning(msg)

print(f'IUT: {iut}')
print(f'Other GDCs: {other_gdcs}')

for wcmp2 in glob(f'{iut}/*.json'):
    LOGGER.info(f'Checking {wcmp2}')
    with open(wcmp2) as fh:
        data = prepare_record(json.load(fh))
        try:
            ts = WMOCoreMetadataProfileTestSuite2(data)
            ts.run_tests()
        except Exception as err:
            print(f'ERROR on {wcmp2}: {err}')
            continue

        for key in other_gdcs.keys():
            if data['id'] not in other_gdcs[key]:
                print(f'ERROR: NOT in {key}')
                continue

            data2 = prepare_record(other_gdcs[key][data['id']])

            diff_ = diff(data, data2)
            if diff_:
                print(diff_)
