#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

import os
from dask.distributed import Client

# This script will be executed by the Pilot X pod running on the dask cluster. A standard example from the dask
# tutorial is included.
# Note: the following env vars are expected to be defined
# DASK_SCHEDULER_IP: the Dask scheduler IP (e.g. tcp://127.0.0.1:8786)
# DASK_SHARED_FILESYSTEM_PATH: path to a shared directory used for pod communication, e.g. /mnt/dask
# PANDA_ID: a PanDA job id, e.g. 123456789 (set it to e.g. this number for local testing)


# do not remove
def get_required_vars_dict():
    """
    Create a dictionary with variables that are required.

    The environment on the dask cluster expects the following env variables to be set
    1. DASK_SCHEDULER_IP: the Dask scheduler IP (e.g. tcp://127.0.0.1:8786)
    2. DASK_SHARED_FILESYSTEM_PATH: path to a shared directory used for pod communication, e.g. /mnt/dask
    3. PANDA_ID: a PanDA job id, e.g. 123456789 (set it to e.g. this number for local testing)

    The function returns a dictionary with the following format:
       required_vars = {
                        'host': $DASK_SCHEDULER_IP,
                        'shared_disk': $DASK_SHARED_FILESYSTEM_PATH,
                        'job_id': 'PANDA_ID'
                        }
    Keys and values will only be added if values are set.

    :return: required_vars (Dictionary)
    """

    required_vars = {}
    vars = {'host': 'DASK_SCHEDULER_IP', 'shared_disk': 'DASK_SHARED_FILESYSTEM_PATH', 'job_id': 'PANDA_ID'}
    for var in vars:
        _value = os.environ.get(vars[var], None)
        if _value:
            required_vars[var] = _value

    return required_vars


if __name__ == '__main__':

    # get the required variables
    required_vars = get_required_vars_dict()
    if not ('host' in required_vars and 'shared_disk' in required_vars and 'job_id' in required_vars):
        os.exit(-1)

    client = Client(required_vars.get('host'))
    if not client:
        print('failed to connect to dask scheduler - cannot continue')
        os.exit(-1)

    # add your code here ..............................................................

    # example code from dask tutorial .. Remove this part ..
    def square(x):
        return x ** 2

    def neg(x):
        return -x

    _amap = client.map(square, range(1000))
    _bmap = client.map(neg, _amap)
    total = client.submit(sum, _bmap)
    print(total.result())

    # end of user code ................................................................

    print('user dask script has finished')
    os.exit(0)
