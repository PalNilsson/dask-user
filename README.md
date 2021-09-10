# dask-user
Template for a user dask script.

This script will be executed by the Pilot X pod running on the dask cluster. A standard example from the dask
tutorial is included.

Note: the following environment variables are expected to be defined
1. DASK_SCHEDULER_IP: the Dask scheduler IP (e.g. tcp://127.0.0.1:8786)
2. DASK_SHARED_FILESYSTEM_PATH: path to a shared directory used for pod communication, e.g. /mnt/dask
3. PANDA_ID: a PanDA job id, e.g. 123456789 (set it to e.g. this number for local testing)

