# -*- coding: utf-8 -*-

"""Download images from a collection with a specific tag."""

from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''
USERNAME = ''
PASSWORD = ''

zc = ZegamiClient(username=USERNAME, password=PASSWORD)

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)

# fetch userdata
userdata = collection.userdata

# add new userdata
userdata = collection.set_userdata({
    'testString': 'string',
    'testNumber': 1
})

# replace userdata
userdata = collection.set_userdata({
    'testString': 'new string'
})

# remove userdata
userdata = collection.set_userdata({
    'testString': None
})

print(userdata)
