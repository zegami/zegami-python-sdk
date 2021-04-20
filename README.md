# Zegami Python SDK

![Zegami](zegami.png)

# zegami-python-sdk
An SDK and general wrapper for the lower level Zegami API for Python. This package provides higher level collection interaction and data retrieval.

# Getting started
Grab this repo, open the script, and load an instance of ZegamiClient into a variable.

```
from zegami_sdk.zegami_client import ZegamiClient

zc = ZegamiClient(username, login)
```

## Credentials
The client operates using a user token. By default, logging in once with a valid username/password will save the acquired token to your home directory as
`zegami.token`. The next time you need to use ZegamiClient, you may call `zc = ZegamiClient()` with no arguments, and it will look for this stored token.

## Example Usage
### Get the metadata and images associated with every dog of the 'beagle' breed in a collection of dogs:
```
zc = ZegamiClient()

# The client will automatically configure its active workspace to your first
# workspace. Change this to any other that you have access to simply by
# setting:
zc.active_workspace_id = '8-character-workspace-id'

# You can list all available workspaces using:
zc.list_workspaces()

# You can list the collections in your current active workspace using:
zc.list_collections()

# To use these results in code, pass the keyword argument and/or suppress
# the print-out:
workspaces = zc.list_workspaces(return_dictionaries=True, suppress_message=True)
collections = zc.list_collections(return_dictionaries=True, suppress_message=True)

# Grab the collection (a dictionary of collection information):
dogs_collection = zc.get_collection_by_name('My dogs collection')

# Grab all the data rows as a pandas.DataFrame:
dog_rows = zc.get_rows(dogs_collection)

# Grab the data rows of the dogs whose 'Breed' values = 'beagle', as a
# pandas.DataFrame:
beagles = zc.get_rows_by_filter(dogs_collection, { 'Breed' : 'beagle' })

# Get the image URLs associated with these rows
beagles_urls = zc.get_image_urls(dogs_collection, beagles)

# Download those images (into memory)
beagles_imgs = zc.download_image_batch(beagles_urls)
```

# In Development
This SDK is in active development, not all features are available yet. Creating/uploading to collections is not supported currently - check back soon!
