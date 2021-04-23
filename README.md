# Zegami Python SDK

![Zegami](zegami.png)

# zegami-python-sdk
An SDK and general wrapper for the lower level Zegami API for Python. This package provides higher level collection interaction and data retrieval.

# Getting started
Grab this repo, open the script, and load an instance of ZegamiClient into a variable.

```
from zegami_sdk.client import ZegamiClient

zc = ZegamiClient(username, login)
```

## Credentials
The client operates using a user token. By default, logging in once with a valid username/password will save the acquired token to your home directory as
`zegami.token`. The next time you need to use ZegamiClient, you may call `zc = ZegamiClient()` with no arguments, and it will look for this stored token.


## Example Usage
### Get the metadata and images associated with every dog of the 'beagle' breed in a collection of dogs:

`zc = ZegamiClient()`


### Workspaces
To see your available workspaces, use:

`zc.show_workspaces()`

You can then ask for a workspace by name, by ID, or just from a list
```
all_workspaces = zc.workspaces
first_workspace = all_workspaces[0]
```

or:

```
zc.show_workspaces()

# Note the ID of a workspace
my_workspace = zc.get_workspace_by_id(id)
```


### Collections
```
my_workspace.show_collections()

# Note the name of a collection
coll = my_workspace.get_collection_by_name(name_of_collection)
```


You can get the metadata in a collection as a Pandas DataFrame using:

```
rows = coll.rows
```

You can get the images of a collection using:

```
first_10_img_urls = coll.get_image_urls(list(range(10)))
imgs = coll.download_image_batch(first_10_img_urls)
```

If your collection supports the new multi-image-source functionality, you can see your available sources using:

```
coll.show_sources()
```

For source 2's (3rd in 0-indexed-list) images, you would use:

```
first_10_source3_img_urls = novo_col.get_image_urls(list(range(10)), source=2)`

# To see the first of these:
coll.download_image(first_10_source3_img_urls[0])
```

# In Development
This SDK is in active development, not all features are available yet. Creating/uploading to collections is not supported currently - check back soon!
