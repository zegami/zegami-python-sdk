from zegami_sdk.client import ZegamiClient

import pandas as pd

client = ZegamiClient()

workspace = client.get_workspace_by_id('')

coll = workspace.get_collection_by_id('')
coll.clear_cache()
rows = coll.get_rows_by_tags([''])
urls = coll.get_image_urls(rows, generate_signed_urls=True, signed_expiry_days=120)

table = []

rows['url'] = urls

subset = rows[['imageName', 'url']]

subset.to_csv('annotations.csv')
