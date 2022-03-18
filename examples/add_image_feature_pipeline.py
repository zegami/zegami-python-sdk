# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''
NAME = 'Image Similarity 2'

zc = ZegamiClient()

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)

collection.add_feature_pipeline(
    name='mRMR20',  # name would be used to derive column ids/titles
    source=0,   # index of source to use
    steps=[     # list of nodes which would feed one into the other in sequence
        {
            'mRMR': {
                'target_column': 'lipid_volume',
                'n_components': 20,
            },
        },
        {
            'UMAP': {
                'n_components': 2,
                "n_neighbors": 15,
                "min_dist": 0.5,
                "spread": 2,
                "random_state": 123,
                "target_weight": 0.8,
                "target_metric": "continuous"
            }
        }
    ],
    generate_snapshot=True
)
