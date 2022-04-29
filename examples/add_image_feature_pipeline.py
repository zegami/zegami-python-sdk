# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)

collection.add_feature_pipeline(
    pipeline_name='mRMR20',  # name would be used to derive column ids/titles
    source=0,   # index or name of source to use
    steps=[     # list of nodes which would feed one into the other in sequence
        {
            'action': 'mRMR',
            'params': {
                'target_column': 'weight',
                'K': 20,
                'option': 'regression'  # another option is classification
            },
        },
        {
            'action': 'cluster',
            'params': {
                # 'algorithm_args': {
                #     'algorithm': 'umap',
                #     'n_components': 2,
                #     "n_neighbors": 15,
                #     "min_dist": 0.5,
                #     "spread": 2,
                #     "random_state": 123,
                # }
            }
        }
    ],
    generate_snapshot=True
)

collection.get_feature_pipelines()
