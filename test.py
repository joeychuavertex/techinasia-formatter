
import json
from algoliasearch.search_client import SearchClient
import pandas as pd


client = SearchClient.create('O2Z4JWP9D3', '8ae8d88ee27d7120ddfd99ddabc5e52d')
index = client.init_index('organizations-production')
# index.browse_objects()
hits = []
bucket_count = 0
# for hit in index.browse_objects({'query': 'sin'}):
#     print('hit', hit)
#     hits.append(hit)

# if len(hits) == 1000:
#     bucket_count += 1
#     df = pd.DataFrame.from_dict(hits)
#     df.to_csv('techinasia_bucket_{}.csv'.format(bucket_count))
#     hits = []


result = index.search('sin', {'hitsPerPage': 100, 'page': 1, })
print(result)

# df = pd.DataFrame.from_dict(result)

# df.to_csv('techinasia.csv')
