import json
from algoliasearch.search_client import SearchClient
import pandas as pd


client = SearchClient.create('219WX3MPV4', 'b528008a75dc1c4402bfe0d8db8b3f8e')
index = client.init_index('companies')
index.browse_objects()
hits = []
bucket_count = 0
for hit in index.browse_objects({'query': ''}):
    print('hit', hit)
    hits.append(hit)

    if len(hits) == 1000:
        bucket_count += 1
        df = pd.DataFrame.from_dict(hits)
        df.to_csv('techinasia_bucket_{}.csv'.format(bucket_count))
        hits = []


# result = index.search('', {'hitsPerPage': 1000, 'page': 10, })
# print(result)

# df = pd.DataFrame.from_dict(result)

# df.to_csv('techinasia.csv')
