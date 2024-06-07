from funda_scraper import FundaScraper
import pandas as pd
import base64
import requests
import weaviate
from weaviate.util import generate_uuid5
import json
from weaviate.classes.query import Filter


COLLECTION_DEF_FILE = 'weaviate/application/fundalytics/collection_def.json'

client = weaviate.connect_to_local()

try:

    current_collections = client.collections.list_all()

    with open(COLLECTION_DEF_FILE) as f:
        collection_def = json.load(f)

    if collection_def['class'] not in current_collections.keys():
        client.collections.create_from_dict(config=collection_def)

    scraper = FundaScraper(area="haarlem", want_to="buy", find_past=False, page_start=1, n_pages=1)
    df = scraper.run(raw_data=False, save=False)
    df.set_index('house_id', inplace=True)

    #process photos
    photos_df = df['photo'].apply(lambda x: x.split(',')).explode()
    photos_df = photos_df.apply(lambda x: x.split()).apply(pd.Series)
    photos_df = photos_df[photos_df[1] == "180w"].drop(1, axis=1)

    cover_photos = photos_df.groupby('house_id').agg(image_url = (0, lambda x: str(x.tolist()[0])))
    cover_photos['image_enc'] = cover_photos['image_url'].apply(lambda x: base64.b64encode(requests.get(x).content).decode("utf-8"))

    ingest_df = df.join(cover_photos).drop('photo', axis=1).reset_index()
    ingest_df['uuid'] = ingest_df['house_id'].apply(lambda x: generate_uuid5(x))

    ingest_df['house_id'] = ingest_df['house_id'].apply(str)

    #import to weaviate

    collection = client.collections.get(collection_def['class'])

    with collection.batch.dynamic() as batch:
        for data_row in ingest_df.to_dict('records'):
            batch.add_object(
                uuid=data_row['uuid'],
                properties=data_row,
            )

    for item in collection.iterator():
        print(item.uuid, item.properties)

finally:
    item
    #client.close()

vectors = collection.query.fetch_objects(
    include_vector=True, 
    filters=Filter.by_property("city").equal(city_name),
    return_properties=['house_id']
    )

vector_df = pd.DataFrame(
    [{'house_id': house.properties['house_id'], 
      'vector': house.vector['default']} 
      for house in vectors.objects])

import numpy as np
from sklearn.manifold import TSNE

vectors_array = np.array(vector_df['vector'].values.tolist())

reduced_vectors = TSNE(
    n_components=3, 
    learning_rate='auto', 
    init='pca', 
    perplexity=3).fit_transform(vectors_array)

vector_df = pd.concat(
    [vector_df, 
     pd.DataFrame(reduced_vectors, columns=['x','y','z'])], 
    axis=1).set_index('house_id')

display_df = ingest_df.set_index('house_id').join(vector_df)[['url','price','x','y','z']]

import plotly.express as px
fig = px.scatter_3d(display_df, x='x', y='y', z='z', color='price', custom_data='url')
fig.show()