from funda_scraper import FundaScraper
import pandas as pd
import base64
import requests
import weaviate
from weaviate.util import generate_uuid5
import json


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
