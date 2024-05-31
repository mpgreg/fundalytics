import webbrowser
from datetime import datetime
from pathlib import Path
from PIL import Image
import streamlit as st
from textwrap import dedent
from funda_scraper import FundaScraper
import pandas as pd
import base64
import requests
import weaviate
from weaviate.util import generate_uuid5
from weaviate.classes.query import Filter
import json
import plotly.express as px
import numpy as np
from sklearn.manifold import TSNE



COLLECTION_DEF_FILE = 'collection_def.json'
CITY_LIST_URL = 'https://simplemaps.com/static/data/country-cities/nl/nl.json'

st.set_page_config(layout="wide")

if "weaviate_client" not in st.session_state:
    weaviate_client = weaviate.connect_to_local()
    st.session_state["weaviate_client"] = weaviate_client
else:
    weaviate_client = st.session_state["weaviate_client"]

if "collection_def" not in st.session_state:
    with open(COLLECTION_DEF_FILE) as f:
        collection_def = json.load(f)
    st.session_state["collection_def"] = collection_def
else:
    collection_def = st.session_state["collection_def"]

if "collection" not in st.session_state:
    collection = weaviate_client.collections.get(collection_def['class'])
    st.session_state["collection"] = collection
else:
    collection = st.session_state["collection"]

if "city_dict" not in st.session_state:
    city_dict = requests.get(CITY_LIST_URL).json()
    st.session_state["city_dict"] = city_dict
else:
    city_dict = st.session_state["city_dict"]

if "city_list" not in st.session_state:
    city_list = [city['city'].lower() for city in city_dict]
    st.session_state["city_list"] = city_list
else:
    city_list = st.session_state["city_list"]

if "import_df" not in st.session_state:
    import_df = pd.DataFrame()
else:
    import_df = st.session_state["import_df"]

if "display_df" not in st.session_state:
    display_df = pd.DataFrame()
else:
    display_df = st.session_state["display_df"]

if "viewer_df" not in st.session_state:
    viewer_df = pd.DataFrame()
else:
    viewer_df = st.session_state["viewer_df"]

def scrape_and_process_data(scraper: FundaScraper) -> pd.DataFrame:

    df = scraper.run(raw_data=False, save=False)
    df.set_index('house_id', inplace=True)

    photos_df = df['photo'].apply(lambda x: x.split(',')).explode()
    photos_df = photos_df.apply(lambda x: x.split()).apply(pd.Series)
    photos_df = photos_df[photos_df[1] == "180w"].drop(1, axis=1)

    cover_photos = photos_df.groupby('house_id').agg(
        image_url = (0, lambda x: str(x.tolist()[0]))
        )
    cover_photos['image_enc'] = cover_photos['image_url'].apply(
        lambda x: base64.b64encode(requests.get(x).content).decode("utf-8")
        )
    
    ingest_df = df.join(cover_photos).drop('photo', axis=1).reset_index()
    ingest_df['uuid'] = ingest_df['house_id'].apply(lambda x: generate_uuid5(x))

    ingest_df['house_id'] = ingest_df['house_id'].apply(str)

    return ingest_df

def import_data(ingest_df: pd.DataFrame):

    try:
        results = []
        with collection.batch.dynamic() as batch:
            for data_row in ingest_df.to_dict('records'):
                results.append(batch.add_object(
                    uuid=data_row['uuid'],
                    properties=data_row,
                ))
    except:
        print("error")


header_image = Image.open(Path(__file__).parent / "logo.png")
    
st.markdown(
    """
<style>
.small-font {
    font-size:1px !important;
}
</style>""",
    unsafe_allow_html=True,
)
disclaimer = dedent("""
    <p><small>Disclaimer & Limitations\n\n 
    This application is a proof-of-concept for multi-modal search and is not created, supported or endorsed by Funda.</small></p>""")

with st.container():
    title_col, logo_col = st.columns([8, 2])
    with title_col:
        st.title("Welcome to Fundalytics!")
        st.write(dedent("""
            Fundalytics is a simple application to search and analyze Dutch real estate listings
            from [Funda](https://www.funda.nl). [Weaviate](https://weaviate.io/) vector database 
            is used to store and search postings based on image or text content."""))
    with logo_col:
        st.image(header_image) 

with st.sidebar:

    ##DEBUG: city_name='amsterdam'; want_to='buy'; property_type='house'; min_price=max_price=min_sqm=days_since=None

    city_name = st.selectbox(
            label="Select a city name. **",
            index=None,
            options=city_list,
            )
    
    want_to = st.selectbox(
            label="Select a transaction type. **",
            index=None,
            options=["buy", "rent"],
            )
    
    property_type = st.selectbox(
            label="Select a property type. **",
            index=None,
            options=["house", "apartment"],
            )
    
    min_price = st.number_input(
            label="Minimum price in €",
            value=None,
            )
    
    max_price = st.number_input(
            label="Maximum price in €",
            value=None,
            )
    
    min_sqm = st.number_input(
            label="Minimum size in m2",
            )
    
    days_since = st.selectbox(
            label="Days since listed",
            options=[None, 1, 3, 5, 10, 30],
            ) 
    
    st.write("** denotes mandatory fields.")
    
    #import_df = None

    if city_name and want_to and property_type:
        
        if st.button(label="Import Data"):
            
            scraper = FundaScraper(
                area=city_name, 
                want_to=want_to, 
                property_type=property_type,
                days_since=days_since,
                min_price=min_price,
                max_price=max_price,
                find_past=False, 
                page_start=1, 
                n_pages=1)
            
            import_df = scrape_and_process_data(scraper=scraper)

            st.session_state["import_df"] = import_df

            collection.data.delete_many(
                where=Filter.by_property("city").equal(city_name)
            )

            import_data(import_df)

# collection.query.fetch_objects(filters=Filter.by_property("city").equal(city_name))

listing_tab, threedviewer_tab, image_search_tab = st.tabs(
    ["Data Viewer", "3D Viewer", "Image Search"]
)

if not import_df.empty:

    display_df = import_df.drop(['image_enc','descrip', 'uuid'], axis=1)
    
    display_df['html_url'] = display_df.apply(
        lambda x: '<a href="{house_url}"></a>'.format(
            house_url=x.url),
        axis=1
        )
    
    display_df['linked_image'] = display_df.apply(
        lambda x: '<a href="{house_url}"><img src="{image_url}" width="60" ></a>'.format(
            image_url=x.image_url,
            house_url=x.url),
        axis=1
        )

    st.session_state["display_df"] = display_df

with listing_tab:
    
    st.header("Data Viewer")

    if display_df.empty:
        
        st.write("⚠️ Select at least a city and transaction type in the side bar.")
    
    else:

        st.write(f"Summary for {property_type}s to {want_to} in {city_name}.")

        display_columns=['address', 'living_area', 'price', 'price_m2', 'bedroom', 'bathroom', 'energy_label']

        dataviewer_df = display_df.set_index('linked_image')
        dataviewer_df.index.name = ''
        
        st.markdown(
            dataviewer_df[display_columns].to_html(
                escape=False,
                border=0), 
            unsafe_allow_html=True)
        

with threedviewer_tab:
    st.header("3D Viewer")

    if display_df.empty:
        
        st.write("⚠️ Select at least a city and transaction type in the side bar.")

    else:

        st.write(f"3D visualization for embedded {property_type}s to {want_to} in {city_name}.")

        display_columns = ['url','price','x','y','z']
        
        vectors = collection.query.fetch_objects(
            include_vector=True, 
            filters=Filter.by_property("city").equal(city_name),
            return_properties=['house_id']
            )

        vector_df = pd.DataFrame(
            [{'house_id': house.properties['house_id'], 
            'vector': house.vector['default']} 
            for house in vectors.objects])

        vectors_array = np.array(vector_df['vector'].values.tolist())

        reduced_vectors = TSNE(
            n_components=3, 
            learning_rate='auto', 
            init='pca', 
            perplexity=3
        ).fit_transform(vectors_array)

        vector_df = pd.concat(
            [vector_df, 
            pd.DataFrame(reduced_vectors, columns=['x','y','z'])], 
            axis=1
        ).set_index('house_id')

        viewer_df = display_df.set_index('house_id').join(vector_df)[display_columns]
        
        fig = px.scatter_3d(
            viewer_df, 
            x='x', 
            y='y', 
            z='z', 
            color='price', 
            hover_data={
                'url':False,
                'price': True,
                'x': False,
                'y': False,
                'z': False,
                },
            # custom_data='url',
            )
        
        fig.update_layout(
            height=800,width=1200,
        )

        st.plotly_chart(
            figure_or_data=fig, 
            use_container_width=True,
            on_select=webbrowser.open_new_tab,
            selection_mode='box'
            )