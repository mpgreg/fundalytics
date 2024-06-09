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
from weaviate.classes.query import Filter, MetadataQuery
import json
import plotly.express as px
import numpy as np
from sklearn.manifold import TSNE
import validators

COLLECTION_DEF_FILE = 'streamlit/collection_def.json'
CITY_LIST_URL = 'https://simplemaps.com/static/data/country-cities/nl/nl.json'

st.set_page_config(
    page_title="Fundalytics", 
    page_icon=str(Path(__file__).parent / "icon.png"), 
    layout='wide')

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
    collection = None
else:
    collection = st.session_state["collection"]

if "city_list" not in st.session_state:
    city_dict = requests.get(CITY_LIST_URL).json()
    city_list = [city['city'].lower() for city in city_dict]
    st.session_state["city_list"] = city_list
else:
    city_list = st.session_state["city_list"]

if "ingest_df" not in st.session_state:
    ingest_df = pd.DataFrame()
else:
    ingest_df = st.session_state["ingest_df"]
 
def scrape_and_process_data(scraper: FundaScraper) -> pd.DataFrame:

    download_df = scraper.run(raw_data=False, save=False)

    download_df['house_id'] = download_df['house_id'].apply(str)
    download_df.set_index('house_id', inplace=True)

    photos_df = download_df['photo'].apply(lambda x: x.split(',')).explode()
    photos_df = photos_df.apply(lambda x: x.split()).apply(pd.Series)
    photos_df = photos_df[photos_df[1] == "180w"].drop(1, axis=1)

    cover_photos = photos_df.groupby('house_id').agg(
        image_url = (0, lambda x: str(x.tolist()[0]))
        )
    cover_photos['image_enc'] = cover_photos['image_url'].apply(
        lambda x: base64.b64encode(requests.get(x).content).decode("utf-8")
        )
    
    ingest_df = download_df.join(cover_photos).drop('photo', axis=1).reset_index()
    
    ingest_df['uuid'] = ingest_df['house_id'].apply(lambda x: generate_uuid5(x))

    ingest_df['html_url'] = ingest_df.apply(
        lambda x: '<a href="{house_url}"></a>'.format(
            house_url=x.url),
        axis=1
        )
    
    ingest_df['linked_image'] = ingest_df.apply(
        lambda x: '<a href="{house_url}"><img src="{image_url}" width="60" ></a>'.format(
            image_url=x.image_url,
            house_url=x.url),
        axis=1
        )
    
    return ingest_df

def import_data(ingest_df: pd.DataFrame) -> weaviate.collections.Collection:

    try:        
        weaviate_client.collections.delete(collection_def['class'])

        collection = weaviate_client.collections.create_from_dict(config=collection_def)
        
        results = []
        with collection.batch.dynamic() as batch:
            for data_row in ingest_df.to_dict('records'):
                results.append(batch.add_object(
                    uuid=data_row['uuid'],
                    properties=data_row,
                ))
    except:
        print("error")

    return collection

def reset_search():
    st.session_state.search_input = ""

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
                    This application is a proof-of-concept for multi-modal search and is not created, 
                    supported or endorsed by Funda. Funda scraping is only allowed for personal use.  
                    Any commercial use of this application is prohibited. The author holds no liability 
                    for any misuse of the application.
                    </small></p>
                    """)

with st.container():
    title_col, logo_col = st.columns([7, 4])
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
    
    # days_since = st.selectbox(
    #         label="Days since listed",
    #         options=[None, 1, 3, 5, 10, 30],
    #         ) 
    
    st.write("** denotes mandatory fields.")
    
    if city_name and want_to and property_type:
        
        if st.button(label="Import Data"):
            
            scraper = FundaScraper(
                area=city_name, 
                want_to=want_to, 
                property_type=property_type,
                # days_since=days_since,
                min_price=min_price,
                max_price=max_price,
                find_past=False, 
                page_start=1, 
                n_pages=1)

            ingest_df = scrape_and_process_data(scraper=scraper)

            st.session_state["ingest_df"] = ingest_df

            collection = import_data(ingest_df)

            st.session_state["collection"] = collection

listing_tab, threedviewer_tab, image_search_tab = st.tabs(
    ["Data Viewer", "3D Viewer", "Multi-Modal Search"]
)

with listing_tab:
    
    st.header("Data Viewer")

    if ingest_df.empty:
        
        st.write("⚠️ Select at least a city and transaction type in the side bar.")
    
    else:

        st.write(f"Summary for {property_type}s to {want_to} in {city_name}.")

        display_columns=[
            'address', 
            'living_area', 
            'price', 
            'price_m2', 
            'bedroom', 
            'bathroom', 
            'energy_label']

        response = collection.query.fetch_objects(
            include_vector=False, 
            filters=(
                Filter.by_property("city").equal(city_name)
                ),
            return_properties=display_columns + ['linked_image']
            )
        
        data_list = []
        _ = [data_list.append(obj.properties) for obj in response.objects]

        dataviewer_df = pd.DataFrame(data_list).set_index('linked_image')
        dataviewer_df.index.name = ''
        
        st.markdown(
            dataviewer_df[display_columns].to_html(
                escape=False,
                border=0), 
            unsafe_allow_html=True)
        
with threedviewer_tab:
    st.header("3D Viewer")

    if ingest_df.empty:
        
        st.write("⚠️ Select at least a city and transaction type in the side bar.")

    else:

        st.write(f"3D visualization for embedded {property_type}s to {want_to} in {city_name}.")

        display_columns = ['house_id', 'url','price']
        
        response = collection.query.fetch_objects(
            include_vector=True, 
            filters=(
                Filter.by_property("city").equal(city_name)
                ),
            return_properties=display_columns
            )
        
        _ = [obj.properties.update({'vector': obj.vector['default']}) for obj in response.objects]
        
        data_list = []
        _ = [data_list.append(obj.properties) for obj in response.objects]

        vector_df = pd.DataFrame(data_list)

        vectors_array = np.array(vector_df['vector'].values.tolist())

        reduced_vectors = TSNE(
            n_components=3, 
            learning_rate='auto', 
            init='pca', 
            perplexity=3
        ).fit_transform(vectors_array)

        vector_df = pd.concat(
            [
                vector_df, 
                pd.DataFrame(reduced_vectors, columns=['x','y','z'])
                ], 
            axis=1
        ).set_index('house_id')
        
        fig = px.scatter_3d(
            vector_df, 
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
            )
        
        fig.update_layout(
            height=800,width=1200,
        )

        st.plotly_chart(
            figure_or_data=fig, 
            use_container_width=True,
            )
        
with image_search_tab:
    st.header("Multi-Modal Search")

    display_columns = [
        "linked_image",
        "address", 
        "living_area", 
        "price", 
        "price_m2", 
        "bedroom", 
        "bathroom", 
        "energy_label"]

    if ingest_df.empty:
        
        st.write("⚠️ Select at least a city and transaction type in the side bar.")

    else:

        search_string = st.text_input(
            label='Enter a URL for an image or a text description to find related properties.',
            key='search_input',
            help='https://cloud.funda.nl/valentina_media/191/337/476_180x120.jpg or "overdekt balkon"'
        )

        if search_string:
            st.button(
                label='Reset Input',
                on_click=reset_search
            )
            
            st.write("Searching for objects similar to:")

            if validators.url(search_string):

                st.image(search_string)
            
                image_content = requests.get(search_string).content
                
                search_image = base64.b64encode(image_content).decode("utf-8")

                response = collection.query.near_image(
                    near_image=search_image,
                    return_properties=display_columns,
                    limit=5,
                    return_metadata=MetadataQuery(distance=True)
                    )
            else:
                response = collection.query.near_text(
                    query=search_string,
                    return_properties=display_columns,
                    limit=5,
                    return_metadata=MetadataQuery(distance=True)
                )
            
            _ = [obj.properties.update({'distance': obj.metadata.distance}) for obj in response.objects]

            display_list = []
            _ = [display_list.append(obj.properties) for obj in response.objects]

            search_df = pd.DataFrame(display_list).set_index('linked_image')
            search_df.index.name = ''
            
            st.markdown(
                search_df.to_html(
                    escape=False,
                    border=0), 
                unsafe_allow_html=True
                )

st.markdown(disclaimer, unsafe_allow_html=True)