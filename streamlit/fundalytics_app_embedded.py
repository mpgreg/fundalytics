from pathlib import Path
from PIL import Image
import streamlit as st
from textwrap import dedent
from funda_scraper import FundaScraper
import pandas as pd
import base64
import requests
import weaviate
from weaviate.embedded import EmbeddedOptions
from weaviate.util import generate_uuid5
from weaviate.classes.query import Filter, MetadataQuery
import json
import plotly.express as px
import numpy as np
from sklearn.manifold import TSNE
import validators
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode
from st_aggrid.shared import JsCode


COLLECTION_DEF_FILE = 'streamlit/collection_def.json'
CITY_LIST_URL = 'https://simplemaps.com/static/data/country-cities/nl/nl.json'

st.set_page_config(
    page_title='Fundalytics', 
    page_icon=str(Path(__file__).parent / 'icon.png'), 
    layout='wide')

def get_and_set_state(collection_def_file: str):

    if 'collection_def' in st.session_state:
        collection_def = st.session_state['collection_def']
    else:
        with open(COLLECTION_DEF_FILE) as f:
            collection_def = json.load(f)
        st.session_state['collection_def'] = collection_def

    if 'weaviate_client' in st.session_state:
        weaviate_client  = st.session_state['weaviate_client']
    else:
        weaviate_client = weaviate.WeaviateClient(
            embedded_options=EmbeddedOptions(
                additional_env_vars={
                    "ENABLE_MODULES": "multi2vec-clip",
                    "DEFAULT_VECTORIZER_MODULE": "multi2vec-clip",
                    "CLIP_INFERENCE_API": "http://localhost:8081"
                }
            )
        )
        st.session_state['weaviate_client'] = weaviate_client

    if not weaviate_client.is_live():
        try:
            weaviate_client.connect()
            st.session_state['weaviate_client'] = weaviate_client

        except Exception as e:
            
            ### Due to multi-threading in fundascraper and streamlit page refreshes we may need to 
            ### connect to an existing session.  This is quite a hack

            if isinstance(e, weaviate.exceptions.WeaviateStartUpError) and \
                "processes are already listening on ports" in e.message:

                existing_port = int([word for word in e.message.split() 
                                     if word.find('http:') >= 0][0].split(':')[1])
                existing_grpcport = int([word for word in e.message.split() 
                                     if word.find('grpc:') >= 0][0].split(':')[1].split('use')[0])
                
                weaviate_client = weaviate.connect_to_local(
                    port=existing_port,
                    grpc_port=existing_grpcport
                    )
                weaviate_client.connect()
                st.session_state['weaviate_client'] = weaviate_client

            else:
                raise e

    if 'collection' in st.session_state:
        collection = st.session_state['collection']
    else:
        if weaviate_client.collections.exists(name=collection_def['class']):
            collection = weaviate_client.collections.get(name=collection_def['class'])
            st.session_state['collection'] = collection
        else:
            collection = None

    if 'city_list' in st.session_state:
        city_list = st.session_state['city_list']
    else:
        city_dict = requests.get(CITY_LIST_URL).json()
        city_list = [city['city'].lower() for city in city_dict]
        city_list.sort()
        city_list.insert(0, 'nl')
        st.session_state['city_list'] = city_list

    if 'ingest_df' in st.session_state:
        ingest_df = st.session_state['ingest_df']
    else:
        ingest_df = pd.DataFrame()

    return collection_def, collection, weaviate_client, city_list, ingest_df
    
def scrape_and_process_data(scraper: FundaScraper) -> pd.DataFrame:

    download_df = scraper.run(raw_data=False, save=False)

    if not download_df.empty:

        download_df['house_id'] = download_df['house_id'].apply(str)
        download_df.set_index('house_id', inplace=True)

        photos_df = download_df['photo'].apply(lambda x: x.split(',')).explode()
        photos_df = photos_df.apply(lambda x: x.split()).apply(pd.Series)
        photos_df = photos_df[photos_df[1] == '180w'].drop(1, axis=1)

        cover_photos = photos_df.groupby('house_id').agg(
            image_url = (0, lambda x: str(x.tolist()[0]))
            )
        cover_photos['image_enc'] = cover_photos['image_url'].apply(
            lambda x: base64.b64encode(requests.get(x).content).decode('utf-8')
            )
        
        ingest_df = download_df.join(cover_photos).drop('photo', axis=1).reset_index()
        
        ingest_df['uuid'] = ingest_df['house_id'].apply(lambda x: generate_uuid5(x))

        ingest_df['html_url'] = ingest_df.apply(
            lambda x: '<a href="{house_url}"></a>'.format(
                house_url=x.url),
            axis=1
            )
        
        ingest_df['linked_image'] = ingest_df.apply(
            lambda x: '<a href="{house_url}" target="_blank"><img src="{image_url}" width="60" ></a>'.format(
                image_url=x.image_url,
                house_url=x.url),
            axis=1
            )
    else:
        ingest_df = download_df
    
    return ingest_df

def import_data(
    weaviate_client: weaviate.Client, 
    collection_def: dict,
    collection: weaviate.collections.Collection | None,
    ingest_df: pd.DataFrame) -> weaviate.collections.Collection:
    
    if weaviate_client.collections.exists(name=collection_def['class']):
        weaviate_client.collections.delete(collection_def['class'])
    
    collection = weaviate_client.collections.create_from_dict(collection_def)

    results = []
    with collection.batch.dynamic() as batch:
        for data_row in ingest_df.to_dict('records'):
            results.append(batch.add_object(
                uuid=data_row['uuid'],
                properties=data_row,
            ))

    ##TODO: error handling for import results

    return collection

def reset_search():
    st.session_state.search_input = ''

def reset_ingest():
    st.session_state.ingest_df = pd.DataFrame()
    ingest_df = pd.DataFrame()

collection_def, collection, weaviate_client, city_list, ingest_df = get_and_set_state(COLLECTION_DEF_FILE)

header_image = Image.open(Path(__file__).parent / 'logo.png')
    
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
                    <div id="footer">
                    <div id="footer-content"><small>Disclaimer & Limitations\n\n 
                    This application is a proof-of-concept for multi-modal search and is not created, 
                    supported or endorsed by Funda. Funda scraping is only allowed for personal use.  
                    Any commercial use of this application is prohibited. The author holds no liability 
                    for any misuse of the application.
                    </small></div></div>
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

    city_name = st.selectbox(
            label="Select a city name. **",
            index=None,
            options=city_list,
            on_change=reset_ingest,
            )
    
    want_to = st.selectbox(
            label="Select a transaction type. **",
            index=None,
            options=['buy', 'rent'],
            on_change=reset_ingest,
            )
    
    property_type = st.selectbox(
            label="Select a property type. **",
            index=None,
            options=['house', 'apartment'],
            on_change=reset_ingest,
            )
    
    max_pages = st.number_input(
            label="Maximum number of Funda pages to pull",
            value=1,
            on_change=reset_ingest,
            )
    
    min_price = st.number_input(
            label="Minimum price in €",
            value=None,
            on_change=reset_ingest,
            )
    
    max_price = st.number_input(
            label="Maximum price in €",
            value=None,
            on_change=reset_ingest,
            )
    
    # min_sqm = st.number_input(
    #         label="Minimum size in m2",
    #         on_change=reset_ingest,
    #         )
    
    days_since = st.selectbox(
            label="Days since listed",
            options=[None, 1, 3, 5, 10, 30],
            on_change=reset_ingest,
            ) 
    
    st.write("** required fields")
    
    if city_name and want_to and property_type:
        
        if st.button(label="Import Data"):

            status_message = st.empty()
            
            ##DEBUG: city_name='almelo'; want_to='buy'; property_type='house'; max_pages=1; min_price=max_price=min_sqm=days_since=None

            scraper = FundaScraper(
                area=city_name, 
                want_to=want_to, 
                property_type=property_type,
                days_since=days_since,
                min_price=min_price,
                max_price=max_price,
                find_past=False, 
                page_start=1, 
                n_pages=max_pages)

            status_message.text('Scraping Data... please wait')

            ingest_df = scrape_and_process_data(scraper=scraper)

            st.session_state['ingest_df'] = ingest_df

            status_message.text('Importing data to Weaviate Embedded instance... please wait')

            collection = import_data(
                weaviate_client=weaviate_client,
                collection_def=collection_def,
                collection=collection,
                ingest_df=ingest_df)
            
            status_message.text('Import completed')
            
            st.session_state['collection'] = collection

listing_tab, threedviewer_tab, image_search_tab = st.tabs(
    ['Data Viewer', '3D Viewer', 'Multi-Modal Search']
)

with listing_tab:
    
    st.header('Data Viewer')

    if ingest_df.empty:
        
        st.write("⚠️ Select at least a city, transaction type and property type in the side bar.")
    
    else:

        st.write(f"Summary for {property_type}s to {want_to} in {city_name}.")

        listing_display_columns=[
            'address', 
            'city',
            'living_area', 
            'price', 
            'price_m2', 
            'bedroom', 
            'bathroom', 
            'energy_label',
            ]
        
        if city_name.lower() == 'nl':
            listing_filters = None
        else:
            listing_filters = (
                    Filter.by_property('city').equal(city_name)
                    )

        listing_response = collection.query.fetch_objects(
            include_vector=False, 
            filters=listing_filters,
            return_properties=['linked_image'] + listing_display_columns,
            limit=10000,
            )
                
        listing_data_list = []
        _ = [listing_data_list.append(obj.properties) for obj in listing_response.objects]

        listing_df = pd.DataFrame(listing_data_list) #.set_index('linked_image')
        # listing_df.index.name = ''
        
        if listing_df.empty:
            st.write('No properties found for the given search criteria')
        else:
            # st.markdown(
            #     listing_df[listing_display_columns].to_html(
            #         escape=False,
            #         border=0), 
            #     unsafe_allow_html=True)
            
            gb = GridOptionsBuilder.from_dataframe(
                listing_df[['linked_image'] + listing_display_columns],
                editable=False,
                )
            
            gb.configure_column('linked_image',
                headerName='',
                cellRenderer=JsCode("""
                    class UrlCellRenderer {
                    init(params) {
                        this.eGui = document.createElement('div');
                        this.eGui.innerHTML = params.data.linked_image
                    }
                    getGui() {
                        return this.eGui;
                    }
                    }
                    """),
                width=100)
            
            gb.configure_grid_options(
                rowHeight=40,
                suppressColumnVirtualisation=True
                )

            AgGrid(
                data=listing_df,
                gridOptions=gb.build(),
                columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
                allow_unsafe_jscode=True
                )
                    
with threedviewer_tab:
    st.header('3D Viewer')

    if ingest_df.empty:
        
        st.write("⚠️ Select at least a city, transaction type and property type in the side bar.")

    else:

        st.write(f"3D visualization for embedded {property_type}s to {want_to} in {city_name}.")

        threed_display_columns = ['house_id', 'url','price']
                        
        if city_name.lower() == 'nl':
            threed_filters = None
        else:
            threed_filters = (
                    Filter.by_property('city').equal(city_name)
                    )

        threed_response = collection.query.fetch_objects(
            include_vector=True, 
            filters=threed_filters,
            return_properties=threed_display_columns,
            limit=10000
            )
        
        len(threed_response.objects)
                    
        _ = [obj.properties.update({'vector': obj.vector['default']}) for obj in threed_response.objects]
        
        threed_data_list = []
        _ = [threed_data_list.append(obj.properties) for obj in threed_response.objects]

        vector_df = pd.DataFrame(threed_data_list)

        vectors_array = np.array(vector_df['vector'].values.tolist())

        if len(vectors_array) <= 3:
            st.write("Insufficient data instances to plot.  Dataset must have at least 3 properties.")
        else:
            reduced_vectors = TSNE(
                n_components=3, 
                learning_rate='auto', 
                init='pca', 
                perplexity=3,
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
    st.header('Multi-Modal Search')

    display_columns = [
        'linked_image',
        'address', 
        'city',
        'living_area', 
        'price', 
        'price_m2', 
        'bedroom', 
        'bathroom', 
        'energy_label']

    if ingest_df.empty:
        
        st.write("⚠️ Select at least a city, transaction type and property type in the side bar.")

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
            
            if city_name.lower() == 'nl':
                search_filters = None
            else:
                search_filters = (
                        Filter.by_property('city').equal(city_name)
                        )            
            
            if validators.url(search_string):

                st.image(search_string)
            
                image_content = requests.get(search_string).content
                
                search_image = base64.b64encode(image_content).decode('utf-8')
                
                search_response = collection.query.near_image(
                    near_image=search_image,
                    filters=search_filters,
                    return_properties=display_columns,
                    limit=5,
                    return_metadata=MetadataQuery(distance=True)
                    )
            else:
                search_response = collection.query.near_text(
                    query=search_string,
                    filters=search_filters,
                    return_properties=display_columns,
                    limit=5,
                    return_metadata=MetadataQuery(distance=True)
                )
            
            _ = [obj.properties.update({'similarity': obj.metadata.distance}) for obj in search_response.objects]

            search_display_list = []
            _ = [search_display_list.append(obj.properties) for obj in search_response.objects]

            search_df = pd.DataFrame(search_display_list).set_index('linked_image')
            search_df.index.name = ''
            
            if listing_df.empty:
                st.write('No properties found for the given search criteria')
            else:
                st.markdown(
                    search_df.to_html(
                        escape=False,
                        border=0), 
                    unsafe_allow_html=True
                    )
 
st.markdown(disclaimer, unsafe_allow_html=True)
