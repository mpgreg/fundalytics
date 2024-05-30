from datetime import datetime
from pathlib import Path
from PIL import Image
import streamlit as st
from time import sleep
from textwrap import dedent
from funda_scraper import FundaScraper
import pandas as pd
import base64
import requests
import weaviate
from weaviate.util import generate_uuid5
from weaviate.classes.query import Filter
import json

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

if "min_size" not in st.session_state:
    min_size = 0
else:
    min_size = st.session_state["min_size"]

if "max_size" not in st.session_state:
    max_size = None
else:
    max_size = st.session_state["max_size"]

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
            This Streamlit application is a simple application to interact with Dutch real estate 
            from [Funda](https://www.funda.nl). [Weaviate](https://weaviate.io/) vector database 
            is used to store and search postings based on image or text content."""))
    with logo_col:
        st.image(header_image) 

with st.sidebar:

    city_name = st.selectbox(
            label="Select a city name.",
            index=None,
            options=city_list,
            )
    
    want_to = st.selectbox(
            label="Select a transaction type.",
            index=None,
            options=["buy", "rent"],
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
    
    if city_name and want_to:
        if st.button(label="Import Data"):
            
            scraper = FundaScraper(
                area=city_name, 
                want_to=want_to, 
                days_since=days_since,
                min_price=min_price,
                max_price=max_price,
                find_past=False, 
                page_start=1, 
                n_pages=1)
            
            import_df = scrape_and_process_data(scraper=scraper)

            collection.data.delete_many(
                where=Filter.by_property("city").equal(city_name)
            )

            import_data(import_df)


# collection.query.fetch_objects(filters=Filter.by_property("city").equal("hoorn"))

# ingest_tab, finsum_tab, finsum_qna = st.tabs(
#     ["Ingest New Ticker", "FinSum 10-Q Summarization", "FinSum Q&A"]
# )

# with finsum_tab:
#     st.header("FinSum 10-Q Summarization")
        
#     if not fp:
#         st.write("⚠️ Select a company, fiscal year and fiscal period in the side bar.")
#     else:
#         st.write(f"Summary for {selected_company['title']} in fiscal period FY{fy}{fp}.")

#         summary = (
#             weaviate_client.query.get(summary_class, ["summary"])
#                 .with_where({
#                     "operator": "And",
#                     "operands": [
#                     {
#                         "path": ["tickerSymbol"],
#                         "operator": "Equal",
#                         "valueText": selected_company["ticker"]
#                     },
#                     {
#                         "path": ["fiscalYear"],
#                         "operator": "Equal",
#                         "valueInt": fy
#                     },
#                     {
#                         "path": ["fiscalPeriod"],
#                         "operator": "Equal",
#                         "valueText": fp
#                     }
#                 ]
#             })
#             .do()
#         )["data"]["Get"][summary_class][0]["summary"]

#         st.markdown(summary)

# with finsum_qna:
#     st.write(dedent("""
#         Ask a question regarding financial statements for the chosen company.  
#         FinSum will vectorize the question, retrieve related documents from 
#         the vector database and use that as context for OpenAI to generate 
#         a response."""))
    
#     if not selected_company:
#         st.write("⚠️ Select a company in the side bar.")
#         question = None
#     else:
#         question = st.text_area("Question:", placeholder="")

#     if question:
#         ask = {
#             "question": question,
#             "properties": ["content", "tickerSymbol", "fiscalYear", "fiscalPeriod"],
#             # "certainty": 0.0
#         }

#         st.write("Showing search results for:  " + question)
#         st.subheader("10-Q results")
#         results = (
#             weaviate_client.query.get(
#                 chunk_class, 
#                 ["docLink", 
#                     "tickerSymbol", 
#                     "_additional {answer {hasAnswer property result} }"
#                 ])
#             .with_where({
#                 "path": ["tickerSymbol"],
#                 "operator": "Equal",
#                 "valueText": selected_company["ticker"]
#             })
#             .with_ask(ask)
#             .with_limit(3)
#             .with_additional(["certainty", "id", "distance"])
#             .do()
#         )

#         if results.get("errors"):
#             for error in results["errors"]:
#                 if ("no api key found" or "remote client vectorize: failed with status: 401 error") in error["message"]:
#                     raise Exception("Cannot vectorize.  Check the OpenAI key in the airflow connection.")
#                 else:
#                     st.write(error["message"])

#         elif len(results["data"]["Get"][chunk_class]) > 0:
#             docLinks = []
#             link_count = 1
#             for result in results["data"]["Get"][chunk_class]:
#                 if result["_additional"]["answer"]["hasAnswer"]:
#                     write_response(result["_additional"]["answer"]["result"])
#                 docLinks.append(f"[{link_count}]({result['docLink']})")
#                 link_count = link_count + 1
#             st.write(",".join(docLinks))
#             # st.markdown(disclaimer, unsafe_allow_html=True)

# with ingest_tab:
#     st.header("Ingest new financial data")

#     st.write("""By selecting a company from the list below an Airflow DAG run will be 
#              triggered to extract, embed and summarize financial statements. Search 
#              by company name, ticker symbol or CIK number.""")
    
#     company_to_ingest = st.selectbox(
#         label="Select a company.",
#         index=None,
#         options=company_list,
#         format_func=format_tickers
#         )
    
#     if company_to_ingest: 

#         if st.button(label="Start Ingest"):

#             response = requests.post(
#                 url=f"{webserver_internal}/api/v1/dags/{dag_id}/dagRuns",
#                 headers={"Content-Type": "application/json"},
#                 auth=requests.auth.HTTPBasicAuth(
#                     webserver_username, webserver_password),
#                 data=json.dumps({
#                     "conf":  {
#                         "run_date": str(datetime.now()), 
#                         "ticker": company_to_ingest["ticker"]
#                         }
#                     })
#                 )
            
#             if response.ok:
#                 run_id = json.loads(response.text)['dag_run_id']
#                 link = f"{webserver_public}/dags/{dag_id}/grid?dag_run_id={run_id}&tab=graph"
#                 status_link = f"{webserver_internal}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
                    
#                 status = requests.get(
#                     url=status_link,
#                     headers={"Content-Type": "application/json"},
#                     auth=requests.auth.HTTPBasicAuth(
#                         webserver_username, webserver_password),
#                     )

#                 if status.ok:
#                     state = json.loads(status.content).get("state")

#                     if state in ["running", "queued"]:
#                         st.markdown(dedent(f"""
#                             Document ingest runnging for ticker {company_to_ingest["ticker"]}. \n
#                             Check status in the [Airflow webserver]({link})"""))
#                         st.write("⚠️ Do not refresh your browser.")
#                     else:
#                         st.error(f"Ingest not running: {state}")
                    
#                     with st.spinner():
            
#                         while state in ["running", "queued"]:
#                             sleep(5)

#                             status = requests.get(url=status_link,
#                                 headers={"Content-Type": "application/json"},
#                                 auth=requests.auth.HTTPBasicAuth(
#                                     webserver_username, webserver_password),
#                                 )
                            
#                             if status.ok:
#                                 state = json.loads(status.content).get("state")
#                             else:
#                                 st.error(status.reason)
                        
#                     if state == "success":
#                         st.success(dedent(f"""
#                             Ingest complete for ticker {company_to_ingest['ticker']}. 
#                             Please refresh your browser."""))
#                     else:
#                         st.error(f"Ingest failed: state {state}")
                
#                 else:
#                     st.error(f"Ingest failed: state {status.reason}")
                    
#             else:
#                 st.error(f"Could not start DAG: {response.reason}")