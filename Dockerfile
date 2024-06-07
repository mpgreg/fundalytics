FROM cr.weaviate.io/semitechnologies/multi2vec-clip:sentence-transformers-clip-ViT-B-32-multilingual-v1

RUN apt-get update && apt-get install -y git
RUN pip install --upgrade pip setuptools

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY streamlit streamlit
COPY run.sh .

ENTRYPOINT ["/app/run.sh"]
