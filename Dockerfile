FROM cr.weaviate.io/semitechnologies/multi2vec-clip:sentence-transformers-clip-ViT-B-32-multilingual-v1

COPY . .

ENV PATH="$PATH:/root/.cargo/bin"
ENV MODEL_NAME=facebook/bart-large-cnn
ENV ONNX_RUNTIME=false
ENV ONNX_CPU=AVX512_VNNI

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y curl build-essential git && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    pip install --upgrade pip setuptools && \
    pip3 install -r requirements.txt && \
    apt remove -y curl build-essential && \
    apt -y autoremove && \
    cd sum-transformers-models && \
    chmod +x ./download.py && \
    ./download.py && \
    cd ..

ENTRYPOINT ["/app/run.sh"]
