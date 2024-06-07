# fundalytics


Standalone app

No particular business value

Not efficient if using costly embeddings

3d view doesn't tell anything specific... needs more research

Play with 1) embedded 2) multi-modal


NOTE: Fundascraper issue


docker run -it --rm -p 8501:8501 \
--mount type=bind,source="$(pwd)"/streamlit,target=/app/streamlit \
fundalytics:latest

docker run -it --rm -p 8501:8501 fundalytics:latest