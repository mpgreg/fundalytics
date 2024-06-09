#!/bin/sh

# Start CLIP
uvicorn app:app --host 0.0.0.0 --port 8081 &

# Start sum-transformers
cd sum-transformers-models
uvicorn app:app --host 0.0.0.0 --port 8080 &
  
# Start streamlit app
cd ..
streamlit run streamlit/fundalytics_app_embedded.py

# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?
