#!/bin/sh

# Start CLIP
uvicorn app:app --host 0.0.0.0 --port 8081 &
  
# Start streamlit app
streamlit run streamlit/fundalytics_app_embedded.py

# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?
