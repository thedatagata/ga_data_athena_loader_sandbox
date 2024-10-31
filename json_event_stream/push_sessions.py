 # Wait 1 second before retrying if unsuccessfulimport json
import requests
from datetime import datetime
import json 

# Load the generated session data JSON file
with open('./mock_sessions.json', 'r') as f:
    session_data_list = json.load(f)

# Define the URL for the session_data endpoint
url = "http://localhost:8000/session_data"


for session_data in session_data_list: 
    try:
        # Make a POST request to the /session_data endpoint
        response = requests.post(url, json=session_data)
        print(response.json())
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

