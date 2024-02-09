import streamlit as st
import pandas as pd
import requests
from rich.filesize import decimal

class ApiRequestFailedException(Exception):
    def __init__(self, response_code, error, endpoint, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.response_code = response_code
        self.error = error
        self.endpoint = endpoint

def show_api_request_fail(e: ApiRequestFailedException):
    """
    Show an error message when an API request fails.
    """
    st.error(f"""
Failed to get {e.endpoint}.\n
Server error (status code: {e.response_code}):\n
{e.error} """, 
icon="🚨")

def send_request(api_endpoint):
    """
    Send a request to the client api server and return the response as a JSON object.
    """
    url = f"http://localhost:3000/{api_endpoint}"
    response = requests.get(url)
    if response.status_code != 200:
        raise ApiRequestFailedException(response.status_code, response.text, api_endpoint)
    return response.json()

def upload_file(filename, file_data):
    """
    Send a request to upload a new client file
    """
    url = "http://localhost:3000/upload_new_client_file"
    response = requests.post(url, files={filename: file_data})
    if response.status_code != 200:
        raise ApiRequestFailedException(response.status_code, response.text, "upload_new_client_file")
    

def show_server_files():
    """
    Display all the server files currently on the client.
    """
    st.subheader("Files available on the server 🌐")

    try:
        files = send_request("list_server_files")
    except ApiRequestFailedException as e:
        show_api_request_fail(e)
        return

    
    files_df = pd.DataFrame(files, columns=["filename", "num_chunks", "chunk_size", "peers"])
    files_df["formatted_chunk_size"] = files_df["chunk_size"].apply(lambda x: decimal(int(x)))
    files_df.drop(columns="chunk_size", inplace=True)
    
    st.dataframe(files_df, column_config={
        "filename": "Filename",
        "num_chunks": st.column_config.NumberColumn(
            "Number of Chunks",
            format="%u",
            help="The number of chunks the file is split into",
        ),
        "formatted_chunk_size": "Chunk size",
        "peers": "Peers",
    }, use_container_width=True)

    st.button("Refresh")

def show_local_files():
    """
    Display all the local files currently on the client
    """
    st.subheader("Local files available on the client 📁")
    

def show_upload_file():
    """
    Allows users to upload files to the client
    """
    st.subheader("Upload a file to make it available to the server 📤")

    if "file_uploader_key" not in st.session_state:
        st.session_state["file_uploader_key"] = 0

    file_uploader_key = st.session_state["file_uploader_key"]
    print(file_uploader_key)
    uploaded_file = st.file_uploader("Upload a file", key=f"file_uploader_key_{file_uploader_key}")
    if uploaded_file is None:
        return
    st.info(f"Uploading {uploaded_file.name} to the client :hourglass_flowing_sand:")
    try:
        upload_file(uploaded_file.name, uploaded_file.read())
        st.session_state["file_uploader_key"] += 1
    except ApiRequestFailedException as e:
        show_api_request_fail(e)
        st.session_state["file_uploader_key"] += 1
        return
    st.rerun()

def run_health_check():
    try:
        send_request("healthcheck")
        return True
    except requests.exceptions.ConnectionError:
        return False


st.title("Download File Client")

if not run_health_check():
    st.error("Failed to connect to the client api. Make sure it is listening on port 3000", icon="🚨")
    st.stop()

show_server_files()
show_local_files()
show_upload_file()

