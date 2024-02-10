import streamlit as st
import pandas as pd
import requests
from rich.filesize import decimal
import time

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
Failed to request {e.endpoint}.\n
Server error (status code: {e.response_code}):\n
{e.error} """, 
icon="🚨")

def send_get_request(endpoint, api_endpoint):
    """
    Send a GET request to the client api server and return the response as a JSON object.
    """
    url = f"http://{endpoint}/{api_endpoint}"
    response = requests.get(url)
    if response.status_code != 200:
        raise ApiRequestFailedException(response.status_code, response.text, api_endpoint)
    return response.json()

def send_post_request(endpoint, api_endpoint, json_data):
    """
    Send a POST request to the client api server and return the response as a JSON object.
    """
    url = f"http://{endpoint}/{api_endpoint}"
    response = requests.post(url, json=json_data)
    if response.status_code != 200:
        raise ApiRequestFailedException(response.status_code, response.text, api_endpoint)

def upload_file(endpoint, filename, file_data):
    """
    Send a request to upload a new client file
    """
    url = f"http://{endpoint}/upload_new_client_file"
    response = requests.post(url, files={filename: file_data})
    if response.status_code != 200:
        raise ApiRequestFailedException(response.status_code, response.text, "upload_new_client_file")
    

def show_server_files(endpoint):
    """
    Display all the server files currently on the client.
    """
    st.subheader("Files available on the server 🌐")

    try:
        files = send_get_request(endpoint, "list_server_files")
        currently_downloading = send_get_request(endpoint, "currently_downloading")
    except ApiRequestFailedException as e:
        show_api_request_fail(e)
        return
    
    files_df = pd.DataFrame(files, columns=["filename", "num_chunks", "chunk_size", "peers", "file_size"])
    if files_df.empty:
        st.info("No files available on the server")
        return
    files_df["select_to_download"] = False
    files_df["formatted_chunk_size"] = files_df["chunk_size"].apply(lambda x: decimal(int(x)))
    files_df["formatted_file_size"] = files_df["file_size"].apply(lambda x: decimal(int(x)))
    
    selected_df = st.data_editor(files_df, column_config={
        "filename": "Filename",
        "num_chunks": st.column_config.NumberColumn(
            "Number of Chunks",
            help="The number of chunks the file is split into",
        ),
        "formatted_chunk_size": "Chunk size",
        "formatted_file_size": "File size",
        "peers": "Peers",
        "select_to_download": "Select to download",
        "chunk_size": None,
        "file_size": None
    }, use_container_width=True, disabled=["filename", "num_chunks", "formatted_chunk_size", "formatted_file_size", "peers"])

    selected_df = selected_df[selected_df["select_to_download"] == True]

    # filter out the files that are already being downloaded
    currently_downloading = pd.DataFrame(currently_downloading, columns=["filename", "peers"])
    selected_df = selected_df[~selected_df["filename"].isin(currently_downloading["filename"].unique())]
    
    if selected_df.empty:
        if len(currently_downloading) > 0:
            st.info(f"Downloading {len(currently_downloading)} files from the server :hourglass_flowing_sand:")
        return
    st.info(f"Downloading {selected_df.shape[0] + len(currently_downloading)} files from the server :hourglass_flowing_sand:")
    for _, row in selected_df.iterrows():
        try:
            send_post_request(endpoint, "download_file", {"filename": row["filename"], "num_chunks": row["num_chunks"],
                                                "chunk_size": row["chunk_size"], "file_size": row["file_size"]})


        except ApiRequestFailedException as e:
            show_api_request_fail(e)
            return
    time.sleep(2)
    st.rerun()

def get_chunk_percentage(row):
    num_available_chunks =  (row['num_chunks'] - len(row['missing_chunks']) * 1.)/row['num_chunks']
    return num_available_chunks * 100

def show_local_files(endpoint):
    """
    Display all the local files currently on the client
    """
    st.subheader("Local files available on the client 📁")
    try:
        completed = send_get_request(endpoint, "list_local_files")
        currently_downloading = send_get_request(endpoint, "currently_downloading")
    except ApiRequestFailedException as e:
        show_api_request_fail(e)
        return
    files_df = pd.DataFrame(completed, columns=["filename", "num_chunks", "missing_chunks", "chunk_size", "chunk_states_directory", "file_size"])
    downloading_df = pd.DataFrame(currently_downloading, columns=["filename", "peers"])
    files_df = files_df.merge(downloading_df, on="filename", how="left")
    if files_df.empty:
        st.info("No local files available on the client")
        return

    files_df["formatted_chunk_size"] = files_df["chunk_size"].apply(lambda x: decimal(int(x)))
    files_df["formatted_file_size"] = files_df["file_size"].apply(lambda x: decimal(int(x)))
    files_df = files_df.drop(columns="chunk_size").drop(columns="chunk_states_directory").drop(columns="file_size")
    files_df["num_chunks"] = files_df["num_chunks"].astype(int)
    # Calculate the progress of the chunks as percentage
    files_df["chunks_progress"] = files_df.apply(lambda row: get_chunk_percentage(row), axis=1)
    files_df = files_df.drop(columns="missing_chunks")
    files_df["select_to_delete"] = False
    files_df["peers"] = files_df["peers"].apply(lambda x: x if isinstance(x, list) else [])

    selected_df = st.data_editor(files_df, column_config={
        "filename": "Filename",
        "num_chunks": st.column_config.NumberColumn(
            "Number of Chunks",
            format="%u",
            help="The number of chunks the file is split into",
        ),
        "formatted_chunk_size": "Chunk size",
        "formatted_file_size": "File size",
        "chunks_progress": st.column_config.ProgressColumn(
            "Chunks Progress",
            help="The number of chunks that have been downloaded",
            min_value=0,
            max_value=100,
            format='%.2f'
        ),
        "select_to_delete": "Select to delete",
        "peers": st.column_config.ListColumn(
            "Peers",
        ),
    }, use_container_width=True, disabled=["filename", "num_chunks", "formatted_chunk_size", "chunks_progress"])

    selected_df = selected_df[selected_df["select_to_delete"] == True]
    if selected_df.empty:
        return
    
    st.info(f"Deleting {selected_df.shape[0]} files from the client :hourglass_flowing_sand:")
    for _, row in selected_df.iterrows():
        try:
            send_post_request(endpoint, "delete_file", {"filename": row["filename"]})
        except ApiRequestFailedException as e:
            show_api_request_fail(e)
            return
    time.sleep(2)
    st.rerun()
    

def show_upload_file(endpoint):
    """
    Allows users to upload files to the client
    """
    st.subheader("Upload a file to make it available to the server 📤")

    if "file_uploader_key" not in st.session_state:
        st.session_state["file_uploader_key"] = 0

    file_uploader_key = st.session_state["file_uploader_key"]
    uploaded_file = st.file_uploader("Upload a file", key=f"file_uploader_key_{file_uploader_key}")
    if uploaded_file is None:
        return
    st.info(f"Uploading {uploaded_file.name} to the client :hourglass_flowing_sand:")
    try:
        upload_file(endpoint, uploaded_file.name, uploaded_file.read())
        st.session_state["file_uploader_key"] += 1
    except ApiRequestFailedException as e:
        show_api_request_fail(e)
        st.session_state["file_uploader_key"] += 1
        return
    time.sleep(2)
    st.rerun()

def run_health_check(endpoint):
    try:
        send_get_request(endpoint, "healthcheck")
        return True
    except requests.exceptions.ConnectionError:
        return False


st.set_page_config(page_title="Download File Client", layout="wide")
st.title("Download File Client")

with st.sidebar:
    st.subheader("Client API")
    endpoint = st.text_input("Client API Endpoint", value="localhost:3000", key="client_api_endpoint")

st.button("Refresh")

if not run_health_check(endpoint):
    st.error("Failed to connect to the client api. Make sure it is listening", icon="🚨")
    st.stop()

show_server_files(endpoint)
show_local_files(endpoint)
show_upload_file(endpoint)
