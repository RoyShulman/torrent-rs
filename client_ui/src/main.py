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

def send_get_request(api_endpoint):
    """
    Send a GET request to the client api server and return the response as a JSON object.
    """
    url = f"http://localhost:3000/{api_endpoint}"
    response = requests.get(url)
    if response.status_code != 200:
        raise ApiRequestFailedException(response.status_code, response.text, api_endpoint)
    return response.json()

def send_post_request(api_endpoint, json_data):
    """
    Send a POST request to the client api server and return the response as a JSON object.
    """
    url = f"http://localhost:3000/{api_endpoint}"
    response = requests.post(url, json=json_data)
    if response.status_code != 200:
        raise ApiRequestFailedException(response.status_code, response.text, api_endpoint)

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
        files = send_get_request("list_server_files")
    except ApiRequestFailedException as e:
        show_api_request_fail(e)
        return

    
    files_df = pd.DataFrame(files, columns=["filename", "num_chunks", "chunk_size", "peers", "file_size"])
    if files_df.empty:
        st.info("No files available on the server")
        return
    files_df["formatted_chunk_size"] = files_df["chunk_size"].apply(lambda x: decimal(int(x)))
    files_df["formatted_file_size"] = files_df["file_size"].apply(lambda x: decimal(int(x)))
    files_df["select_to_download"] = False
    
    selected_df = st.data_editor(files_df, column_config={
        "filename": "Filename",
        "num_chunks": st.column_config.NumberColumn(
            "Number of Chunks",
            format="%u",
            help="The number of chunks the file is split into",
        ),
        "formatted_chunk_size": "Chunk size",
        "formatted_file_size": "File size",
        "peers": "Peers",
        "select_to_download": "Select to download",
        "chunk_size": None,
        "file_size": None
    }, use_container_width=True)

    selected_df = selected_df[selected_df["select_to_download"] == True]
    if selected_df.empty:
        return
    st.info(f"Downloading {selected_df.shape[0]} files from the server :hourglass_flowing_sand:")
    for _, row in selected_df.iterrows():
        try:
            send_post_request("download_file", {"filename": row["filename"], "num_chunks": row["num_chunks"],
                                                "chunk_size": row["chunk_size"], "file_size": row["file_size"]})
        except ApiRequestFailedException as e:
            show_api_request_fail(e)
            return

def get_chunk_percentage(row):
    num_available_chunks =  (row['num_chunks'] - len(row['missing_chunks']) * 1.)/row['num_chunks']
    return num_available_chunks * 100

def show_local_files():
    """
    Display all the local files currently on the client
    """
    st.subheader("Local files available on the client 📁")
    try:
        completed = send_get_request("list_local_files")
        currently_downloading = send_get_request("currently_downloading")
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
        "select_to_delete": "Select to delete"
    }, use_container_width=True, disabled=["filename", "num_chunks", "formatted_chunk_size", "chunks_progress"])

    selected_df = selected_df[selected_df["select_to_delete"] == True]
    if selected_df.empty:
        return
    
    st.info(f"Deleting {selected_df.shape[0]} files from the client :hourglass_flowing_sand:")
    for _, row in selected_df.iterrows():
        try:
            send_post_request("delete_file", {"filename": row["filename"]})
        except ApiRequestFailedException as e:
            show_api_request_fail(e)
            return
    time.sleep(2)
    st.rerun()
    

def show_upload_file():
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
        upload_file(uploaded_file.name, uploaded_file.read())
        st.session_state["file_uploader_key"] += 1
    except ApiRequestFailedException as e:
        show_api_request_fail(e)
        st.session_state["file_uploader_key"] += 1
        return
    time.sleep(2)
    st.rerun()

def run_health_check():
    try:
        send_get_request("healthcheck")
        return True
    except requests.exceptions.ConnectionError:
        return False


st.title("Download File Client")
st.button("Refresh")

if not run_health_check():
    st.error("Failed to connect to the client api. Make sure it is listening on port 3000", icon="🚨")
    st.stop()

show_server_files()
show_local_files()
show_upload_file()

