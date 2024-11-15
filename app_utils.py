import pandas as pd 
import av
import io 
import logging
import asyncio
import aiohttp
import os
import logging
from io import BytesIO
import pickle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_data_pkl(data, file_path):
    """
    Save a dictionary to a file in pickle format.

    If the file already exists, update the data by inserting new data.

    Args:
        data (dict): The dictionary to save.
        file_path (str): The path to the file.

    Returns:
        None
    """
    if os.path.exists(file_path):
        with open(file_path, 'rb') as f:
            existing_data = pickle.load(f)
            existing_data.append(data)
        with open(file_path, 'wb') as f:
            pickle.dump(existing_data, f)
    else:
        with open(file_path, 'wb') as f:
            pickle.dump([data], f)

def load_data_pkl(file_path):
    """
    Load data from a file in pickle format.

    Args:
        file_path (str): The path to the file.

    Returns:
        list: A list of dictionaries.
    """
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
    return data

async def producer(queue, producer_id, video_urls):
    async with aiohttp.ClientSession() as session:
        for url in video_urls:
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    video_data = BytesIO()
                    while True:
                        chunk = await response.content.read(8192)
                        if not chunk:
                            break
                        video_data.write(chunk)
                    video_data.seek(0)
                    data = {"video_url": url, "data": video_data}
                    await queue.put(data)
                    logging.info(f"Producer {producer_id} produced video with URL {url}")
            except Exception as e:
                logging.error(f"Producer {producer_id} failed to fetch {url}: {e}")





def read_frames_from_bytes(video_bytes):
    # Create an in-memory bytes buffer (restore BytesIO)
    video_buffer = io.BytesIO(video_bytes)

    # Open the video using PyAV
    container = av.open(video_buffer)

    frames = []

    # Enable multi-threaded decoding for performance boost
    stream = container.streams.video[0]
    stream.thread_type = 'AUTO'  # Use multi-threaded decoding

    for frame in container.decode(stream):
        # Convert AVFrame to numpy array (in BGR format)
        img = frame.to_ndarray(format='bgr24')
        frames.append(img)

    return frames





def sync_video_urls(pickle_path, csv_file_name):
    """
    Synchronizes video URLs between two CSV files.

    Loads video URLs from the trimmed CSV file, exports data to a new CSV file,
    and updates the video URLs by removing common elements.

    Args:
        csv_file_name (str): Name of the trimmed CSV file.
        saved_data_csv_name (str): Name of the saved data CSV file.

    Returns:
        list: Updated list of video URLs.
    """
    # Load video URLs from trimmed CSV
    df = pd.read_csv(csv_file_name)
    video_urls1 = df["Video_URL"].tolist()
    loaded_data = load_data_pkl(pickle_path)
    
    if loaded_data is not None and len(loaded_data) > 0:
        video_urls2 = [i["url"] for i in loaded_data]
    else:
        video_urls2 = []
        
    print(video_urls2)
    print("=====================================================")
    print("Excluded files")
    video_urls = remove_common_elements(video_urls1, video_urls2)
    return video_urls



def load_data_pkl(file_path):
    """
    Load data from a file in pickle format.

    Args:
        file_path (str): The path to the file.

    Returns:
        list: A list of dictionaries.
    """
    if os.path.exists(file_path):
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        
        return data
    else:
        return None
    

def split_video_urls(video_urls, num_producers):
    chunk_size = len(video_urls) // num_producers
    return [video_urls[i:i + chunk_size] for i in range(0, len(video_urls), chunk_size)]







def remove_common_elements(list_a, list_b):
    """
    Removes elements from list_a that exist in list_b.

    Args:
        list_a (list): The list from which elements will be removed.
        list_b (list): The list containing elements to be removed.

    Returns:
        list: A new list with common elements removed.
    """
    # Convert list_b to a set for efficient lookups
    set_b = set(list_b)
    
    # Use a list comprehension to create a new list with common elements removed
    return [element for element in list_a if element not in set_b]






 
    
    