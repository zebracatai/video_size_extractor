import asyncio
import aiohttp
import os
import logging
import time 
import ffmpeg
import tempfile
import io
from app_utils import (producer,
                       read_frames_from_bytes,
                       split_video_urls,
                       sync_video_urls,
                       save_data_pkl
                      )       
logging.basicConfig(level=logging.INFO)


# ================ Parameters ==============================
num_producers = 50
num_consumers = 50
queue_size = 200
CSV_FILE_NAME = "pexel_new.csv"
pickle_path = "pexel_new.pkl"



# ===== Consumer function which extracts embedding and saves it to Milvus ======
async def consumer(queue, consumer_id):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        while True:
            try:
                data = await queue.get()
                if data is None:
                    queue.task_done()
                    break
                url = data["video_url"]
                video_data = data["data"]

                video_data.seek(0)
                video_bytes = video_data.getvalue()
                
                temp_video_path = None  # Initialize to ensure cleanup
                try:
                    # Write video bytes to a temporary file
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_video_file:
                        temp_video_file.write(video_bytes)
                        temp_video_path = temp_video_file.name

                    # Extract video width and height using ffmpeg
                    probe = ffmpeg.probe(temp_video_path)
                    video_info = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
                    width = int(video_info['width'])
                    height = int(video_info['height'])

                    new_data = {
                        'url': url,
                        'width': width,
                        'height': height
                    }

                    save_data_pkl(new_data, pickle_path)
                    del new_data

                finally:
                    # Ensure the temporary file is removed
                    if temp_video_path and os.path.exists(temp_video_path):
                        os.remove(temp_video_path)

                await asyncio.sleep(0.1)
                queue.task_done()
                print(f"[INFO]Consumer {consumer_id} done")
            except Exception as e:
                logging.error(f"Consumer {consumer_id} encountered an error: {e}")
                queue.task_done()

# ===== Cunsumer function which extract embedding and save it to milvus ======
async def consumer1(queue, consumer_id):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        while True:
            try:
                data = await queue.get()
                if data is None:
                    queue.task_done()
                    break
                url = data["video_url"]
                video_data = data["data"]

                video_data.seek(0)
                video_bytes = video_data.getvalue()
                                # Write video bytes to a temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_video_file:
                    temp_video_file.write(video_bytes)
                    temp_video_path = temp_video_file.name

                # Extract video width and height using ffmpeg
                probe = ffmpeg.probe(temp_video_path)

                # Extract video width and height using ffmpeg

                video_info = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
                width = int(video_info['width'])
                height = int(video_info['height'])


                new_data = {
                    'url': url,
                    'width': width,
                    'height': height
                }

                save_data_pkl(new_data, pickle_path)
                del new_data

                await asyncio.sleep(0.1)
                queue.task_done()
                print(f"[INFO]Consumer {consumer_id} done")
            except Exception as e:
                logging.error(f"Consumer {consumer_id} encountered an error: {e}")
                queue.task_done()
                
                
                
# async def consumer2(queue, consumer_id):
#     async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
#         while True:
#             try:
#                 data = await queue.get()
#                 if data is None:
#                     queue.task_done()
#                     break
#                 url = data["video_url"]
#                 video_data = data["data"]
                
#                 video_data.seek(0)
#                 video_bytes = video_data.getvalue()

#                 # Extract frames
#                 extract_frame = time.time()
#                 # frames = read_frames_from_bytes(video_bytes)
#                 # del video_bytes
#                 # print(f"[INFO] Extract frames : {time.time() - extract_frame}")

#                 # inference_tik = time.time()
#                 # video_embedding = video_embedding_extractor(
#                 #     frames=frames, 
#                 #     model=intern_models[consumer_id], 
#                 #     config=config, 
#                 #     device=torch.device('cuda')
#                 # )
#                 # del frames

#                 new_data = {'url':url,
#                             'embedding':embeddings_array
#                             }
#                 # save_data_pkl(new_data, f"data_{consumer_id}.pkl")
#                 save_data_pkl(new_data, pickle_path)
#                 del embeddings_array
#                 del new_data

#                 await asyncio.sleep(0.1)
#                 queue.task_done()
#             except Exception as e:
#                 logging.error(f"Consumer {consumer_id} encountered an error: {e}")
#                 queue.task_done()



async def main():


    tik_1 = time.time()
    video_urls = sync_video_urls(pickle_path = pickle_path,
                                 csv_file_name=CSV_FILE_NAME)
    
    video_url_chunks = split_video_urls(video_urls, num_producers)
    queue = asyncio.Queue(maxsize=queue_size)
    
    producers = [asyncio.create_task(producer(queue, i, chunk)) for i, chunk in enumerate(video_url_chunks)]
    consumers = [asyncio.create_task(consumer(queue, i)) for i in range(num_consumers)]
    
    await asyncio.gather(*producers)
    
    # Send a sentinel value for each consumer to signal completion
    for _ in range(num_consumers):
        await queue.put(None)
    
    await asyncio.gather(*consumers)
    
    print(f"[INFO] Total time: {time.time() - tik_1}")

if __name__ == "__main__":
    asyncio.run(main())
