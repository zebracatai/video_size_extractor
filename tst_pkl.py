import pickle
import os

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

if __name__=="__main__":
    # Create a dictionary
    # data = {"url": "https://example.com", "embedding": [1, 2, 3]}

    # # Save the dictionary to a file
    # save_data_pkl(data, "data.pkl")

    # Load the data from the file
    loaded_data = load_data_pkl("pexel_new.pkl")
    video_urls2 = [ i["url"] for i in loaded_data]
    
    print(len(video_urls2))
    # for i in loaded_data:
    #     print(i)  # prints: [{"url": "https://example.com", "embedding": [1, 2, 3]}]

    # # Create another dictionary
    # new_data = {"url": "https://example2.com", "embedding": [4, 5, 6]}

    # # Save the new dictionary to the same file
    # save_data_pkl(new_data, "data.pkl")

    # # Load the updated data from the file
    # updated_data = load_data_pkl("data.pkl")

    # print(updated_data)  # prints: [{"url": "https://example.com", "embedding": [1, 2, 3]}, {"url": "https://example2.com", "embedding": [4, 5, 6]}]