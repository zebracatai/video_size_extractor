import cv2

def get_video_dimensions(video_path):
    # Open the video file
    cap = cv2.VideoCapture(video_path)
    
    # Check if video opened successfully
    if not cap.isOpened():
        print("Error: Could not open video.")
        return
    
    # Get the width and height of the video
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    
    # Print the width and height
    print(f"Video width: {width} pixels")
    print(f"Video height: {height} pixels")
    
    # Release the video capture object
    cap.release()

if __name__ == "__main__":
    # Example usage
    video_path = "11240584_2.mp4"
    get_video_dimensions(video_path)