import time
import subprocess
import os
import threading
import boto3
import json

 

def save_rtsp_video(rtsp_url, output_directory, segment_duration, output_resolution, camera_number):
    # Generate unique filename based on timestamp
    current_time = time.strftime('%Y%m%d%H%M%S')
    current_date = time.strftime('%d-%m-%Y')
    output_path = os.path.join(output_directory, current_date)
    camera_directory = os.path.join(output_path, f'camera{camera_number}')
    os.makedirs(camera_directory, exist_ok=True)
    segment_filename = f'segment_{current_time}-%03d.mp4'
    segment_filepath = os.path.join(camera_directory, segment_filename)

    ffmpeg_command = f'ffmpeg -rtsp_transport tcp -i {rtsp_url} -vf "scale={output_resolution}" -c:v libx264 -preset fast -crf 23 -c:a copy -t {segment_duration} -f segment -segment_time {segment_duration} -reset_timestamps 1 {segment_filepath}'
    subprocess.run(ffmpeg_command, shell=True)

 

def upload_videos_to_s3(bucket_name, local_directory):
    s3_client = boto3.client('s3')

 

    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_key = os.path.join(s3_folder, relative_path)

 

            try:
                s3_client.upload_file(local_path, bucket_name, s3_key)
                print(f"Uploaded {local_path} to S3 bucket: {bucket_name} at key: {s3_key}")
            except Exception as e:
                print(f"Failed to upload {local_path} to S3 bucket: {bucket_name}")
                print(str(e))

 

# Load configuration from JSON file
with open('config.json') as config_file:
    config = json.load(config_file)
# Extract values from the configuration
rtsp_streams = config['rtsp_streams']
output_directory = config['output_directory']
output_resolution = config['output_resolution']
bucket_name = config['bucket_name']
s3_folder = config['s3_folder_name']
segment_duration = config['segment_duration']

 

while True:
    current_time = time.strftime('%H:%M')
    if current_time >= "03:00" and current_time < "04:00":
        threads = []
        for stream in rtsp_streams:
            rtsp_url = stream['url']
            camera_number = stream['camera_number']
            thread = threading.Thread(target=save_rtsp_video, args=(rtsp_url, output_directory, segment_duration, output_resolution, camera_number))
            thread.start()
            threads.append(thread)

        # Wait for all threads to finish
        for thread in threads:
            thread.join()