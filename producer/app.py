import csv
import boto3
import json
import streamlit as st
import time
from tqdm import tqdm

# Initialize the Kinesis client
kinesis_client = boto3.client('kinesis', region_name='eu-west-1')
stream_name = None  # Initialize as None, will be set in main()

def send_to_kinesis(data, stream_name=None):
    partition_key = data['trip_id']
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=partition_key
    )

def process_file(file_path, num_records=None, stream_name=None):
    total_records = 0
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        # Count total records if we need to process all
        if num_records is None:
            total_records = sum(1 for _ in csv_reader)
            file.seek(0)  # Reset file pointer
            next(csv_reader)  # Skip header again
        else:
            total_records = num_records

    progress_bar = st.progress(0.0)
    progress_text = st.empty()
    stop_button = st.empty()
    
    processed_count = 0
    should_stop = False
    
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if should_stop or (num_records is not None and processed_count >= num_records):
                break
                
            if stop_button.button("Stop Processing", key=f"stop_button_{processed_count}"):
                should_stop = True
                break
                
            send_to_kinesis(row, stream_name)
            processed_count += 1
            
            # Calculate progress as a value between 0.0 and 1.0
            progress = min(processed_count / total_records, 1.0)
            progress_bar.progress(progress)
            progress_text.text(f"Processed {processed_count} of {total_records} records")
            
            # Add a small delay to make the progress visible
            time.sleep(0.01)

def main():
    st.title("Kinesis Data Uploader")
    
    # Stream selection
    stream_name = st.selectbox(
        "Select Kinesis Stream",
        ["trip-start-stream", "trip-end-stream"]
    )
    
    # File selection
    file_path = st.selectbox(
        "Select CSV File",
        ["data/trip_start.csv", "data/trip_end.csv"]
    )
    
    # Record count selection
    record_option = st.radio(
        "Select number of records to process",
        ["All records", "Specific number"]
    )
    
    num_records = None
    if record_option == "Specific number":
        num_records = st.number_input(
            "Enter number of records to process",
            min_value=1,
            value=10
        )
    
    if st.button("Start Processing"):
        st.info(f"Processing data from {file_path} to {stream_name}")
        process_file(file_path, num_records, stream_name)
        st.success("Processing completed!")

if __name__ == "__main__":
    main()
