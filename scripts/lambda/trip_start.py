import json
import boto3
import base64
from datetime import datetime
from decimal import Decimal

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('trip-table')

def lambda_handler(event, context):
    # Process each record from Kinesis
    for record in event['Records']:
        # Decode and parse the data
        payload = base64.b64decode(record['kinesis']['data'])
        trip_data = json.loads(payload)
        
        # Extract trip_id and other fields
        trip_id = trip_data['trip_id']
        
        try:
            # Prepare item for DynamoDB
            item = {
                'trip_id': trip_id,
                'trip_status': 'started',
                'pickup_datetime': trip_data['pickup_datetime'],
                'pickup_location_id': int(trip_data['pickup_location_id']),
                'dropoff_location_id': int(trip_data['dropoff_location_id']),
                'vendor_id': int(trip_data['vendor_id']),
                'estimated_dropoff_datetime': trip_data['estimated_dropoff_datetime'],
                'estimated_fare_amount': Decimal(trip_data['estimated_fare_amount']),
                'creation_timestamp': datetime.now().isoformat()
            }
            
            # Write to DynamoDB
            table.put_item(Item=item)
            print(f"Successfully processed trip start for trip_id: {trip_id}")
            
        except Exception as e:
            print(f"Error processing trip start for trip_id: {trip_id}")
            print(f"Error: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Trip start events processed successfully')
    }