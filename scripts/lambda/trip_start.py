import json
import boto3
import base64
from datetime import datetime

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
            # Check if there's already a record for this trip_id (possible trip_end arrived first)
            response = table.get_item(Key={'trip_id': trip_id})
            
            if 'Item' in response:
                # A record already exists (likely a "partial" record from trip_end)
                existing_item = response['Item']
                
                if existing_item.get('trip_status') == 'partial':
                    # This is a trip_start arriving after trip_end
                    # Update the existing record with trip_start data and mark as completed
                    
                    update_expression = """
                        SET trip_status = :status,
                            pickup_datetime = :pickup_datetime,
                            pickup_location_id = :pickup_location_id,
                            dropoff_location_id = :dropoff_location_id,
                            vendor_id = :vendor_id,
                            estimated_dropoff_datetime = :estimated_dropoff_datetime,
                            estimated_fare_amount = :estimated_fare_amount,
                            update_timestamp = :update_timestamp
                    """
                    
                    expression_values = {
                        ':status': 'completed',  # Now we have both start and end data
                        ':pickup_datetime': trip_data['pickup_datetime'],
                        ':pickup_location_id': trip_data['pickup_location_id'],
                        ':dropoff_location_id': trip_data['dropoff_location_id'],
                        ':vendor_id': trip_data['vendor_id'],
                        ':estimated_dropoff_datetime': trip_data['estimated_dropoff_datetime'],
                        ':estimated_fare_amount': trip_data['estimated_fare_amount'],
                        ':update_timestamp': datetime.now().isoformat()
                    }
                    
                    # Update the DynamoDB record
                    table.update_item(
                        Key={'trip_id': trip_id},
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=expression_values
                    )
                    
                    print(f"Trip start received for trip_id: {trip_id} with existing end data. Marked as completed.")
                    
                    # Since this trip is now complete, trigger the aggregation process
                    # Extract completion date from the existing dropoff_datetime
                    completion_date = existing_item['dropoff_datetime'].split()[0]
                    
                    # Trigger aggregation
                    trigger_aggregation(completion_date, float(existing_item['fare_amount']))
                else:
                    # This is unexpected - a record exists that's not "partial"
                    # Could be a duplicate trip start event, just log and skip
                    print(f"Warning: Received duplicate trip start for trip_id: {trip_id}, status: {existing_item.get('trip_status')}")
            else:
                # No existing record, this is the normal flow - create a new one
                item = {
                    'trip_id': trip_id,
                    'trip_status': 'started',
                    'pickup_datetime': trip_data['pickup_datetime'],
                    'pickup_location_id': trip_data['pickup_location_id'],
                    'dropoff_location_id': trip_data['dropoff_location_id'],
                    'vendor_id': trip_data['vendor_id'],
                    'estimated_dropoff_datetime': trip_data['estimated_dropoff_datetime'],
                    'estimated_fare_amount': trip_data['estimated_fare_amount'],
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

# Add this function to the Trip Start Processor
def trigger_aggregation(date, fare_amount):
  pass