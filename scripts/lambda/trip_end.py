import json
import boto3
import base64
from datetime import datetime
from decimal import Decimal  # Import Decimal

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('trip-table')

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    # Process each record from Kinesis
    for record in event['Records']:
        # Decode and parse the data
        payload = base64.b64decode(record['kinesis']['data'])
        trip_end_data = json.loads(payload)
        
        # Extract trip_id
        trip_id = trip_end_data['trip_id']
        
        try:
            # First, get the existing trip record
            response = table.get_item(Key={'trip_id': trip_id})
            
            if 'Item' not in response:
                # This is a trip end without a corresponding start
                # Consider this an edge case - create a new record with status "partial"
                print(f"Warning: Trip end received for unknown trip_id: {trip_id}")
                item = {
                    'trip_id': trip_id,
                    'trip_status': 'partial',
                    # Add trip end data
                    'dropoff_datetime': trip_end_data['dropoff_datetime'],
                    'rate_code': Decimal(trip_end_data['rate_code']),  # Use Decimal
                    'passenger_count': Decimal(trip_end_data['passenger_count']),  # Use Decimal
                    'trip_distance': Decimal(trip_end_data['trip_distance']),  # Use Decimal
                    'fare_amount': Decimal(trip_end_data['fare_amount']),  # Use Decimal
                    'tip_amount': Decimal(trip_end_data['tip_amount']),  # Use Decimal
                    'payment_type': Decimal(trip_end_data['payment_type']),  # Use Decimal
                    'trip_type': Decimal(trip_end_data['trip_type']),  # Use Decimal
                    'update_timestamp': datetime.now().isoformat()
                }
                table.put_item(Item=item)
            else:
                # This is an update to an existing trip record
                existing_item = response['Item']
                
                # Extract the date from dropoff_datetime for aggregation
                dropoff_datetime = trip_end_data['dropoff_datetime']
                completion_date = dropoff_datetime.split()[0]  # Format: YYYY-MM-DD
                
                # Update the trip record
                update_expression = """
                    SET trip_status = :status,
                        dropoff_datetime = :dropoff_datetime,
                        rate_code = :rate_code,
                        passenger_count = :passenger_count,
                        trip_distance = :trip_distance,
                        fare_amount = :fare_amount,
                        tip_amount = :tip_amount,
                        payment_type = :payment_type,
                        trip_type = :trip_type,
                        completion_date = :completion_date,
                        update_timestamp = :update_timestamp
                """
                
                expression_values = {
                    ':status': 'completed',
                    ':dropoff_datetime': trip_end_data['dropoff_datetime'],
                    ':rate_code': Decimal(trip_end_data['rate_code']),  # Use Decimal
                    ':passenger_count': Decimal(trip_end_data['passenger_count']),  # Use Decimal
                    ':trip_distance': Decimal(trip_end_data['trip_distance']),  # Use Decimal
                    ':fare_amount': Decimal(trip_end_data['fare_amount']),  # Use Decimal
                    ':tip_amount': Decimal(trip_end_data['tip_amount']),  # Use Decimal
                    ':payment_type': Decimal(trip_end_data['payment_type']),  # Use Decimal
                    ':trip_type': Decimal(trip_end_data['trip_type']),  # Use Decimal
                    ':completion_date': completion_date,
                    ':update_timestamp': datetime.now().isoformat()
                }
                
                # Update the DynamoDB record
                table.update_item(
                    Key={'trip_id': trip_id},
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_values
                )
                
                print(f"Successfully updated trip record for trip_id: {trip_id}")
                
                # Trigger the aggregation Lambda only for completed trips
                if existing_item.get('trip_status') != 'completed':
                    # This is the first time this trip is being marked as completed
                    trigger_aggregation(completion_date, trip_end_data['fare_amount'])
                
        except Exception as e:
            print(f"Error processing trip end for trip_id: {trip_id}")
            print(f"Error: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Trip end events processed successfully')
    }

def trigger_aggregation(date, fare_amount):
    pass
