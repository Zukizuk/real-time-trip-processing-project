import json
import boto3
import base64
from datetime import datetime
from decimal import Decimal

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('NSP_Bolt_Trips')

# Initialize Lambda client for invoking the aggregator
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
                # Trip start data isn't in the database yet - log the error and skip
                # Later archive this to S3 for further analysis
                print(f"Error: Trip end received for unknown trip_id: {trip_id}. Skipping.")
                continue
            
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
                ':rate_code': Decimal(trip_end_data['rate_code']),
                ':passenger_count': Decimal(trip_end_data['passenger_count']),
                ':trip_distance': Decimal(trip_end_data['trip_distance']),
                ':fare_amount': Decimal(trip_end_data['fare_amount']),
                ':tip_amount': Decimal(trip_end_data['tip_amount']),
                ':payment_type': Decimal(trip_end_data['payment_type']),
                ':trip_type': Decimal(trip_end_data['trip_type']),
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
            
            # Trigger the aggregation Lambda for completed trips
            trigger_aggregation(completion_date, Decimal(trip_end_data['fare_amount']))
                
        except Exception as e:
            print(f"Error processing trip end for trip_id: {trip_id}")
            print(f"Error: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Trip end events processed successfully')
    }

def trigger_aggregation(date, fare_amount):
    pass