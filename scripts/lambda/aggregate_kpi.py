import json
import boto3
import os
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr

# Initialize clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
table = dynamodb.Table('trip-table')

# Configuration
S3_BUCKET = 'nsp-bolt-metrics-zuki'
S3_PREFIX = 'daily'

def lambda_handler(event, context):
    # Determine which date to process
    target_date = event.get('date')
    if not target_date:
        # Default to yesterday if no date provided
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"Starting aggregation for date: {target_date}")
    
    # Aggregate metrics for the target date
    metrics = aggregate_daily_metrics(target_date)
    
    # Save to S3
    s3_key = f"{S3_PREFIX}/{target_date}/metrics.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(metrics, indent=2),
        ContentType='application/json'
    )
    
    print(f"Successfully saved metrics to S3: {s3_key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Metrics updated for {target_date}')
    }

def aggregate_daily_metrics(date):
    """Calculate metrics for all completed trips on a given date"""
    try:
        # Query DynamoDB for all completed trips on this date
        response = table.query(
    IndexName='CompletionDateStatusIndex',
    KeyConditionExpression=Key('completion_date').eq(date) & Key('trip_status').eq('completed')
)

        
        items = response.get('Items', [])
        
        # Continue querying if we have more items (pagination)
        while 'LastEvaluatedKey' in response:
            response = table.query(
                IndexName='CompletionDateIndex',
                KeyConditionExpression=Key('completion_date').eq(date),
                FilterExpression=Attr('trip_status').eq('completed'),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        # Calculate metrics
        if not items:
            # No completed trips for this date
            metrics = {
                'date': date,
                'metrics': {
                    'total_fare': 0,
                    'count_trips': 0,
                    'average_fare': 0,
                    'max_fare': 0,
                    'min_fare': 0
                },
                'generated_at': datetime.now().isoformat(),
                'trip_count': 0
            }
        else:
            fare_amounts = [float(item['fare_amount']) for item in items]
            total_fare = sum(fare_amounts)
            count_trips = len(items)
            
            metrics = {
                'date': date,
                'metrics': {
                    'total_fare': round(total_fare, 2),
                    'count_trips': count_trips,
                    'average_fare': round(total_fare / count_trips, 2) if count_trips > 0 else 0,
                    'max_fare': round(max(fare_amounts), 2) if fare_amounts else 0,
                    'min_fare': round(min(fare_amounts), 2) if fare_amounts else 0
                },
                'generated_at': datetime.now().isoformat(),
                'trip_count': count_trips
            }
        
        print(f"Successfully aggregated metrics for date: {date}, found {len(items)} completed trips")
        return metrics
        
    except Exception as e:
        print(f"Error aggregating daily metrics: {str(e)}")
        raise