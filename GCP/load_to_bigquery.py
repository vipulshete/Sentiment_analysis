import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import json

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def insert_into_bigquery(cloud_event):
    data = cloud_event.data

    bucket_name = data["bucket"]
    file_name = data["name"]

    # Set up BigQuery client
    bq_client = bigquery.Client()
    
    # Set up Cloud Storage client
    storage_client = storage.Client()
    
    # Get the file from Cloud Storage
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Download the file content as a string
    file_content = blob.download_as_text()
    
    # Parse the file content as JSON
    review_sentiment = json.loads(file_content)
    
    # Define the BigQuery table and dataset details
    dataset_id = 'Sentiment_Analysis'
    table_id = 'reviews_sentiments'
    
    # Construct the BigQuery table reference
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    
    # Create a BigQuery row to insert
    row_to_insert = {
        'Sentiment_magnitude': review_sentiment['Sentiment_magnitude'],
        'Sentiment_score': review_sentiment['Sentiment_score'],
        'review': review_sentiment['review']
    }
    
    # Insert the row into BigQuery table
    errors = bq_client.insert_rows_json(table_ref, [row_to_insert])
    
    if errors:
        print(f"Encountered errors while inserting row: {errors}")
    else:
        print("Row inserted successfully!")
    

   
