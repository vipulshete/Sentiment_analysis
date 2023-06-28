import functions_framework
from google.cloud import storage
import json
from google.cloud import language_v1


def sentiment(review):
    # Instantiates a client
    client = language_v1.LanguageServiceClient()

    # The text to analyzeda
    document = language_v1.types.Document(
        content=review, type_=language_v1.types.Document.Type.PLAIN_TEXT
    )

    # Detects the sentiment of the review
    sentiment = client.analyze_sentiment(
        request={"document": document}
    ).document_sentiment

    sentiment_review = {"review" : review, "Sentiment_score" : sentiment.score, "Sentiment_magnitude" : sentiment.magnitude}
    
    return sentiment_review

def write_json_to_storage(data, bucket_name, file_name):
    # Instantiate a client to interact with Cloud Storage
    client = storage.Client()
    
    # Get the bucket
    bucket = client.get_bucket(bucket_name)
    
    # Create a blob with the specified file name
    blob = bucket.blob(file_name)

    # Convert the data to JSON string
    json_data = json.dumps(data)
    
    # Upload the JSON string as the content of the blob
    blob.upload_from_string(json_data, content_type='application/json')


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def review_sentiment_nlp(cloud_event):
    data = cloud_event.data

    bucket_name = data["bucket"]
    object_name = data["name"]
   
    # Instantiate a client to interact with Cloud Storage
    client = storage.Client()
    
    # Get the bucket containing the object
    bucket = client.get_bucket(bucket_name)
    
    # Get the object
    blob = bucket.blob(object_name)

    # Download the blob as a string
    review = blob.download_as_text() 

    sentiment_reviews = sentiment(review)

    destination_bucket_name = "reviews_sentiment"
    write_json_to_storage(sentiment_reviews, destination_bucket_name, object_name)

    return sentiment_reviews