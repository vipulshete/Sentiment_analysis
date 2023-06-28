from flask import Flask, render_template, request
from google.cloud import storage
import datetime
import random
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit_review', methods=['POST'])
def submit_review():
    review = request.form['review']

    # Generate the file name based on the current date
    now = datetime.datetime.now()
    file_name = now.strftime("%Y-%m-%d")

    bucket_name = "restaurant_review"

    # Generate the folder name based on the current date
    now = datetime.datetime.now()
    folder_name = now.strftime("%Y-%m-%d-%H-%M-%S")


    # Generate the file name based on the current time
    
    # Generate a random 6-digit number
    random_number = random.randint(100000, 999999)
    file_name = f"review_{random_number}"

    storage_client = storage.Client.from_service_account_json(json_credentials_path='sentiment-analysis.json')

    # Save the review to a text file
    bucket = storage_client.bucket(bucket_name)  
    blob = bucket.blob(f'{folder_name}/{file_name}')
    blob.upload_from_string(review, content_type='text/plain')

    return 'Review submitted successfully.'

if __name__ == '__main__':
    app.run()
