from google.cloud import storage


class CreateBucketHelper:

    def __init__(self, bucket_name) -> None:
        self.bucket_name = bucket_name

    def create_bucket(self):
        """Creates a new bucket."""
        # bucket_name = "your-new-bucket-name"

        storage_client = storage.Client()

        bucket = storage_client.create_bucket(self.bucket_name)

        print("Bucket {} created".format(bucket.name))

    def upload_blob(self, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        # The path to your file to upload
        # source_file_name = "local/path/to/file"
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(
            "File {} uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )
