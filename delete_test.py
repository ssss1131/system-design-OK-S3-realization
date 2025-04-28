import boto3

s3 = boto3.client(
  "s3",
  endpoint_url="http://localhost:5000",
  aws_access_key_id="x",
  aws_secret_access_key="x",
  config=boto3.session.Config(signature_version="s3v4"),
  region_name="us-east-1",
)

bucket = "b"
parent = "a/d"
name = "book.pdf"
full_key = f"{parent}/{name}"

s3.delete_object(Bucket=bucket, Key=full_key)
print("Deleted!")
