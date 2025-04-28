import boto3

s3 = boto3.client(
  "s3",
  endpoint_url="http://localhost:5000",
  aws_access_key_id="x", aws_secret_access_key="x",
  config=boto3.session.Config(signature_version="s3v4"),
  region_name="us-east-1",
)

bucket, parent, name = "b", "a/d", "book.pdf"
resp = s3.create_multipart_upload(Bucket=bucket, Key=f"{parent}/{name}")
uid = resp["UploadId"]

parts = []
with open("Java_Effektivnoe_Programmirovanie_3-E_Izd__2019_Dzhoshua_Blokh.pdf","rb") as f:
  i=1
  while True:
    chunk = f.read(5*1024*1024)
    if not chunk: break
    r = s3.upload_part(
      Bucket=bucket, Key=f"{parent}/{name}",
      UploadId=uid, PartNumber=i, Body=chunk
    )
    parts.append({"ETag":r["ETag"], "PartNumber":i})
    i+=1

s3.complete_multipart_upload(
  Bucket=bucket, Key=f"{parent}/{name}",
  UploadId=uid, MultipartUpload={"Parts":parts}
)
print("OK")
