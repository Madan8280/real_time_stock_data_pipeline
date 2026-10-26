import boto3
s3 = boto3.client("s3",endpoint_url="http://localhost:9000",
aws_access_key_id="minioadmin",
aws_secret_access_key="minioadmin")


s3.put_object(Bucket='raw', Key="test.txt", Body=b"hello minio"
              )

print(s3.list_objects_v2(Bucket='raw'))