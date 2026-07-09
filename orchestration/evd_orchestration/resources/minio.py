import boto3
from dagster import ConfigurableResource


class MinIOResource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str = "evd"

    def get_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def list_keys(self, prefix: str) -> list[str]:
        client = self.get_client()
        paginator = client.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    objects.append(obj)
        objects.sort(key=lambda obj: obj["LastModified"])
        return [obj["Key"] for obj in objects]

    def move_to_processed(self, key: str, from_prefix: str, to_prefix: str) -> None:
        client = self.get_client()
        dest_key = to_prefix + key[len(from_prefix):]
        client.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": key},
            Key=dest_key,
        )
        client.delete_object(Bucket=self.bucket, Key=key)

    def list_prefixes(self, prefix: str) -> list[str]:
        """List immediate sub-prefixes ("folders") directly under `prefix`."""
        client = self.get_client()
        paginator = client.get_paginator("list_objects_v2")
        prefixes = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
            for common in page.get("CommonPrefixes", []):
                prefixes.append(common["Prefix"])
        return prefixes

    def ensure_prefix_marker(self, prefix: str) -> None:
        """Write a zero-byte object at `prefix` so it stays visible as a
        folder in MinIO/S3 listings even once all real files under it have
        been moved out."""
        client = self.get_client()
        client.put_object(Bucket=self.bucket, Key=prefix, Body=b"")
