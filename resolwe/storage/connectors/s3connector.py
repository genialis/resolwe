"""Amazon S3 storage connector."""

import json
import logging
import mimetypes
import os
import threading
import uuid
from pathlib import Path
from typing import Dict, Optional

import boto3
import botocore

from .baseconnector import (
    BaseStorageConnector,
    ConnectorType,
    validate_url,
    validate_urls,
)

logger = logging.getLogger(__name__)


class AwsS3Connector(BaseStorageConnector):
    """Amazon S3 storage connector."""

    REQUIRED_SETTINGS = ["bucket"]
    CONNECTOR_TYPE = ConnectorType.S3

    def __init__(self, config: dict, name: str):
        """Connector initialization."""
        super().__init__(config, name)
        self.bucket_name = config["bucket"]
        self.supported_hash = ["awss3etag"]
        self.refresh_hash_after_transfer = ["awss3etag"]
        self.hash_property = {"awss3etag": "ETag"}
        self.multipart_threshold = self.config.get(
            "multipart_threshold", self.CHUNK_SIZE
        )
        self.multipart_chunksize = self.config.get(
            "multipart_chunksize", self.CHUNK_SIZE
        )
        self.use_threads = True

        # Ensured by TLS protocol used for transport.
        self.get_ensures_data_integrity = True
        # Boto library computes MD5 checksums for every chunk of data it
        # transmits to the bucket and compares it to the actual md5 of the
        # data received by the server.
        self.put_ensures_data_integrity = True
        self._init_lock = threading.Lock()
        self._initialized = False

    def duplicate(self):
        """Duplicate existing connector.

        The low-level rountines of boto3 are thread safe. As long as we do not
        use session and resources in threads we should be safe, so no clonning
        is necessary.
        """
        return self

    def _initialize(self):
        """Initializaton."""
        logger.debug("Initializing S3 connector")
        session_kwargs = {"region_name": self.config.get("region_name")}
        # Credentials may be given explicitely in config. When they are not
        # the system credentials are used instead.
        if "credentials" in self.config:
            logger.debug("Reading credentials from %s.", self.config["credentials"])
            with open(self.config["credentials"]) as f:
                settings = json.load(f)
                session_kwargs["aws_access_key_id"] = settings["AccessKeyId"]
                session_kwargs["aws_secret_access_key"] = settings["SecretAccessKey"]

        self.session = boto3.Session(**session_kwargs)
        self.original_session = self.session

        # Assume role if needed.
        if "role_arn" in self.config:
            logger.debug("Assuming role %s.", self.config["role_arn"])
            # This has to be unique identifier and can be seen in logs.
            self._session_name = "s3connector"
            session_credentials = (
                botocore.credentials.RefreshableCredentials.create_from_metadata(
                    metadata=self._refresh_credentials_metadata(),
                    refresh_using=self._refresh_credentials_metadata,
                    method="sts-assume-role",
                )
            )
            botocore_session = botocore.session.get_session()
            botocore_session._credentials = session_credentials
            if "region_name" in self.config:
                botocore_session.set_config_variable(
                    "region", self.config["region_name"]
                )
            # Replace the session with the new one.
            self.session = boto3.Session(botocore_session=botocore_session)

        self.client = self.session.client(
            "s3",
            config=botocore.client.Config(
                signature_version="s3v4", max_pool_connections=50
            ),
        )

    def _refresh_credentials_metadata(self, duration: int = 3600):
        """Prepare metadata for refreshing credentials when assuming role."""
        logger.debug(
            "Creating metadata for refreshable credentials for role '%s', duration %d.",
            self.config["role_arn"],
            duration,
        )
        params = {
            "RoleArn": self.config["role_arn"],
            "RoleSessionName": str(uuid.uuid4()),
            "DurationSeconds": duration,
        }
        response = (
            self.original_session.client("sts").assume_role(**params).get("Credentials")
        )
        return {
            "access_key": response.get("AccessKeyId"),
            "secret_key": response.get("SecretAccessKey"),
            "token": response.get("SessionToken"),
            "expiry_time": response.get("Expiration").isoformat(),
        }

    def __getattr__(self, name):
        """Lazy initialize some attributes."""
        requires_initialization = [
            "client",
            "awss3",
            "sts",
            "session",
            "original_session",
        ]
        if name not in requires_initialization:
            raise AttributeError()
        with self._init_lock:
            if not self._initialized:
                self._initialize()
                self._initialized = True
        return getattr(self, name)

    @validate_url
    def push(self, stream, url, chunk_size=BaseStorageConnector.CHUNK_SIZE, hashes={}):
        """Push data from the stream to the given URL."""
        url = os.fspath(url)
        mime_type = mimetypes.guess_type(url)[0]
        extra_args = {} if mime_type is None else {"ContentType": mime_type}
        extra_args["Metadata"] = {"_upload_chunk_size": str(chunk_size)}
        extra_args["Metadata"].update(hashes)
        self.client.upload_fileobj(
            stream,
            self.bucket_name,
            url,
            Config=self._get_transfer_config(chunk_size),
            ExtraArgs=extra_args,
        )

    def multipart_push_start(self, url, size=None):
        """Start a multipart upload.

        :returns: the upload id.
        """
        url = os.fspath(url)
        mime_type = mimetypes.guess_type(url)[0]
        upload_args = {"Bucket": self.bucket_name, "Key": url}
        if mime_type is not None:
            upload_args["ContentType"] = mime_type
        response = self.client.create_multipart_upload(**upload_args)
        return response["UploadId"]

    def multipart_push(self, upload_id, url, part_number, chunk_size, data, md5=None):
        """Upload a single part of multipart upload."""
        result = self.client.upload_part(
            Body=data,
            Bucket=self.bucket_name,
            ContentMD5=md5,
            Key=os.fspath(url),
            PartNumber=part_number,
            UploadId=upload_id,
        )
        return {"ETag": result["ETag"], "PartNumber": part_number}

    def multipart_push_complete(self, upload_id, url, completed_chunks):
        """Complete the multipart push."""
        return self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=os.fspath(url),
            UploadId=upload_id,
            MultipartUpload={"Parts": completed_chunks},
        )

    def multipart_push_abort(self, upload_id, url):
        """Abort multiport upload."""
        return self.client.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=os.fspath(url),
            UploadId=upload_id,
        )

    @validate_urls
    @validate_url
    def delete(self, url, urls):
        """Remove objects."""
        # At most 1000 objects can be deleted at the same time.
        max_chunk = 1000
        for i in range(0, len(urls), max_chunk):
            next_chunk = urls[i : i + max_chunk]
            objects = [
                {"Key": os.fspath(self.base_path / url / delete_url)}
                for delete_url in next_chunk
            ]
            self.client.delete_objects(
                Bucket=self.bucket_name, Delete={"Objects": objects, "Quiet": True}
            )

    @validate_url
    def get(self, url, stream, chunk_size=BaseStorageConnector.CHUNK_SIZE):
        """Get data from the given URL and write it into the given stream."""
        chunk_size = max(chunk_size, self.multipart_threshold)
        self.client.download_fileobj(
            self.bucket_name,
            os.fspath(url),
            stream,
            Config=self._get_transfer_config(chunk_size),
        )

    def _get_transfer_config(self, chunk_size=BaseStorageConnector.CHUNK_SIZE):
        """Get transfer config object."""
        chunk_size = max(chunk_size, self.multipart_threshold)
        return boto3.s3.transfer.TransferConfig(
            multipart_threshold=self.multipart_threshold,
            multipart_chunksize=chunk_size,
            use_threads=self.use_threads,
        )

    @validate_url
    def get_object_list(self, url):
        """Get a list of objects stored bellow the given URL."""
        url = os.path.join(url, "")
        paginator = self.client.get_paginator("list_objects_v2")
        kwargs = {"Bucket": self.bucket_name, "Prefix": url}
        ret = []
        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break
            for obj in contents:
                ret.append(Path(obj["Key"]).relative_to(url).as_posix())
        return ret

    @validate_url
    def get_hash(self, url, hash_type):
        """Get the hash of the given type for the given object."""
        try:
            response = self.client.head_object(
                Bucket=self.bucket_name, Key=os.fspath(url)
            )
            if hash_type in self.hash_property:
                return response[self.hash_property[hash_type]].strip('"')
            else:
                return response["Metadata"].get(hash_type)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "404":
                return None
            else:
                # Something else has gone wrong.
                raise

    @validate_url
    def get_hashes(self, url, hash_types):
        """Get the hash of the given type for the given object."""
        hashes = dict()
        try:
            response = self.client.head_object(
                Bucket=self.bucket_name, Key=os.fspath(url)
            )
            for hash_type in hash_types:
                if hash_type in self.hash_property:
                    hashes[hash_type] = response[self.hash_property[hash_type]].strip(
                        '"'
                    )
                else:
                    hashes[hash_type] = response["Metadata"].get(hash_type)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "404":
                return None
            else:
                # Something else has gone wrong.
                raise
        return hashes

    @validate_url
    def exists(self, url):
        """Get if the object at the given URL exist."""
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=os.fspath(url))
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "404":
                return False
            else:
                # Something else has gone wrong.
                raise
        else:
            return True

    @validate_url
    def set_hashes(self, url, hashes):
        """Set the  hashes for the given object."""
        # Changing metadata on existing objects in S3 is annoyingly hard.
        # See
        # https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3.html
        # under managed copy for example.
        # If one uses copy_object method proposed by some solutions the e_tag
        # value on object can (and will) change. That causes error downloading
        # since hash check fails.
        url = os.fspath(url)
        head = self.client.head_object(Bucket=self.bucket_name, Key=url)
        content_type = head["ResponseMetadata"]["HTTPHeaders"]["content-type"]
        meta = head["Metadata"]
        chunk_size = int(meta["_upload_chunk_size"])
        hashes = {k: v for (k, v) in hashes.items() if k not in self.hash_property}
        meta.update(hashes)
        copy_source = {
            "Bucket": self.bucket_name,
            "Key": url,
        }
        self.client.copy(
            copy_source,
            self.bucket_name,
            url,
            ExtraArgs={
                "Metadata": meta,
                "MetadataDirective": "REPLACE",
                "ContentType": content_type,
            },
            Config=self._get_transfer_config(chunk_size),
        )

    @property
    def base_path(self):
        """Get a base path for this connector."""
        return Path("")

    @validate_url
    def url(self, url):
        """Return the URL to the given file in connector native notation.

        AWS native notation for file is S3 bucket is s3://bucket/file .
        """
        return f"s3://{self.bucket_name}/{url}"

    @validate_url
    def presigned_url(self, url, expiration=60, force_download=False):
        """Create a presigned URL.

        The URL is used to obtain temporary access to the object ar the
        given URL using only returned URL.

        :param expiration: expiration time of the link (in seconds), default
            is one minute.

        :param force_download: force download.

        :returns: URL that can be used to access object or None.
        """
        content_disposition = "attachment" if force_download else "inline"
        content_disposition += f';filename="{Path(url).name}"'
        response = None
        try:
            response = self.client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": self.bucket_name,
                    "Key": os.fspath(url),
                    "ResponseContentDisposition": content_disposition,
                },
                ExpiresIn=expiration,
            )
        except botocore.exceptions.ClientError:
            logger.exception("Error creating presigned URL")

        # The response contains the presigned URL
        return response

    def temporary_credentials(
        self, prefix: str, duration: int = 900, role_arn: Optional[str] = None, **kwargs
    ) -> Dict:
        """Get the temporary credentials for role_arn.

        The credentials are further limited to access only objects which names
        start with '/prefix/'.

        :attr role_arn: arn of the role to assume. If none is given it is taken
            from connector settings.

        :attr prefix: the string representing prefix.

        :attr duration: the duration of the credentials in seconds (defaults to 60).

        :raises KeyError: when no role_arn is gives as argument or connector
            setting.

        :returns: the dictionary with credentials, see
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html#STS.Client.assume_role
        for detailed description.
        """
        # Policy to add access only to the objects named '/prefix/*'.
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAllS3ActionsInUserFolder",
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:AbortMultipartUpload",
                        "s3:GetObject",
                    ],
                    "Resource": [f"arn:aws:s3:::{self.bucket_name}/{prefix}/*"],
                },
                {
                    "Sid": "AllowUserKMSUsage",
                    "Effect": "Allow",
                    "Action": ["kms:GenerateDataKey", "kms:Decrypt"],
                    "Resource": ["*"],
                },
            ],
        }
        aws_response = self.original_session.client("sts").assume_role(
            RoleArn=role_arn or self.config["role_arn"],
            RoleSessionName="upload-" + prefix,
            DurationSeconds=duration,
            Policy=json.dumps(policy),
        )
        return {
            "credentials": aws_response["Credentials"],
            "bucket_name": self.bucket_name,
            "region": self.config.get("region_name"),
            "prefix": prefix,
        }
