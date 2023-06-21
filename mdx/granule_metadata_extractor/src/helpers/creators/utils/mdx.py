from datetime import datetime
from urllib.parse import urlparse
import subprocess
import hashlib
import zipfile
import boto3
import json
import os


class S3URI:
    def __init__(self, bucket, prefix, filename):
        self.bucket = bucket
        self.prefix = prefix
        self.filename = filename


class MDX:
    """
    Class to wrap standard MDX processing
    """

    def __init__(self):
        """
        Used to define instance variables
        """
        self.in_AWS = 'us-west-2' in os.getenv('HOSTNAME', "")

    def parse_s3_uri(self, s3uri: str) -> dict:
        """
        Parse s3 uri to bucket/prefix
        :param s3uri: s3 uri to parse
        :type s3uri: str
        :return: dict containing bucket and prefix
        :rtype: dict
        """
        parsed_s3uri = urlparse(s3uri, allow_fragments=False)
        return S3URI(parsed_s3uri.netloc, parsed_s3uri.path.lstrip('/'),
                     os.path.basename(s3uri))

    def get_object_list(self, prefix: str, bucket: str = "ghrcw-private") -> list:
        """
        Get all objects at a provided bucket prefix
        :param prefix: prefix key defining where s3 objects are stored
        :type prefix: str
        """
        obj_list = list()
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=bucket, Prefix=f"{prefix.rstrip('/')}/")
        for page in page_iterator:
            for obj in page["Contents"]:
                obj_list.append(f"s3://{bucket}/{obj['Key']}")
        return obj_list

    def download_stream(self, bucket: str, prefix: str):
        """
        Download s3 object as file oject stream for processing
        :param bucket: s3 bucket of file object
        :type bucket: str
        :param prefix: prefix of file object
        :type prefix: str
        :return: file object stream
        :rtype: botocore.response.StreamingBody
        """
        s3 = boto3.client("s3")
        return s3.get_object(Bucket=bucket, Key=prefix)

    def get_checksum(self, file_obj_stream):
        """
        Get checksum from file object stream
        :param file_obj_stream: file object stream to hash
        :type file_obj_stream: botocore.response.StreamingBody
        :return: checksum of stream
        :rtype: 
        """
        # Checksum not currently included as part of lookup due to nature of
        # object stream. i.e. consumed when processed. If checksum is needed
        # in the future, downloading another stream or storing file object in
        # memory will be necessary
        md5 = hashlib.md5()
        for chunk in file_obj_stream.iter_chunks(chunk_size=(128 * md5.block_size)):
            md5.update(chunk)
        return md5.hexdigest()

    def update_collection_metadata_summary(self, collection_metadata: dict, granule_metadata: dict) -> dict:
        """
        Compare collection metadata summary to granule metadata and update if needed
        :param collection_metadata: collection metadata summary
        :type collection_metadata: dict
        :param granule_metadata: individual granule metadata to compare
        :type granule_metadata: dict
        :return: updated collection_metadata summary dict
        :rtype: dict
        """
        min_keys = ["south", "west", "start"]
        max_keys = ["north", "east", "end"]
        for key in min_keys:
            collection_metadata[key] = min(
                collection_metadata[key], granule_metadata[key])
        for key in max_keys:
            collection_metadata[key] = max(
                collection_metadata[key], granule_metadata[key])
        collection_metadata["sizeMB"] += granule_metadata["sizeMB"]
        collection_metadata["num_of_files"] += 1
        return collection_metadata

    def format_dict_times(self, input_dict: dict) -> dict:
        """
        Format dict's start/end times from datetime type
        :param input_dict: dictionary with start/end time attributes whose 
        type are datetime
        :type input_dict: dict
        :return: updated dict with start/end times in string format
        :rtype: dict
        """
        time_format = '%Y-%m-%dT%H:%M:%SZ'
        for elem in ["start", "end"]:
            input_dict[elem] = datetime.strftime(input_dict[elem], time_format)
        return input_dict

    def write_to_lookup(self, short_name: str, lookup_dict: dict, collection_summary: dict, path: str = '../'):
        """
        Write lookup file and collection summary to zip file
        """
        os.makedirs(path, exist_ok=True)
        zip_path = os.path.join(f"{path.rstrip('/')}", f"{short_name}.zip")

        with zipfile.ZipFile(zip_path, 'w') as zip_f:
            zip_f.writestr("lookup.json", json.dumps(lookup_dict))
            zip_f.writestr("summary.json", json.dumps(collection_summary))

    def process(self, filename: str, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        pass

    def validate_spatial_coordinates(self, metadata: dict) -> bool:
        """
        Checks whether or not spatial coordinates fall within standard bounds
        i.e. -90 < north/south < 90 & -180 < east/west < 180
        :param metadata: dictionary of granule metadata
        :type metadata: dict
        :return: bool describing whether or not coordinates are valid
        :rtype: bool
        """
        for elem in ["north", "south", "east", "west"]:
            # Elem value may be float-like types such as float32 which may
            # cause issue in json parsing, so we type cast here to python type
            metadata[elem] = float(metadata[elem])
            value = metadata[elem]
            if (value > 180 or value < -180) or \
               (elem in ["north", "south"] and (value < -90 or value > 90)):
                raise Exception(f"Invalid spatial coordinate system:\n"
                                    f"\tnorth: {metadata['north']}\n"
                                    f"\tsouth: {metadata['south']}\n"
                                    f"\teast: {metadata['east']}\n"
                                    f"\twest: {metadata['west']}\n")
        return metadata

    def process_collection(self, short_name, provider_path):
        collection_lookup = {}
        collection_metadata_summary = {
            "start": datetime(2100, 1, 1),
            "end": datetime(1900, 1, 1),
            "north": -90.0, "south": 90.0, "east": -180.0, "west": 180.0,
            "sizeMB": 0, "num_of_files": 0
        }
        # Get s3uri of all objects at s3 prefix
        s3uri_list = self.get_object_list(prefix=provider_path)
        # Only process first file if run outside AWS
        s3uri_list = s3uri_list if self.in_AWS else s3uri_list[:1]
        for s3uri in s3uri_list:
            try:
                uri = self.parse_s3_uri(s3uri)
                # Download file object stream and size
                response = self.download_stream(
                    bucket=uri.bucket, prefix=uri.prefix)
                file_obj_stream = response["Body"]
                # Extract temporal and spatial metadata
                initial_metadata = self.process(uri.filename, file_obj_stream)
                metadata = self.validate_spatial_coordinates(initial_metadata)
                metadata["sizeMB"] = 1E-6 * response["ContentLength"]
                # Compare to collection metadata summary
                collection_metadata_summary = self.update_collection_metadata_summary(
                    collection_metadata_summary, metadata)
                # Format time outputs
                metadata = self.format_dict_times(metadata)
                for elem in ["north", "south", "east", "west"]:
                    metadata[elem] = str(round(metadata[elem], 3))
                metadata["sizeMB"] = round(metadata["sizeMB"], 2)
                collection_lookup[uri.filename] = metadata
            except Exception as e:
                print(f"Problem processing {s3uri}:\n{e}\n")

        collection_metadata_summary = self.format_dict_times(
            collection_metadata_summary)

        # Write lookup and summary to zip
        self.write_to_lookup(short_name, collection_lookup,
                             collection_metadata_summary)

    def shutdown_ec2(self):
        """
        Shutdown ec2 (or local) after script finishes execution. Esepcially
        useful for cost savings when deployed to AWS
        """
        # This conditional to avoid accidental local shutdowns.
        if self.in_AWS:
            subprocess.call(["sudo", "shutdown", "1"])
