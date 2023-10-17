from urllib.parse import urlparse
from datetime import datetime
import concurrent.futures
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

    def get_files_list(self, path: str) -> list:
        result = []
        for dirpath, dirnames, filenames in os.walk(path):
            result.extend([os.path.join(dirpath, filename) for filename in filenames])
        return result

    def parse_file_path(self, path: str) -> dict:
        parsed_path = os.path.slit(path)
        return {"dirpath": parsed_path[0], "filename": parsed_path[1]}

    def read_file_to_array(self, filepath: str) -> list:
        with open(filepath, 'r') as f:
            return f.readlines()

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

    def generate_collection_metadata_summary(self, collection_lookup: dict) -> dict:
        """
        Generate collection metadata summary from collection lookup dict
        :param collection_lookup: collection lookup dictionary containing all files metadata
        :type collection_lookup: dict
        :return: collection_metadata summary dict
        :rtype: dict
        """
        # Initialize vars
        start, end = ["2100-01-01T00:00:00Z",  "1900-01-15T00:00:00Z"]
        north, south, east, west = [-90.0, 90.0, -180.0, 180.0]
        size = 0
        file_count = len(collection_lookup)

        for granule in collection_lookup.values():
            # ISO8601 Format is comparable directly as strings, so no
            # conversion to datetime object is necessary:
            # https://fits.gsfc.nasa.gov/iso-time.html
            start = min(start, granule['start'])
            end = max(end, granule['end'])
            north = max(north, float(granule['north']))
            south = min(south, float(granule['south']))
            east = max(east, float(granule['east']))
            west = min(west, float(granule['west']))
            size += float(granule['sizeMB'])

        return {
            "start": start,
            "end": end,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "sizeMB": size,
            "num_of_files": file_count
        }

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

    def process_file(self, filepath):
        """
        Process metadata for file
        :param s3uri: s3uri of file to process
        :type s3uri: string
        """
        try:
            # Download file object stream and size
            file_lines = self.read_file_to_array(filepath)
            # Extract temporal and spatial metadata
            initial_metadata = self.process(os.path.basename(filepath), file_lines)
            metadata = self.validate_spatial_coordinates(initial_metadata)
            metadata["sizeMB"] = 1E-6 * os.path.getsize(filepath)
            # Format time outputs
            metadata = self.format_dict_times(metadata)
            for elem in ["north", "south", "east", "west"]:
                metadata[elem] = str(round(metadata[elem], 3))
            metadata["sizeMB"] = round(metadata["sizeMB"], 2)
            self.collection_lookup[os.path.basename(filepath)] = metadata
        except Exception as e:
            print(f"Problem processing {os.path.basename(filepath)}:\n{e}\n")

    def process_collection(self, short_name, provider_path):
        self.collection_lookup = {}
        files_list = self.get_files_list(provider_path)
        with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
            # Start the process operations and mark each future with its uri
            future_to_uri = {executor.submit(self.process_file, filepath): filepath for filepath in files_list}
            for future in concurrent.futures.as_completed(future_to_uri):
                uri = future_to_uri[future]
                try:
                    data = future.result()
                except Exception as e:
                    print(f'{uri} generated an exception: {e}')
        # for filepath in files_list:
        #     self.process_file(filepath)

        collection_metadata_summary = self.generate_collection_metadata_summary(self.collection_lookup)

        # Write lookup and summary to zip
        self.write_to_lookup(short_name, self.collection_lookup,
                             collection_metadata_summary)
