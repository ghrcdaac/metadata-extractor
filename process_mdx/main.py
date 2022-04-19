from process_mdx.extract_metadata import ExtractMetadata
from cumulus_logger import CumulusLogger
from cumulus_process import Process, s3
from re import match
import logging
import boto3
import os
logger = CumulusLogger(name='MDX-Processing') if os.getenv('enable_logging', 'false').lower() == 'true' else logging


class MDX(Process):
    """
    Class to extract spatial and temporal metadata
    """

    def __init__(self, input, **kwargs):
        super().__init__(input, **kwargs)
        self.extractMetadata = ExtractMetadata(self.config)

    def upload_file(self, filename):
        info = self.get_publish_info(filename)
        if info is None:
            return filename
        try:
            uri = None
            if info.get('s3', None) is not None:
                uri = s3.upload(filename, info['s3'], extra={})
            return uri
        except Exception as e:
            logger.error("Error uploading file %s: %s" % (
                os.path.basename(os.path.basename(filename)), str(e)))

    def exclude_fetch(self):
        """
        This function is to exclude fetching the granules from specific shortnames
        :return:
        """
        return ["tcspecmwf", "gpmwrflpvex", "relampagolma", "goesrpltavirisng", "gpmvanlpvex",
                "gpmikalpvex", "gpmkorlpvex", "gpmkerlpvex", "gpmkumlpvex", "gpmseafluxicepop",
                "kakqimpacts", "kccximpacts", "kbgmimpacts", "kboximpacts", "kbufimpacts",
                "gpmkarx2ifld", "gpmkdmx2ifld", "gpmkdvn2ifld", "gpmkmpx2ifld"]

    def mutate_input(self, output_folder, input_file):
        """
        Point the input to local folder instead of S3
        :param input_file: The input file from S3
        :param output_folder: The local folder to ouput file to
        :return: path to the file in local machine
        """
        filename = os.path.basename(input_file)
        return [f"{output_folder.rstrip('/')}/{filename}"]

    def upload_output_files_to_staging(self, staging_file):
        """
        Uploads all self.output files to same location as input file
        :return: list of files uploaded to S3 (as well as input which should already be in S3)
        """
        uploaded_files = list()
        for _output in self.output:
            try:
                uploaded_files.append(s3.upload(_output, staging_file, extra={}))
            except Exception as e:
                logger.error("Error uploading file %s: %s" % (os.path.basename(os.path.basename(filename)), str(e)))
        return uploaded_files

    @property
    def input_keys(self):
        return {
            'input_key': r'^(.*)\.(nc|tsv|txt|gif|tar|zip|png|kml|dat|gz|pdf|docx|kmz|xlsx|eos|csv'
                         r'|hdf5|hdf|nc4|ict|xls|.*rest|h5|xlsx|1Hz|impacts_archive|\d{5}|ar2v)$'
        }

    @staticmethod
    def get_output_files(output_file_path):
        """
        """
        output_files = []
        if os.path.isfile(f"{output_file_path}.cmr.json"):
            output_files += [f"{output_file_path}.cmr.json"]
        return output_files

    def download_files(self, file_uri) -> dict:
        """
        Mimic self.fetch_all behavior with specified file uri
        :param file_uri: file uri to download
        :return: dict of input_keys with values which match downloadable regex
        """
        return_dict = dict()
        for key in self.input_keys:
            regex = self.input_keys.get(key, None)
            if regex is None:
                raise Exception('No files matching %s' % regex)
            outfiles = []
            m = match(regex, os.path.basename(file_uri))
            if m is not None:
                fname = s3.download(file_uri, path=self.path)
                self.downloads.append(fname)
                outfiles.append(fname)
            return_dict[key] = outfiles
        return return_dict

    @staticmethod
    def belong_to_protected_bucket(files, file_name):
        """
        This function check if the file needs to be in protected bucket

        """
        for _file in files:
            if _file['bucket'] == 'protected' and match(_file['regex'], file_name):
                return True
        return False

    def process(self):
        """
        Override the processing wrapper
        :return:
        """

        # def __init__(self):
        #     logging_level = logging.INFO if os.getenv('enable_logging', 'false').lower() == 'true' else logging.WARNING
        #     logger = CumulusLogger(name='MDX-Process', level=logging_level)
        logger.info('MDX processing started.')
        collection = self.config.get('collection')
        collection_name = collection.get('name')
        collection_version = collection.get('version')
        key = 'input_key'
        self.config['fileStagingDir'] = self.config.get('fileStagingDir',
                                                        f"{collection_name}__{collection_version}")
        excluded = collection_name in self.exclude_fetch()
        granules = self.input['granules']
        system_bucket = self.config.get('bucket')
        for granule in granules:
            files = []
            for file in granule['files']:
                file_uri = f"s3://{file['bucket']}/{file['key']}"
                file_name = os.path.basename(file['key'])
                if not self.belong_to_protected_bucket(files=collection['files'], file_name=file_name):
                    continue
                if excluded:
                    self.output.append(file_uri)
                    output = {key: self.mutate_input(self.path, file_uri)}
                else:
                    output = self.download_files(file_uri)
                # Assert we have inputs to process
                assert output[key], "fetched files list should not be empty"
                files_sizes = {}
                input_size = None
                checksum = None
                for output_file_path in output.get(key):
                    data = self.extractMetadata.extract_metadata(file_path=output_file_path,
                                                                 config=self.config,
                                                                 output_folder=self.path)
                    input_size = float(data.get('SizeMBDataGranule', 0)) * 1E6
                    checksum = data.get('checksum')
                    generated_files = self.get_output_files(output_file_path)
                    if data.get('UpdatedGranuleUR', False):
                        updated_output_path = self.get_output_files(os.path.join(self.path,data['UpdatedGranuleUR']))
                        generated_files.extend(updated_output_path)
                    for generated_file in generated_files:

                        files_sizes[generated_file.split('/')[-1]] = os.path.getsize(generated_file)
                    self.output += generated_files
                uploaded_files = self.upload_output_files_to_staging(f"{file_uri}.cmr.json")
                for uploaded_file in uploaded_files:
                    if uploaded_file is None or not uploaded_file.startswith('s3'):
                        continue
                    parsed_uri = s3.uri_parser(uploaded_file)
                    filename = parsed_uri['filename']
                    individual_file_data = {
                        'bucket': parsed_uri['bucket'],
                        'key': parsed_uri['key'],
                        'fileName': parsed_uri['filename'],
                        'checksum': checksum,
                        'checksumType': "md5",
                        'type': "metadata",
                        'size': files_sizes.get(filename, input_size)
                    }
                    files.append(individual_file_data)

                    # Workaround for local file since system bucket shouldn't matter locally
                    system_bucket_path = uploaded_files[0] if len(uploaded_files) > 0 else \
                        f"s3://{os.path.basename(self.input[0])}"
                    self.input['system_bucket_path'] = system_bucket_path
            granule['files'] += files

        # Clean up
        for generated_file in self.output:
            if os.path.exists(generated_file):
                os.remove(generated_file)

        logger.info('MDX processing completed.')
        return {"granules": granules, "input": self.input,
                "system_bucket": system_bucket}


if __name__ == '__main__':
    MDX.cli()
