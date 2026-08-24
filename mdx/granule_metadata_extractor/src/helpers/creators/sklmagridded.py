"""Creator file for South Korea LMA Level 2 (sklmaef)"""
# for all future collections
from datetime import datetime
from utils.mdx import MDX
from pathlib import Path
from zipfile import ZipFile
import re
import json

short_name = "sklmagridded"
provider_path = "sklmagridded"
file_type = "netCDF-4"


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """

        # Level 3 has same bounds as Level 1
        # Just read from the existing lookup file

        if m := re.match(r'^\w+_(\d{8})_(\d{6})_(\d{3})_.*\.nc$', filename):
            base_file = f"sklma_{m.group(1)[2:]}_{m.group(2)}_0{m.group(3)}.dat"
        else:
            print(f"Did not fit pattern: {filename}")
            return {}

        lookup_zip_path = Path(__file__).resolve().parent.parent / "sklma.zip"

        with ZipFile(lookup_zip_path) as lookup_zip:
            with lookup_zip.open("lookup.json") as collection_lookup:
                sklma_lookup = json.load(collection_lookup)

        if base_file in sklma_lookup:
            metadata = sklma_lookup[base_file]
        else:
            print(f"Not found in lookup: {base_file}")
            return {}

        for key, val in metadata.items():
            if key in ['north', 'south', 'east', 'west']:
                metadata[key] = float(val)
            elif key in ['start', 'end']:
                metadata[key] = datetime.fromisoformat(val)
        return metadata

    def main(self):
        # start_time = time.time()
        self.process_collection(short_name, provider_path)
        # elapsed_time = time.time() - start_time
        # print(f"Elapsed time in seconds: {elapsed_time}")
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')
