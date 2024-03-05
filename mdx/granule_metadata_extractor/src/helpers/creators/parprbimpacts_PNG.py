# create lookup zip for parprbimpacts images
# for all future collections
from datetime import datetime
from utils.mdx import MDX
import cProfile
import time
import math
import re

import json
import os
import pathlib
from zipfile import ZipFile

short_name = "parprbimpacts"
provider_path = "parprbimpacts/fieldCampaigns/impacts/NCAR_Particle_Probes/data/images/"
file_type = "PNG"


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_path = file_path

        #Get lookup dataset's metadata attributes from lookup zip
        lookup_zip_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                               f"../src/helpers/parprbimpacts.zip")
        with ZipFile(lookup_zip_path) as lookup_zip:
            with lookup_zip.open("lookup.json") as collection_lookup:
                self.lookup_json = json.load(collection_lookup)


    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        if filename.endswith('.tar'):
           return self.get_png_metadata(filename)


    def get_png_metadata(self, filename):
        """
        Extract temporal and spatial metadata for image files
        """
        utc_date = filename.split('/')[-1].split('_')[-3] #i.e., 20200118
        keys = [x for x in self.lookup_json.keys() if utc_date in x]
        start = [datetime.strptime(self.lookup_json[x]['start'],'%Y-%m-%dT%H:%M:%SZ') for x in keys]
        end = [datetime.strptime(self.lookup_json[x]['end'],'%Y-%m-%dT%H:%M:%SZ') for x in keys]
        north = [float(self.lookup_json[x]['north']) for x in keys]
        south = [float(self.lookup_json[x]['south']) for x in keys]
        west = [float(self.lookup_json[x]['west']) for x in keys]
        east = [float(self.lookup_json[x]['east']) for x in keys]
        minTime = min(start)
        maxTime = max(end)
        minlat = min(south) 
        maxlat = max(north)
        minlon = min(west)
        maxlon = max(east)
        return {
            "start": minTime,
            "end": maxTime,
            "north": maxlat,
            "south": minlat,
            "east": maxlon,
            "west": minlon,
            "format": file_type
        }

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
