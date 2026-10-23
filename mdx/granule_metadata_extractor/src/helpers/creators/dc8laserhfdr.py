"""Lookup creator for DC-8 LASERH FDR (dc8laserhfdr)."""
from datetime import datetime, timedelta
from utils.mdx import MDX
from utils.streams import as_text_stream

short_name = "dc8laserhfdr"
provider_path = "dc8laserhfdr/"

nav_lookup = {
 'CAMEX3_DC8FDR_LASERH_19980806.txt': {'start': '1998-08-06T23:07:24',
  'end': '1998-08-06T17:37:08',
  'north': 36.202,
  'south': 27.466,
  'east': -117.429,
  'west': -122.979},
 'CAMEX3_DC8FDR_LASERH_19980810.txt': {'start': '1998-08-10T22:16:24',
  'end': '1998-08-10T15:20:29',
  'north': 36.021,
  'south': 27.403000000000002,
  'east': -78.744,
  'west': -118.217},
 'CAMEX3_DC8FDR_LASERH_19980813.txt': {'start': '1998-08-13T23:39:33',
  'end': '1998-08-13T19:15:34',
  'north': 28.929000000000002,
  'south': 24.068,
  'east': -77.283,
  'west': -82.18}
}

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
        metadata = nav_lookup[filename]
        for field in ['start', 'end']:
            metadata[field] = datetime.fromisoformat(metadata[field])
        metadata['format'] = "ASCII"
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