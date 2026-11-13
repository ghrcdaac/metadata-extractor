"""Lookup creator for WB-57 ARGUS TC4 (wb57argustc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "wb57argustc4"
provider_path = "wb57argustc4/"

nav_lookup = {'TC4_WB57_ARGUS_20070805.txt': {'start': '2007-08-05T13:14:55+00:00',
  'end': '2007-08-05T17:11:39+00:00',
  'north': 10.1067,
  'south': 6.1498,
  'east': -79.5299,
  'west': -84.6701},
 'TC4_WB57_ARGUS_20070806.txt': {'start': '2007-08-06T12:57:15+00:00',
  'end': '2007-08-06T17:04:37+00:00',
  'north': 10.0534,
  'south': 1.8828,
  'east': -84.1877,
  'west': -92.2821},
 'TC4_WB57_ARGUS_20070808.txt': {'start': '2007-08-08T12:42:23+00:00',
  'end': '2007-08-08T16:43:14+00:00',
  'north': 9.9706,
  'south': 5.8979,
  'east': -82.1013,
  'west': -85.1679}
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
        self.file_type = 'ASCII'
        if filename in nav_lookup:
            metadata = nav_lookup[filename]
        else:
            raise KeyError(f"No nav lookup for file {filename}")
        for field in ['start', 'end']:
            metadata[field] = datetime.fromisoformat(metadata[field])
        metadata['format'] = self.file_type
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
