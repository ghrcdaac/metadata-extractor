"""Lookup creator for WB-57 CLH TC4 (wb57clhtc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "wb57clhtc4"
provider_path = "wb57clhtc4/"

nav_lookup = {'TC4_WB57_CLH_20070805.txt': {'start': '2007-08-05T13:26:10+00:00',
  'end': '2007-08-05T16:46:54+00:00',
  'north': 10.1024,
  'south': 6.1498,
  'east': -79.5299,
  'west': -84.2718},
 'TC4_WB57_CLH_20070806.txt': {'start': '2007-08-06T13:06:27+00:00',
  'end': '2007-08-06T16:43:00+00:00',
  'north': 9.8841,
  'south': 1.8826,
  'east': -84.4835,
  'west': -92.2824},
 'TC4_WB57_CLH_20070808.txt': {'start': '2007-08-08T12:30:25+00:00',
  'end': '2007-08-08T16:20:22+00:00',
  'north': 9.651,
  'south': 5.8979,
  'east': -82.1013,
  'west': -85.1679},
 'TC4_WB57_CLH_20070809.txt': {'start': '2007-08-09T13:05:38+00:00',
  'end': '2007-08-09T16:31:19+00:00',
  'north': 28.8599,
  'south': 10.2727,
  'east': -84.4826,
  'west': -94.6936},
 'TC4_WB57_CLH_20070813.txt': {'start': '2007-08-13T15:47:06+00:00',
  'end': '2007-08-13T18:53:31+00:00',
  'north': 36.1823,
  'south': 29.3556,
  'east': -85.1797,
  'west': -94.5451}
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
