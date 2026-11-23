"""Lookup creator for DC-8 SSFR TC4 (dc8ssfrtc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8ssfrtc4"
provider_path = "dc8ssfrtc4/"

nav_lookup = {'TC4_DC8_SSFR_20070628.txt': {'start': '2007-06-28T22:00:00+00:00',
  'end': '2007-06-29T00:00:00+00:00',
  'north': 39.993096,
  'south': 38.794944,
  'east': -119.540344,
  'west': -125.899012},
 'TC4_DC8_SSFR_20070630.txt': {'start': '2007-06-30T18:00:00+00:00',
  'end': '2007-06-30T23:00:00+00:00',
  'north': 43.283972,
  'south': 39.656836,
  'east': -122.846012,
  'west': -127.30924},
 'TC4_DC8_SSFR_20070702.txt': {'start': '2007-07-02T16:00:00+00:00',
  'end': '2007-07-03T00:00:00+00:00',
  'north': 38.771896,
  'south': 32.858364,
  'east': -117.858356,
  'west': -123.03934},
 'TC4_DC8_SSFR_20070713.txt': {'start': '2007-07-13T15:00:00+00:00',
  'end': '2007-07-13T22:00:00+00:00',
  'north': 33.809223,
  'south': 10.463104,
  'east': -88.376598,
  'west': -117.815151},
 'TC4_DC8_SSFR_20070717.txt': {'start': '2007-07-17T13:00:00+00:00',
  'end': '2007-07-17T20:00:00+00:00',
  'north': 9.997215,
  'south': -3.272896,
  'east': -80.354176,
  'west': -84.525204},
 'TC4_DC8_SSFR_20070721.txt': {'start': '2007-07-21T12:00:00+00:00',
  'end': '2007-07-21T20:00:00+00:00',
  'north': 12.060413,
  'south': 2.855415,
  'east': -73.356743,
  'west': -84.475422},
 'TC4_DC8_SSFR_20070722.txt': {'start': '2007-07-22T13:00:00+00:00',
  'end': '2007-07-22T21:00:00+00:00',
  'north': 15.719376,
  'south': 6.213284,
  'east': -79.019165,
  'west': -85.179234},
 'TC4_DC8_SSFR_20070724.txt': {'start': '2007-07-24T13:00:00+00:00',
  'end': '2007-07-24T17:00:00+00:00',
  'north': 9.647541,
  'south': 5.686798,
  'east': -84.335346,
  'west': -85.79258},
 'TC4_DC8_SSFR_20070728.txt': {'start': '2007-07-28T13:00:00+00:00',
  'end': '2007-07-28T19:00:00+00:00',
  'north': 14.782276,
  'south': 9.420605,
  'east': -81.678543,
  'west': -88.76627},
 'TC4_DC8_SSFR_20070729.txt': {'start': '2007-07-29T12:00:00+00:00',
  'end': '2007-07-29T20:00:00+00:00',
  'north': 10.043564,
  'south': -5.209236,
  'east': -78.943291,
  'west': -84.512501},
 'TC4_DC8_SSFR_20070731.txt': {'start': '2007-07-31T12:00:00+00:00',
  'end': '2007-07-31T20:00:00+00:00',
  'north': 10.222263,
  'south': 8.391838,
  'east': -82.905235,
  'west': -89.425106},
 'TC4_DC8_SSFR_20070803.txt': {'start': '2007-08-03T13:00:00+00:00',
  'end': '2007-08-03T20:00:00+00:00',
  'north': 13.26273,
  'south': 5.875111,
  'east': -80.762043,
  'west': -85.703316},
 'TC4_DC8_SSFR_20070805.txt': {'start': '2007-08-05T12:00:00+00:00',
  'end': '2007-08-05T19:00:00+00:00',
  'north': 9.333057,
  'south': 5.869789,
  'east': -76.780357,
  'west': -82.934589},
 'TC4_DC8_SSFR_20070806.txt': {'start': '2007-08-06T12:00:00+00:00',
  'end': '2007-08-06T19:00:00+00:00',
  'north': 9.923573,
  'south': -3.000469,
  'east': -84.394398,
  'west': -92.006721},
 'TC4_DC8_SSFR_20070808.txt': {'start': '2007-08-08T12:00:00+00:00',
  'end': '2007-08-08T21:00:00+00:00',
  'north': 9.378891,
  'south': 2.410297,
  'east': -70.932713,
  'west': -84.465294},
 'TC4_DC8_SSFR_20070810.txt': {'start': '2007-08-10T15:00:00+00:00',
  'end': '2007-08-10T21:00:00+00:00',
  'north': 38.683205,
  'south': 10.602837,
  'east': -84.790764,
  'west': -119.488678}gi
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
