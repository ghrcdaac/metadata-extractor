"""Lookup creator for WB-57 HARVARD TC4 (wb57harvardtc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "wb57harvardtc4"
provider_path = "wb57harvardtc4/"

nav_lookup = {'TC4_WB57_HCO2_20070801.txt': {'start': '2007-08-01T16:49:10+00:00',
  'end': '2007-08-01T20:17:32+00:00',
  'north': 36.8234,
  'south': 29.2671,
  'east': -86.6039,
  'west': -94.9756},
 'TC4_WB57_HCO2_20070803.txt': {'start': '2007-08-03T12:26:11+00:00',
  'end': '2007-08-03T16:34:52+00:00',
  'north': 29.1651,
  'south': 9.8522,
  'east': -83.9917,
  'west': -94.7082},
 'TC4_WB57_HCO2_20070805.txt': {'start': '2007-08-05T13:26:12+00:00',
  'end': '2007-08-05T16:57:35+00:00',
  'north': 10.1067,
  'south': 6.1498,
  'east': -79.6128,
  'west': -84.6682},
 'TC4_WB57_HCO2_20070806.txt': {'start': '2007-08-06T13:06:11+00:00',
  'end': '2007-08-06T16:41:35+00:00',
  'north': 9.8944,
  'south': 1.9787,
  'east': -84.4835,
  'west': -92.2824},
 'TC4_WB57_HCO2_20070808.txt': {'start': '2007-08-08T12:30:10+00:00',
  'end': '2007-08-08T16:27:38+00:00',
  'north': 9.6731,
  'south': 5.8979,
  'east': -82.1013,
  'west': -85.1679},
 'TC4_WB57_HCO2_20070809.txt': {'start': '2007-08-09T13:06:12+00:00',
  'end': '2007-08-09T16:45:22+00:00',
  'north': 29.569,
  'south': 10.305,
  'east': -84.5077,
  'west': -94.9687},
 'TC4_WB57_HCO2_20070813.txt': {'start': '2007-08-13T15:48:11+00:00',
  'end': '2007-08-13T19:14:12+00:00',
  'north': 36.1823,
  'south': 29.3742,
  'east': -85.1797,
  'west': -94.9844},
 'TC4_WB57_HOX_20070803.txt': {'start': '2007-08-03T12:28:42+00:00',
  'end': '2007-08-03T16:22:09+00:00',
  'north': 29.0158,
  'south': 9.8523,
  'east': -83.9919,
  'west': -94.542},
 'TC4_WB57_HOX_20070806.txt': {'start': '2007-08-06T13:09:05+00:00',
  'end': '2007-08-06T16:35:09+00:00',
  'north': 9.7653,
  'south': 1.8828,
  'east': -84.6047,
  'west': -92.2776},
 'TC4_WB57_HOX_20070808.txt': {'start': '2007-08-08T12:33:26+00:00',
  'end': '2007-08-08T16:14:30+00:00',
  'north': 9.4582,
  'south': 5.8979,
  'east': -82.1015,
  'west': -85.0766},
 'TC4_WB57_HOX_20070809.txt': {'start': '2007-08-09T13:09:55+00:00',
  'end': '2007-08-09T16:32:39+00:00',
  'north': 28.8794,
  'south': 10.5294,
  'east': -84.6991,
  'west': -94.6936},
 'TC4_WB57_HOX_20070813.txt': {'start': '2007-08-13T18:21:19+00:00',
  'end': '2007-08-13T18:51:19+00:00',
  'north': 32.0536,
  'south': 29.7216,
  'east': -90.9053,
  'west': -93.6358},
 'TC4_WB57_HWV_20070801.txt': {'start': '2007-08-01T16:26:59+00:00',
  'end': '2007-08-01T20:15:50+00:00',
  'north': 36.8234,
  'south': 29.2671,
  'east': -86.6039,
  'west': -95.1684},
 'TC4_WB57_HWV_20070803.txt': {'start': '2007-08-03T12:07:32+00:00',
  'end': '2007-08-03T16:28:49+00:00',
  'north': 29.6191,
  'south': 9.8522,
  'east': -83.9917,
  'west': -95.1668},
 'TC4_WB57_HWV_20070805.txt': {'start': '2007-08-05T13:10:42+00:00',
  'end': '2007-08-05T16:59:37+00:00',
  'north': 10.1067,
  'south': 6.1498,
  'east': -79.5299,
  'west': -84.6701},
 'TC4_WB57_HWV_20070806.txt': {'start': '2007-08-06T12:50:19+00:00',
  'end': '2007-08-06T16:54:28+00:00',
  'north': 10.0534,
  'south': 1.8826,
  'east': -84.1874,
  'west': -92.2824},
 'TC4_WB57_HWV_20070808.txt': {'start': '2007-08-08T12:13:09+00:00',
  'end': '2007-08-08T16:33:54+00:00',
  'north': 10.0233,
  'south': 5.8979,
  'east': -82.1013,
  'west': -85.1679},
 'TC4_WB57_HWV_20070809.txt': {'start': '2007-08-09T12:50:12+00:00',
  'end': '2007-08-09T16:40:49+00:00',
  'north': 29.335,
  'south': 9.9687,
  'east': -84.1834,
  'west': -94.887},
 'TC4_WB57_HWV_20070813.txt': {'start': '2007-08-13T15:22:49+00:00',
  'end': '2007-08-13T19:10:50+00:00',
  'north': 36.1823,
  'south': 29.34,
  'east': -85.1797,
  'west': -95.1651},
 'TC4_WB57_ICOS_20070808.txt': {'start': '2007-08-08T12:33:42+00:00',
  'end': '2007-08-08T16:22:34+00:00',
  'north': 9.6729,
  'south': 6.0885,
  'east': -82.4625,
  'west': -85.1679},
 'TC4_WB57_ICOS_20070809.txt': {'start': '2007-08-09T13:11:02+00:00',
  'end': '2007-08-09T15:07:42+00:00',
  'north': 21.1813,
  'south': 10.5951,
  'east': -84.7688,
  'west': -89.7976}
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
