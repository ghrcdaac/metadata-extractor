"""Lookup creator for METNAV NURTURE (metnavnurture)."""
from datetime import datetime, time, timedelta, timezone
from utils.mdx import MDX
from typing import Any

short_name = "ozonenurture"
provider_path = "ozonenurture/"

# Nav had to be extracted from metnavnurture dataset
# Extracted in notebook and imported here
nav_lookup = {
    'NURTURE_MIRO_G3_20260124_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-01-24T20:06:57',
          'north': 53.427371979,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -76.362876892},
    'NURTURE_MIRO_G3_20260126_R0.ict':
         {'start': '2026-01-24T15:44:20',
          'end': '2026-01-26T18:57:36',
          'north': 58.623390198,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -76.362876892},
    'NURTURE_MIRO_G3_20260127_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-01-27T18:48:30',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -84.0184593},
    'NURTURE_MIRO_G3_20260129_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-01-29T19:23:18',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -84.0184593},
    'NURTURE_MIRO_G3_20260201_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-01T21:52:47',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -84.0184593},
    'NURTURE_MIRO_G3_20260203_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-03T18:45:58',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260204_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-04T19:22:52',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260205_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-05T19:31:48',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260206_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-06T19:21:03',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260207_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-07T20:08:05',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260209_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-09T19:22:02',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -48.332118988,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260210_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-10T20:10:12',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -45.994091,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260211_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-11T19:01:01',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -45.994091,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260214_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-14T19:35:23',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -45.994091,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260215_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-15T19:46:36',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -45.994091,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260216_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-16T19:52:26',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -45.994091,
          'west': -86.2049103},
    'NURTURE_MIRO_G3_20260217_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-17T19:40:52',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_MIRO_G3_20260218_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-18T19:18:54',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_MIRO_G3_20260219_R0_L1.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T17:52:02',
          'north': 63.3696556,
          'south': 37.081947327,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_MIRO_G3_20260219_R0_L2.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_MIRO_G3_20260219_RA.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260124_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260126_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260127_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260129_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260203_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260204_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260210_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260211_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260214_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260215_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260216_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260217_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260218_R0.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260219_R0_L1.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260219_R0_L2.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212},
    'NURTURE_Ozone_G3_20260219_RA.ict':
          {'start': '2026-01-24T15:44:20',
          'end': '2026-02-19T20:54:49',
          'north': 63.3696556,
          'south': 37.0095062,
          'east': -45.994091,
          'west': -86.4447212}
}

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.lookup = {}

    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

    def process(self, filename: str, stream=None) -> dict[str, Any]:
        if filename in nav_lookup:
            metadata = nav_lookup[filename]
            metadata["format"] = "ASCII"
            metadata["start"] = datetime.fromisoformat(metadata["start"])
            metadata["end"] = datetime.fromisoformat(metadata["end"])

        return metadata

if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')