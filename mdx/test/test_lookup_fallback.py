import unittest
from granule_metadata_extractor.processing.process_lookup import ExtractLookupMetadata
from pathlib import Path

LOOKUP_COLLECTION = "auslma"
FIXTURES_PATH = Path(__file__).parent / "fixtures"
IN_LOOKUP_GRANULE = "IGNIS_250923_081000_0600.dat.gz"
OUT_OF_LOOKUP_GRANULE = "IGNIS_261016_150000_0600.dat.gz"

class TestLookupFallback(unittest.TestCase):
    def test_get_metadata_from_lookup(self):
        """Lookup a file that is in the lookup dict."""
        lookup = ExtractLookupMetadata(FIXTURES_PATH / IN_LOOKUP_GRANULE)
        metadata = lookup.get_metadata(LOOKUP_COLLECTION, format='Not Provided')
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata['ShortName'], 'auslma')
        self.assertEqual(metadata['GranuleUR'], 'IGNIS_250923_081000_0600.dat.gz')
        self.assertEqual(metadata['BeginningDateTime'], '2025-09-23T16:21:48Z')
        self.assertEqual(metadata['EndingDateTime'], '2025-09-23T16:24:02Z')
        self.assertEqual(metadata['EastBoundingCoordinate'], '117.253')
        self.assertEqual(metadata['WestBoundingCoordinate'], '117.0')
        self.assertEqual(metadata['NorthBoundingCoordinate'], '-32.186')
        self.assertEqual(metadata['SouthBoundingCoordinate'], '-32.504')
        self.assertEqual(metadata['SizeMBDataGranule'], '0.0')
        self.assertEqual(metadata['DataFormat'], 'ASCII')

    def test_get_metadata_from_fallback(self):
        """Lookup a file that's not in the lookup dict."""
        lookup = ExtractLookupMetadata(FIXTURES_PATH / OUT_OF_LOOKUP_GRANULE)
        metadata = lookup.get_metadata(LOOKUP_COLLECTION, format='Not Provided')
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata['ShortName'], 'auslma')
        self.assertEqual(metadata['GranuleUR'], 'IGNIS_261016_150000_0600.dat.gz')
        self.assertEqual(metadata['BeginningDateTime'], '2026-10-16T15:00:00Z')
        self.assertEqual(metadata['EndingDateTime'], '2026-10-16T15:10:00Z')
        self.assertEqual(metadata['EastBoundingCoordinate'], '119.740')
        self.assertEqual(metadata['WestBoundingCoordinate'], '115.460')
        self.assertEqual(metadata['NorthBoundingCoordinate'], '-31.001')
        self.assertEqual(metadata['SouthBoundingCoordinate'], '-34.599')
        self.assertEqual(metadata['SizeMBDataGranule'], '0.0')
        self.assertEqual(metadata['DataFormat'], 'ASCII')


if __name__ == '__main__':
    unittest.main()
