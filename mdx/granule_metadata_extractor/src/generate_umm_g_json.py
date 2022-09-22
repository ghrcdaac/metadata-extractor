from datetime import datetime, timedelta
import json
import re
import os


class GenerateUmmGJson:
    """
    Class to generate UMM-G json from Python metadata dict
    """

    cmr_enum_values = ["ASCII", "BINARY", "BMP", "BUFR", "CSV", "GEOTIFF", "GIF", "GEOTIFFINT16",
                       "GEOTIFFFLOAT32", "GRIB", "GZIP", "HDF4", "HDF5", "HDF-EOS2", "HDF-EOS5",
                       "HTML", "ICARTT", "JPEG", "JSON", "KML", "NETCDF-3", "NETCDF-4",
                       "NETCDF-CF", "PNG", "PNG24", "TAR", "TIFF", "XLSX", "XML", "ZIP",
                       "Not provided"]

    def __init__(self, data, age_off=None):
        """
        Initiate the GenerateECHO10XML instance
        :param data: a json object with metadata needed to generate an echo10 xml
        """
        self.data = data
        self.age_off = age_off

    def generate_umm_json_data(self):
        """
        Generate a UMM-G json file according to CMR schema
        :return: xml_data, granule_ur
        """
        umm_json = dict()
        umm_json['GranuleUR'] = self.data['GranuleUR']
        now = datetime.utcnow().strftime(
            '%Y-%m-%dT%H:%M:%SZ')  # Using UTC Time and time format required by CMR
        umm_json['ProviderDates'] = [
            {
                "Type": "Insert",
                "Date": now
            },
            {
                "Type": "Update",
                "Date": now
            }
        ]
        umm_json['CollectionReference'] = {
            "ShortName": self.data['ShortName'],
            "Version": self.data['VersionId']
        }
        umm_json['AccessConstraints'] = {
            "Description": "This product has full public access",
            "Value": 0.0
        }
        umm_json['DataGranule'] = {
            "DayNightFlag": self.data.get("DayNightFlag", "Unspecified"),
            "ProductionDateTime": now,
            "ArchiveAndDistributionInformation": [
                {
                    "Name": self.data['GranuleUR'],
                    "Size": float(self.data['SizeMBDataGranule']),
                    "SizeUnit": "MB",
                }
            ]
        }
        data_format = self.data['DataFormat'].upper()
        # Add format only if it is in cmr's enum value list for format validation
        if data_format in self.cmr_enum_values:
            umm_json['DataGranule']['ArchiveAndDistributionInformation'][0]['Format'] = data_format
        umm_json['TemporalExtent'] = {
            "RangeDateTime": {
                "BeginningDateTime": self.data['BeginningDateTime'],
                "EndingDateTime": self.data['EndingDateTime']
            }
        }
        umm_json['SpatialExtent'] = {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "BoundingRectangles": [
                        {
                            "WestBoundingCoordinate": float(self.data['WestBoundingCoordinate']),
                            "EastBoundingCoordinate": float(self.data['EastBoundingCoordinate']),
                            "NorthBoundingCoordinate": float(self.data['NorthBoundingCoordinate']),
                            "SouthBoundingCoordinate": float(self.data['SouthBoundingCoordinate'])
                        }
                    ]
                }
            }
        }
        umm_json['RelatedUrls'] = [
            {
                "URL": self.data['OnlineAccessURL'],
                "Type": "GET DATA",
                "Description": "Files may be downloaded directly to your workstation from this "
                               "link"
            }
        ]
        umm_json['MetadataSpecification'] = {
            "URL": "https://cdn.earthdata.nasa.gov/umm/granule/v1.6.2",
            "Name": "UMM-G",
            "Version": "1.6.2"
        }

        # Add delete parameter if age off is set for collection
        if self.data.get('AgeOffFlag', False) and self.age_off:
            age_off_date = datetime.strftime(
                datetime.strptime(now, '%Y-%m-%dT%H:%M:%SZ')
                + timedelta(days=float(re.search(r'(.*) days', self.age_off)[1])),
                '%Y-%m-%dT%H:%M:%SZ')
            umm_json['ProviderDates'].append({
                "Type": "Delete",
                "Date": age_off_date
            })

        return [umm_json, self.data['GranuleUR']]

    def generate_umm_json_file(self, output_folder='/tmp/'):
        """
        Genrate a local UMM-G json file at the provided location
        :param output_folder: the folder where the json file will reside
        :return:
        """
        xml_data, filename = self.generate_umm_json_data()
        with open(f"{os.path.join(output_folder.rstrip('/'), filename)}.cmr.json", 'w+') as f:
            json.dump(xml_data, f, indent=4)
