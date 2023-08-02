from run_cumulus_task import run_cumulus_task
import granule_metadata_extractor.processing as mdx
import granule_metadata_extractor.src as src
from cumulus_process import Process, s3
from re import match
import os
import boto3
from helpers import get_logger
import copy
import shutil

logger = get_logger()

class MDX(Process):
    """
    Class to extract spatial and temporal metadata
    """
    def __init__(self, input, config, path='/tmp/mdx'):
        super().__init__(input, config=config, path=path)
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path)

    def generate_json_data(self, data, access_url, output_folder):
        """
        Generates umm-g json for CMR metadata
        :param data: python dict containing granule extracted metadata
        :param access_url: url to get data
        :param output_folder: local folder to create json file at
        :return: input dict
        """

        granule_new_name = data.get('UpdatedGranuleUR', None)
        if granule_new_name:
            access_url = access_url.replace(os.path.basename(access_url),
                                            os.path.basename(granule_new_name))
        data['OnlineAccessURL'] = access_url
        umm_json = src.GenerateUmmGJson(data,
                                        age_off=self.config.get('collection', {}).get('meta', {}).
                                        get('age-off', None))
        umm_json.generate_umm_json_file(output_folder=output_folder)
        return data

    def extract_netcdf_metadata(self, ds_short_name, version, access_url, netcdf_file, netcdf_vars,
                                output_folder='/tmp', format='netCDF-4'):
        """
        Function to extract metadata from netCDF files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param netcdf_file: Path to netCDF file
        :param netcdf_vars: netCDF variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """
        regex = netcdf_vars.get('regex')
        switcher = {
            "goesrpltcrs": mdx.ExtractGeosrpltcrsMetadata,
            "gpmwrflpvex": mdx.ExtractLpvexwrfMetadata,
            "gpmseafluxicepop": mdx.ExtractGpmseafluxicepopMetadata,
            "uiucsndimpacts": mdx.ExtractUiucsndimpactsMetadata,
            "gpmvn": mdx.ExtractGpmvnMetadata,
            "noaasndimpacts": mdx.ExtractNoaasndimpactsMetadata,
            "kakqimpacts": mdx.ExtractKakqimpactsMetadata,
            "kccximpacts": mdx.ExtractKccximpactsMetadata,
            "kbgmimpacts": mdx.ExtractKbgmimpactsMetadata,
            "kboximpacts": mdx.ExtractKboximpactsMetadata,
            "kbufimpacts": mdx.ExtractKbufimpactsMetadata,
            "sbusndimpacts": mdx.ExtractSbusndimpactsMetadata,
            "nexeastimpacts": mdx.ExtractNexeastimpactsMetadata,
            "nexmidwstimpacts": mdx.ExtractNexmidwstimpactsMetadata,
            "ncsusndimpacts": mdx.ExtractNcsusndimpactsMetadata,
            "goesimpacts": mdx.ExtractGoesimpactsMetadata,
            "amsua15sp": mdx.ExtractAMSUAMetadata,
            "npolimpacts": mdx.ExtractNpolimpactsMetadata,
            "sbuceilimpacts": mdx.ExtractSbuceilimpactsMetadata,
            "sbulidarimpacts": mdx.ExtractSbulidarimpactsMetadata,
            "sbukasprimpacts": mdx.ExtractSbukasprimpactsMetadata,
            "sbumrr2impacts": mdx.ExtractSbumrr2impactsMetadata,
            "sbumetimpacts": mdx.ExtractSbumetimpactsNetCDFMetadata,
            "rss1tpwnv7r01": mdx.ExtractRssClimatologyMetadata,
            "rss1windnv7r01": mdx.ExtractRssClimatologyMetadata,
            "sbuparsimpacts": mdx.ExtractSbuparsimpactsMetadata,
            "cosmirimpacts": mdx.ExtractCosmirimpactsMetadata,
            "cplimpacts": mdx.ExtractCplimpactsMetadata,
            "parprbimpacts": mdx.ExtractParprbimpactsMetadata,
            "isslis_v1_nrt": mdx.ExtractIsslisv1Metadata,
            "isslis_v1_nqc": mdx.ExtractIsslisv1Metadata,
            "isslisg_v1_nrt": mdx.ExtractIsslisgv1Metadata,
            "isslisg_v1_nqc": mdx.ExtractIsslisgv1Metadata,
            "seaflux": mdx.ExtractSeafluxMetadata,
            "asosimpacts": mdx.ExtractAsosimpactsMetadata,
            "wrfimpacts": mdx.ExtractWrfimpactsMetadata,
            "hs3shis": mdx.ExtractHs3shisMetadata,
            "hiwrapimpacts": mdx.ExtractHiwrapimpactsMetadata,
            "gpmwacrc3vp": mdx.ExtractGpmwacrc3vpMetadata,
            "crsimpacts": mdx.ExtractCrsimpactsMetadata,
            "exradimpacts": mdx.ExtractExradimpactsMetadata,
            "amprimpacts": mdx.ExtractAmprimpactsMetadata,
            "kjklimpacts": mdx.ExtractKjklimpactsMetadata,
            "klotimpacts": mdx.ExtractKlotimpactsMetadata,
            "klwximpacts": mdx.ExtractKlwximpactsMetadata,
            "kmhximpacts": mdx.ExtractKmhximpactsMetadata,
            "kmkximpacts": mdx.ExtractKmkximpactsMetadata,
            "isslisg_v1_fin": mdx.ExtractIsslisgv1Metadata,
            "isslis_v1_fin": mdx.ExtractIsslisv1Metadata,
            "kokximpacts": mdx.ExtractNexradimpactsMetadata,
            "kpbzimpacts": mdx.ExtractNexradimpactsMetadata,
            "kraximpacts": mdx.ExtractNexradimpactsMetadata,
            "krlximpacts": mdx.ExtractNexradimpactsMetadata,
            "ktyximpacts": mdx.ExtractNexradimpactsMetadata,
            "kvwximpacts": mdx.ExtractNexradimpactsMetadata,
            "kcleimpacts": mdx.ExtractNexradimpactsMetadata,
            "kcxximpacts": mdx.ExtractNexradimpactsMetadata,
            "kdiximpacts": mdx.ExtractNexradimpactsMetadata,
            "kdoximpacts": mdx.ExtractNexradimpactsMetadata,
            "kdtximpacts": mdx.ExtractNexradimpactsMetadata,
            "kgyximpacts": mdx.ExtractNexradimpactsMetadata,
            "kilnimpacts": mdx.ExtractNexradimpactsMetadata,
            "kilximpacts": mdx.ExtractNexradimpactsMetadata,
            "kindimpacts": mdx.ExtractNexradimpactsMetadata,
            "kiwximpacts": mdx.ExtractNexradimpactsMetadata,
            "kdvnimpacts": mdx.ExtractNexradimpactsMetadata,
            "kenximpacts": mdx.ExtractNexradimpactsMetadata,
            "kfcximpacts": mdx.ExtractNexradimpactsMetadata,
            "kgrbimpacts": mdx.ExtractNexradimpactsMetadata,
            "kgrrimpacts": mdx.ExtractNexradimpactsMetadata,
            "lislip": mdx.ExtractLislipMetadata,
            "lislipG": mdx.ExtractLislipGMetadata,
            "msutls": mdx.ExtractMsuMetadata,
            "msutlt": mdx.ExtractMsuMetadata,
            "msutmt": mdx.ExtractMsuMetadata,
            "msuttp": mdx.ExtractMsuMetadata,
            "isslis_v2_nrt": mdx.ExtractIsslisv1Metadata,
            "isslis_v2_nqc": mdx.ExtractIsslisv1Metadata,
            "isslis_v2_fin": mdx.ExtractIsslisv1Metadata,
            "isslisg_v2_nrt": mdx.ExtractIsslisgv1Metadata,
            "isslisg_v2_nqc": mdx.ExtractIsslisgv1Metadata,
            "isslisg_v2_fin": mdx.ExtractIsslisgv1Metadata,
            "ualbparsimpacts": mdx.ExtractUalbparsimpactsMetadata,
            "ualbmrr2impacts": mdx.ExtractUalbmrr2impactsMetadata,
            "goesrglmgrid": mdx.ExtractGoesrglmgridMetadata,
            "exradepoch": mdx.ExtractExradepochMetadata,
            "gpmkarx2ifld": mdx.ExtractGpmkarx2ifldMetadata,
            "gpmkdmx2ifld": mdx.ExtractGpmkdmx2ifldMetadata,
            "gpmkdvn2ifld": mdx.ExtractGpmkdvn2ifldMetadata,
            "gpmkmpx2ifld": mdx.ExtractGpmkmpx2ifldMetadata,
            "pmhailclim": mdx.ExtractPmhailclimMetadata,
            "hamsrepoch": mdx.ExtractHamsrepochMetadata,
            "hamsrcpexaw": mdx.ExtractHamsrcpexawMetadata,
            "glmgoesL3": mdx.ExtractGlmgoesL3Metadata,
            "apr3cpexaw": mdx.ExtractApr3cpexawMetadata,
            "mrmsimpacts": mdx.ExtractMrmsimpactsMetadata,
            "apr3cpex": mdx.ExtractApr3cpexMetadata,
            "scrxsondecpexaw": mdx.ExtractScrxsondecpexawMetadata,
            "ascatcpex": mdx.ExtractAscatcpexMetadata,
            "gfscpex": mdx.ExtractGfscpexMetadata,
            "goescpex": mdx.ExtractGoescpexMetadata,
            "imergcpex": mdx.ExtractImergcpexMetadata,
            "hamsrcpex": mdx.ExtractHamsrcpexMetadata,
            "masccpex": mdx.ExtractMasccpexMetadata,
            "modiscpex": mdx.ExtractModiscpexMetadata,
            "exrad3dimpacts": mdx.ExtractExrad3dimpactsMetadata,
            "aamhcpex": mdx.ExtractAamhcpexMetadata,
            "airscpex": mdx.ExtractAirscpexMetadata,
            "sbuskylerimpacts": mdx.ExtractSbuskylerimpactsMetadata,
            "gpmpal": mdx.ExtractGpmpalMetadata,
            "hamsrcpexcv": mdx.ExtractHamsrcpexcvMetadata,
            "apr3cpexcv": mdx.ExtractApr3cpexcvMetadata,
            "goescpexcv": mdx.ExtractGoescpexcvMetadata,
            "sondecpexcv": mdx.ExtractSondecpexcvMetadata,
            "hiwat": mdx.ExtractHiwatMetadata
        }

        time_variable_key = netcdf_vars.get('time_var_key')
        lon_variable_key = netcdf_vars.get('lon_var_key')
        lat_variable_key = netcdf_vars.get('lat_var_key')
        time_units = netcdf_vars.get('time_units', 'units')

        if match(regex, os.path.basename(netcdf_file)):
            metadata = switcher.get(ds_short_name, src.ExtractNetCDFMetadata)(netcdf_file)
            format = 'netCDF-3' if '.nc4' not in netcdf_file else format
            data = metadata.get_metadata(ds_short_name=ds_short_name,
                                         time_variable_key=time_variable_key,
                                         lon_variable_key=lon_variable_key,
                                         lat_variable_key=lat_variable_key,
                                         time_units=time_units, format=format, version=version)
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def extract_csv_metadata(self, ds_short_name, version, access_url, csv_file, csv_vars={},
                             output_folder='/tmp', format='CSV'):
        """
        Function to extract metadata from netCDF files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param csv_file: Path to csv file
        :param csv_vars: Variables within csv file
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        metadata = src.ExtractCSVMetadata(csv_file)

        time_position = csv_vars.get('time_row_position', 0)
        lon_postion = csv_vars.get('lon_row_position', 15)
        lat_postion = csv_vars.get('lat_row_position', 16)
        time_units = csv_vars.get('time_units', 'hours')
        regex = csv_vars.get('regex', '.*')

        if match(regex, os.path.basename(csv_file)):
            data = metadata.get_metadata(ds_short_name=ds_short_name, time_position=time_position,
                                         time_units=time_units, lon_postion=lon_postion,
                                         lat_postion=lat_postion,
                                         format=format, version='0.9')
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def extract_binary_metadata(self, ds_short_name, version, access_url, binary_file,
                                binary_vars={},
                                output_folder='/tmp', format='Binary'):
        """

        """
        switcher = {
            "aces1cont": mdx.ExtractAces1ContMetadata,
            "aces1efm": mdx.ExtractAces1EfmMetadata,
            "aces1log": mdx.ExtractAces1LogMetadata,
            "aces1time": mdx.ExtractAces1TimeMetadata,
            "globalir": mdx.ExtractGlobalirMetadata
        }
        regex = binary_vars.get('regex', '.*')
        if match(regex, os.path.basename(binary_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(binary_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, version=version,
                                         format=format)
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def extract_ascii_metadata(self, ds_short_name, version, access_url, ascii_file, ascii_vars={},
                               output_folder='/tmp', format='ASCII'):
        """
        Function to extract metadata from ASCII files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param ascii_file: Path to ascii file
        :param ascii_vars: ASCII variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        switcher = {
            "gpmodmlpvex": mdx.ExtractGpmodmlpvexMetadata,
            "gpmjwlpvex": mdx.ExtractGpmjwlpvexMetadata,
            "gpmparawifld": mdx.ExtractGpmparawifldMetadata,
            "gpmpipicepop": mdx.ExtractGpmpipicepopASCIIMetadata,
            "namdrop_raw": mdx.ExtractNamdrop_rawMetadata,
            "gripstorm": mdx.ExtractGripstormASCIIMetadata,
            "relampagolma": mdx.ExtractLmarelampagoMetadata,
            "goesrpltavirisng": mdx.ExtractGoesrpltavirisngMetadata,
            "gpmvanlpvex": mdx.ExtractGpmvanlpvexMetadata,
            "gpmikalpvex": mdx.ExtractGpmikalpvexMetadata,
            "gpmkorlpvex": mdx.ExtractGpmkorlpvexMetadata,
            "gpmkerlpvex": mdx.ExtractGpmkerlpvexMetadata,
            "gpmkumlpvex": mdx.ExtractGpmkumlpvexMetadata,
            "gpm2dc3vp": mdx.ExtractGpm2dc3vpMetadata,
            "gpmlipiphx": mdx.ExtractGpmlipiphxASCIIMetadata,
            "misrepimpacts": mdx.ExtractMisrepimpactsMetadata,
            "2dimpacts": mdx.Extract2dimpactsMetadata,
            "apuimpacts": mdx.ExtractApuimpactsMetadata,
            "sbumetimpacts": mdx.ExtractSbumetimpactsASCIIMetadata,
            "sbuplimpacts": mdx.ExtractSbuplimpactsMetadata,
            "er2navimpacts": mdx.ExtractEr2navimpactsMetadata,
            "nalma": mdx.ExtractNalmaMetadata,
            "nalmanrt": mdx.ExtractNalmanrtMetadata,
            "nalmaraw": mdx.ExtractNalmarawMetadata,
            "p3metnavimpacts": mdx.ExtractP3metnavimpactsMetadata,
            "tammsimpacts": mdx.ExtractTammsimpactsMetadata,
            "lipimpacts": mdx.ExtractLipimpactsMetadata,
            "gpmsurmetc3vp": mdx.ExtractGpmsurmetc3vpMetadata,
            "gpmarsifld": mdx.ExtractGpmarsifldMetadata,
            "gpmvisecc3vp": mdx.ExtractGpmvisecc3vpMetadata,
            "cmimpacts": mdx.ExtractCmimpactsMetadata,
            "gpmxetc3vp": mdx.ExtractGpmxetc3vpMetadata,
            "avapsimpacts": mdx.ExtractAvapsimpactsMetadata,
            "wisperimpacts": mdx.ExtractWisperimpactsMetadata,
            "fltrepepoch": mdx.ExtractFltrepepochMetadata,
            "navghepoch": mdx.ExtractNavghepochMetadata,
            "metnavcpexaw": mdx.ExtractMetnavcpexawMetadata,
            "prsondecpexaw": mdx.ExtractPrsondecpexawMetadata,
            "musondeimpacts": mdx.ExtractMusondeimpactsMetadata,
            "navdc8cpex": mdx.ExtractNavdc8cpexMetadata,
            "metnavcpexcv": mdx.ExtractMetnavcpexcvMetadata,
            "gpmkcxxgcpex": mdx.ExtractGpmkcxxgcpexMetadata
        }

        regex = ascii_vars.get('regex', '.*')

        if match(regex, os.path.basename(ascii_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(ascii_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=format,
                                         version=version)
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def extract_kml_metadata(self, ds_short_name, version, access_url, kml_file, ascii_vars={},
                             output_folder='/tmp', format='KML'):
        """
        Function to extract metadata from KML files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param kml_file: Path to kml file
        :param ascii_vars: ascii variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        switcher = {
            "gpmsatpaifld": mdx.ExtractGpmsatpaifldMetadata,
            "gripstorm": mdx.ExtractGripstormKMLMetadata
        }

        regex = ascii_vars.get('regex', '.*')

        if match(regex, os.path.basename(kml_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(kml_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=format,
                                         version=version)
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def extract_browse_metadata(self, ds_short_name, version, access_url, browse_file,
                                browse_vars={}, output_folder='/tmp', format='BROWSE'):
        """
        Function to extract metadata from Browse files
        :param format:
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param browse_file: Path to Browse file
        :param browse_vars: Browse variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        switcher = {
            "er2edop": mdx.ExtractEr2edopMetadata,
            "dc8lase": mdx.ExtractDc8laseMetadata,
            "gpmsatpaifld": mdx.ExtractGpmsatpaifldPNGMetadata,
            "gpmpipicepop": mdx.ExtractGpmpipicepopPNGMetadata,
            "aces1am": mdx.ExtractAces1amMetadata,
            "tcspecmwf": mdx.ExtractTcspecmwfMetadata,
            "er2mir": mdx.ExtractEr2mirMetadata,
            "dc8ammr": mdx.ExtractDc8ammrMetadata,
            "goesrpltmisrep": mdx.ExtractGoesrpltmisrepMetadata,
            "gpmlipiphx": mdx.ExtractGpmlipiphxPNGMetadata,
            "phipsimpacts": mdx.ExtractPhipsimpactsMetadata,
            "nymesoimpacts": mdx.ExtractNymesoimpactsMetadata,
            "misrepcpexaw": mdx.ExtractMisrepcpexawMetadata
        }

        format_template = {
            "er2edop": "GIF",
            "dc8lase": "GIF",
            "gpmsatpaifld": "PNG",
            "gpmpipicepop": "PNG",
            "aces1am": "MATLAB",
            "tcspecmwf": "GRIB",
            "er2mir": "GIF",
            "dc8ammr": "GIF",
            "goesrpltmisrep": "PNG",
            "gpmlipiphx": "PNG",
            "phipsimpacts": "PNG",
            "nymesoimpacts": "PNG",
            "misrepcpexaw": "Word"
        }

        regex = browse_vars.get('regex', '.*')
        if match(regex, os.path.basename(browse_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(browse_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name,
                                         format=format_template.get(ds_short_name, format),
                                         version=version)
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def extract_avi_metadata(self, ds_short_name, version, access_url, browse_file, browse_vars={},
                             output_folder='/tmp', format='AVI'):
        """
        Function to extract metadata from Browse files
        :param format:
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param browse_file: Path to Browse file
        :param browse_vars: Browse variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        switcher = {
            "gpmpipicepop": mdx.ExtractGpmpipicepopAVIMetadata
        }

        regex = browse_vars.get('regex', '.*')

        if match(regex, os.path.basename(browse_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(browse_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=format,
                                         version=version)
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def extract_legacy_metadata(self, ds_short_name, version, access_url, legacy_file,
                                legacy_vars={}, output_folder='/tmp', format='ASCII'):
        """
        Function to extract metadata from legacy dataset files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param legacy_file: Path to legacy file
        :param legacy_vars: legacy variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """
        regex = legacy_vars.get('regex', '.*')

        if match(regex, os.path.basename(legacy_file)):
            metadata = mdx.ExtractLegacyMetadata(legacy_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=format,
                                         version=version)
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def extract_lookup_metadata(self, ds_short_name, version, access_url, source_file,
                                source_vars={}, output_folder='/tmp', format='Not Provided'):
        """
        Function to extract metadata from source dataset files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param source_file: Path to source file
        :param source_vars: source variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """
        regex = source_vars.get('regex', '.*')

        if match(regex, os.path.basename(source_file)):
            metadata = mdx.ExtractLookupMetadata(source_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=format,
                                         version=version)
            return MDX.generate_json_data(self, data=data, access_url=access_url,
                                          output_folder=output_folder)
        return {}

    def upload_file(self, filename):
        info = self.get_publish_info(filename)
        if info is None:
            return filename
        try:
            uri = None
            if info.get('s3', None) is not None:
                uri = s3.upload(filename, info['s3'], extra={})
            return uri
        except Exception as e:
            logger.error("Error uploading file %s: %s" % (
                os.path.basename(os.path.basename(filename)), str(e)))

    def extract_metadata(self, file_path, config, output_folder):
        """
        High-level extract metadata from file
        :param file_path: Path to file
        :param config: Config passed in through aws step function
        :param output_folder: Location to output created echo10xml file
        :return:
        """
        collection = config.get('collection')
        buckets = config.get('buckets')
        protected_bucket = buckets.get('protected').get('name')
        ds_short_name = collection.get('name')
        version = collection.get('version')
        metadata_extractor_vars = collection.get('meta', {}).get('metadata_extractor', [])
        access_url = os.path.join(config.get('distribution_endpoint'), protected_bucket,
                                  config['fileStagingDir'],
                                  os.path.basename(file_path))
        processing_switcher = {
            "netcdf": self.extract_netcdf_metadata,
            "csv": self.extract_csv_metadata,
            "binary": self.extract_binary_metadata,
            "ascii": self.extract_ascii_metadata,
            "browse": self.extract_browse_metadata,
            "kml": self.extract_kml_metadata,
            "avi": self.extract_avi_metadata,
            "legacy": self.extract_legacy_metadata,
            "lookup": self.extract_lookup_metadata
        }

        return_data_dict = {}
        for metadata_extractor_var in metadata_extractor_vars:
            data = processing_switcher.get(metadata_extractor_var.get('module'),
                                           self.default_switch)(ds_short_name, version, access_url,
                                                                file_path, metadata_extractor_var,
                                                                output_folder)
            return_data_dict = data if data else return_data_dict
        return return_data_dict

    def get_bucket(self, filename, files, buckets):
        """
        Extract the bucket from the files
        :param filename: Granule file name
        :param files: list of collection files
        :param buckets: Object holding buckets info
        :return: Bucket object
        """
        bucket_type = "public"
        for file in files:
            if match(file.get('regex', '*.'), filename):
                bucket_type = file['bucket']
                break
        return buckets[bucket_type]

    def exclude_fetch(self):
        """
        This function is to exclude fetching the granules from specific shortnames
        :return:
        """
        return ["tcspecmwf", "gpmwrflpvex", "relampagolma", "goesrpltavirisng", "gpmvanlpvex",
                "gpmikalpvex", "gpmkorlpvex", "gpmkerlpvex", "gpmkumlpvex", "gpmseafluxicepop",
                "kakqimpacts", "kccximpacts", "kbgmimpacts", "kboximpacts", "kbufimpacts",
                "gpmkarx2ifld", "gpmkdmx2ifld", "gpmkdvn2ifld", "gpmkmpx2ifld", "parprbimpacts"]

    def mutate_input(self, output_folder, input_file):
        """
        Point the input to local folder instead of S3
        :param input_file: The input file from S3
        :param output_folder: The local folder to ouput file to
        :return: path to the file in local machine
        """
        filename = os.path.basename(input_file)
        return [f"{output_folder.rstrip('/')}/{filename}"]

    def upload_output_files(self):
        """
        Uploads all self.output files to same location as input file
        :return: list of files uploaded to S3 (as well as input which should already be in S3)
        """
        upload_output_list = list()
        source_path = os.path.dirname(self.input[0])
        for output_file in self.output:
            output_filename = os.path.basename(output_file)
            uri_out = os.path.join(source_path, output_filename)
            upload_output_list.append(uri_out)
            if output_filename != os.path.basename(self.input[0]):
                try:
                    uri_out_info = s3.uri_parser(uri_out)
                    s3_client = boto3.resource('s3').Bucket(uri_out_info["bucket"]).Object(
                        uri_out_info['key'])
                    with open(output_file, 'rb') as data:
                        s3_client.upload_fileobj(data)
                except Exception as e:
                    logger.error(f'Error uploading file {output_filename}: {str(e)}')
                    raise e from None
        return upload_output_list

    @property
    def input_keys(self):
        return {
            'input_key': r'^(.*)\.?(nc|tsv|txt|gif|tar|zip|png|kml|dat|gz|pdf|docx|kmz|xlsx|eos|csv'
                         r'|hdf5|hdf|nc4|ict|xls|.*rest|h5|xlsx|1Hz|impacts_archive|\d{5}|ar2v|mat'
                         r'|he5|raw|bz2|grb2f\d{4}|_\d{2}-00-00)$',
            'lookup_key': r'^(.*).*$'
        }

    @staticmethod
    def get_output_files(output_file_path, excluded):
        """
        """
        output_files = [] if excluded else [output_file_path]
        if os.path.isfile(f"{output_file_path}.cmr.json"):
            output_files += [f"{output_file_path}.cmr.json"]
        return output_files

    def process(self):
        """
        Override the processing wrapper
        :return:
        """
        logger.info('MDX processing started.')
        self.path = os.getenv('efs_mount_path', self.path)
        granules = self.input['granules']
        cumulus_granules_meta = copy.deepcopy(granules[0])
        [cumulus_granules_meta.pop(ele, False) for ele in ['granuleId', 'files']]
        self.input = []
        for granule in granules:
            for _file in granule['files']:
                if 'metadata' not in _file.get('type', ""):
                    self.input.append(f"s3://{_file['bucket']}/{_file['key']}")
        collection = self.config.get('collection')
        collection_name = collection.get('name')
        collection_version = collection.get('version')
        has_lookup = collection.get('meta', {}).get('metadata_extractor', [])[0].get(
            'module') in ["legacy", "lookup"]
        key = 'lookup_key' if has_lookup else 'input_key'
        
        self.config['fileStagingDir'] = None if 'fileStagingDir' not in self.config.keys() else \
            self.config['fileStagingDir']
        self.config['fileStagingDir'] = f"{collection_name}__{collection_version}" if \
            self.config['fileStagingDir'] is None else self.config['fileStagingDir']

        excluded = collection_name in self.exclude_fetch() or has_lookup
        if excluded:
            self.output.append(self.input[0])
            output = {key: self.mutate_input(self.path, self.input[0])}
        else:
            output = self.fetch_all()
        # Assert we have inputs to process
        assert output[key], "fetched files list should not be empty"
        files_sizes = {}
        for output_file_path in output.get(key):
            data = self.extract_metadata(file_path=output_file_path, config=self.config,
                                         output_folder=self.path)
            generated_files = self.get_output_files(output_file_path, excluded)
            if data.get('UpdatedGranuleUR', False):
                updated_output_path = self.get_output_files(os.path.join(self.path, data['UpdatedGranuleUR']), excluded)
                generated_files.extend(updated_output_path)
            for generated_file in generated_files:
                files_sizes[generated_file.split('/')[-1]] = os.path.getsize(generated_file)
            self.output += generated_files
        temp_output = copy.deepcopy(self.output)
        for ele in temp_output:
            if os.path.basename(ele) in [os.path.basename(base_name) for base_name in self.input]:
                self.output.remove(ele)
        uploaded_files = self.upload_output_files()
        granule_data = {}
        for uploaded_file in uploaded_files:
            if uploaded_file is None or not uploaded_file.startswith('s3'):
                continue
            granule_id = os.path.basename(uploaded_file).split('.cmr.json')[0]
            if granule_id not in granule_data.keys():
                granule_data[granule_id] = {'granuleId': granule_id, 'files': [], **cumulus_granules_meta}
            parsed_uri = s3.uri_parser(uploaded_file)
            granule_data[granule_id]['files'].append(
                    {
                    'bucket': parsed_uri['bucket'],
                    "fileName": os.path.basename(uploaded_file),  # Cumulus changed the key name to be camelCase
                    "key": parsed_uri['key'],
                    "size": files_sizes.get(os.path.basename(uploaded_file), 1983),
                    }
                )
        granule_data_temp = copy.deepcopy(granule_data)
        for granule in granules:
            for granule_ in granule_data_temp:
                if granule['granuleId'] == granule_:
                    granule['files'] += granule_data[granule_]['files']
                    granule_data.pop(granule_)

        for granule_ in granule_data:
            granules.append(granule_data[granule_])

        # Clean up
        for granule_dict in granules:
            for file_dict in granule_dict.get('files'):
                self.delete_file(file_dict.get('fileName'))

        # Workaround for local file since system bucket shouldn't matter locally
        system_bucket_path = uploaded_files[0] if len(uploaded_files) > 0 else \
            f"s3://{os.path.basename(self.input[0])}"
        logger.info('MDX processing completed.')
        return {"granules": granules, "input": uploaded_files,
                "system_bucket": s3.uri_parser(system_bucket_path)['bucket']}

    def delete_file(self, filename):
        path = f'{self.path.rstrip("/")}/{filename}'
        if os.path.exists(path):
            os.remove(path)
            logger.info(f'Deleted: {path}')

    def default_switch(self, *args):
        """
        This is a default switch to pass
        *args is an arbitrary argument
        :return:
        """
        pass


def task(event, context):
    """
    Intermediate task to parse event and initialize MDX
    :param event: AWS event passed into lambda
    :param context: object provides methods and properties that provide information about the
                    invocation, function, and execution environment
    :return: mdx processing output
    """
    logger.info(event)
    mdx_instance = MDX(input=event['input'], config=event['config'])
    return mdx_instance.process()


def handler(event, context):
    """
    Lambda handler entry point which will run the mdx_task
    :param event: AWS event passed into lambda
    :param context: object provides methods and properties that provide information about the
                    invocation, function, and execution environment
    """

    return run_cumulus_task(task, event, context)


if __name__ == '__main__':
    MDX.cli()
