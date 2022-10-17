import os

import requests
from tracking_decorator import TrackingDecorator


def download_file(logger, file_path, url):
    try:
        data = requests.get(url)
        with open(file_path, 'wb') as file:
            file.write(data.content)
    except Exception as e:
        logger.log_line(f"✗️ Exception: {str(e)}")
        return None


#
# Main
#

class LorSenateDataLoader:

    @TrackingDecorator.track_time
    def run(self, logger, results_path, clean=False, quiet=False):
        # Make results path
        os.makedirs(os.path.join(results_path), exist_ok=True)

        base_url = "https://www.stadtentwicklung.berlin.de/planen/basisdaten_stadtentwicklung/lor/download"

        for url, file_name in [
            # ETRS89 UTM 33 N DXF (01.01.2019)
            (base_url + "", "LOR_DXF_2019.zip"),
            # ETRS89 UTM 33 N SHP (01.01.2019)
            (base_url + "", "LOR_SHP_2019.zip"),
            # WGS 84 KMZ (01.01.2019)
            (base_url + "", "LOR_KMZ_2019.zip"),
            # Namensverzeichnis (01.01.2019)
            (base_url + "", "LOR-Schluesselsystematik_2019.xls"),

            # ETRS89 UTM 33 N DXF (März 2015)
            (base_url + "", "LOR_DXF_EPSG_25833.zip"),
            # ETRS89 UTM 33 N SHP (März 2015)
            (base_url + "", "LOR_SHP_EPSG_25833.zip"),
            # Soldner Berlin DXF (März 2015)
            (base_url + "", "LOR_DXF_EPSG_3068.zip"),
            # Soldner Berlin SHP (März 2015)
            (base_url + "", "LOR_SHP_EPSG_3068.zip"),
            # WGS 84 KMZ (März 2015)
            (base_url + "", "LOR_KMZ_4326.zip"),
            # Flächennutzung der Planungsräume 2015 (März 2015)
            (base_url + "", "lor_nutzung_ISU5_2015.xls"),
            # Namensverzeichnis (März 2015)
            (base_url + "", "LOR-Schluesselsystematik.xls"),

            # Übersichtskarte LOR (Januar 2021)
            (base_url + "", "LOR_2021_Kartenseite.pdf"),
            # Registerseite der Übersichtskarte LOR (Januar 2021)
            (base_url + "", "LOR_2021_Registerseite.pdf"),
            # Bezirkskarten LOR, Stand Januar 2021
            (base_url + "", "LOR_2021_Bezirkskarten_DINA4.pdf"),
            # Dokumentation zur Modifikation der LOR 2020
            (base_url + "", "Dokumentation_zur_Modifikation_LOR_2020.pdf"),
        ]:

            # Define file path
            file_path = os.path.join(results_path, file_name)

            # Check if result needs to be generated
            if clean or not os.path.exists(file_path):

                download_file(
                    logger=logger,
                    file_path=file_path,
                    url=f"{url}/{file_name}"
                )

                if not quiet:
                    logger.log_line(f"✓ Download {file_path}")
            else:
                logger.log_line(f"✓ Already exists {file_path}")
