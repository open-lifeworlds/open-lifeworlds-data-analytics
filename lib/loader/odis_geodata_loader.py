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

class OdisGeoDataLoader:

    @TrackingDecorator.track_time
    def run(self, logger, results_path, clean=False, quiet=False):
        # Make results path
        os.makedirs(os.path.join(results_path), exist_ok=True)

        base_url = "https://tsb-opendata.s3.eu-central-1.amazonaws.com/"

        for url, file_name in [
            # Lebensweltlich orientierte Räume (LOR) - Bezirksregionen (bis Ende 2020)
            (base_url + "lor_bezirksregionen", "lor_bezirksregionen.csv"),
            (base_url + "lor_bezirksregionen", "lor_bezirksregionen.gml"),
            (base_url + "lor_bezirksregionen", "lor_bezirksregionen.csv"),
            (base_url + "lor_bezirksregionen", "lor_bezirksregionen.shp.zip"),
            (base_url + "lor_bezirksregionen", "lor_bezirksregionen.geojson"),
            (base_url + "lor_bezirksregionen", "lor_bezirksregionen.kml"),
            (base_url + "lor_bezirksregionen", "lor_bezirksregionen.sqlite"),
            # Lebensweltlich orientierte Räume (LOR) - Bezirksregionen (seit 2021)
            (base_url + "lor_bezirksregionen_2021", "lor_bezirksregionen_2021.csv"),
            (base_url + "lor_bezirksregionen_2021", "lor_bezirksregionen_2021.gml"),
            (base_url + "lor_bezirksregionen_2021", "lor_bezirksregionen_2021.csv"),
            (base_url + "lor_bezirksregionen_2021", "lor_bezirksregionen_2021.shp.zip"),
            (base_url + "lor_bezirksregionen_2021", "lor_bezirksregionen_2021.geojson"),
            (base_url + "lor_bezirksregionen_2021", "lor_bezirksregionen_2021.kml"),
            (base_url + "lor_bezirksregionen_2021", "lor_bezirksregionen_2021.sqlite"),
            # Lebensweltlich orientierte Räume (LOR) - Planungsräume (bis Ende 2020)
            (base_url + "lor_planungsgraeume", "lor_planungsraeume.csv"),
            (base_url + "lor_planungsgraeume", "lor_planungsraeume.gml"),
            (base_url + "lor_planungsgraeume", "lor_planungsraeume.csv"),
            (base_url + "lor_planungsgraeume", "lor_planungsraeume.shp.zip"),
            (base_url + "lor_planungsgraeume", "lor_planungsraeume.geojson"),
            (base_url + "lor_planungsgraeume", "lor_planungsraeume.kml"),
            (base_url + "lor_planungsgraeume", "lor_planungsraeume.sqlite"),
            # Lebensweltlich orientierte Räume (LOR) - Planungsräume (seit 2021)
            (base_url + "lor_planungsgraeume_2021", "lor_planungsraeume_2021.csv"),
            (base_url + "lor_planungsgraeume_2021", "lor_planungsraeume_2021.gml"),
            (base_url + "lor_planungsgraeume_2021", "lor_planungsraeume_2021.csv"),
            (base_url + "lor_planungsgraeume_2021", "lor_planungsraeume_2021.shp.zip"),
            (base_url + "lor_planungsgraeume_2021", "lor_planungsraeume_2021.geojson"),
            (base_url + "lor_planungsgraeume_2021", "lor_planungsraeume_2021.kml"),
            (base_url + "lor_planungsgraeume_2021", "lor_planungsraeume_2021.sqlite"),
            # Lebensweltlich orientierte Räume (LOR) - Prognoseräume (bis Ende 2020)
            (base_url + "lor_prognoseraeume", "lor_prognoseraeume.csv"),
            (base_url + "lor_prognoseraeume", "lor_prognoseraeume.gml"),
            (base_url + "lor_prognoseraeume", "lor_prognoseraeume.csv"),
            (base_url + "lor_prognoseraeume", "lor_prognoseraeume.shp.zip"),
            (base_url + "lor_prognoseraeume", "lor_prognoseraeume.geojson"),
            (base_url + "lor_prognoseraeume", "lor_prognoseraeume.kml"),
            (base_url + "lor_prognoseraeume", "lor_prognoseraeume.sqlite"),
            # Lebensweltlich orientierte Räume (LOR) - Prognoseräume (seit 2021)
            (base_url + "lor_prognoseraeume_2021", "lor_prognoseraeume_2021.csv"),
            (base_url + "lor_prognoseraeume_2021", "lor_prognoseraeume_2021.gml"),
            (base_url + "lor_prognoseraeume_2021", "lor_prognoseraeume_2021.csv"),
            (base_url + "lor_prognoseraeume_2021", "lor_prognoseraeume_2021.shp.zip"),
            (base_url + "lor_prognoseraeume_2021", "lor_prognoseraeume_2021.geojson"),
            (base_url + "lor_prognoseraeume_2021", "lor_prognoseraeume_2021.kml"),
            (base_url + "lor_prognoseraeume_2021", "lor_prognoseraeume_2021.sqlite"),
            # Bezirke
            (base_url + "bezirksgrenzen", "bezirksgrenzen.csv"),
            (base_url + "bezirksgrenzen", "bezirksgrenzen.gml"),
            (base_url + "bezirksgrenzen", "bezirksgrenzen.csv"),
            (base_url + "bezirksgrenzen", "bezirksgrenzen.shp.zip"),
            (base_url + "bezirksgrenzen", "bezirksgrenzen.geojson"),
            (base_url + "bezirksgrenzen", "bezirksgrenzen.kml"),
            (base_url + "bezirksgrenzen", "bezirksgrenzen.sqlite"),
            # Block- und Teilblockflächen
            (base_url + "blockflaechen", "blockflaechen.csv"),
            (base_url + "blockflaechen", "blockflaechen.gml"),
            (base_url + "blockflaechen", "blockflaechen.csv"),
            (base_url + "blockflaechen", "blockflaechen.shp.zip"),
            (base_url + "blockflaechen", "blockflaechen.geojson"),
            (base_url + "blockflaechen", "blockflaechen.kml"),
            (base_url + "blockflaechen", "blockflaechen.sqlite"),
            # Detailnetz - Bauwerke
            (base_url + "detailnetz_bauwerke", "Detailnetz-Bauwerke.csv"),
            (base_url + "detailnetz_bauwerke", "Detailnetz-Bauwerke.gml"),
            (base_url + "detailnetz_bauwerke", "Detailnetz-Bauwerke.csv"),
            (base_url + "detailnetz_bauwerke", "Detailnetz-Bauwerke.shp.zip"),
            (base_url + "detailnetz_bauwerke", "Detailnetz-Bauwerke.geojson"),
            (base_url + "detailnetz_bauwerke", "Detailnetz-Bauwerke.kml"),
            (base_url + "detailnetz_bauwerke", "Detailnetz-Bauwerke.sqlite"),
            # Detailnetz - Straßenabschnitte
            (base_url + "detailnetz_strassenabschnitte", "Detailnetz-Strassenabschnitte.csv"),
            (base_url + "detailnetz_strassenabschnitte", "Detailnetz-Strassenabschnitte.gml"),
            (base_url + "detailnetz_strassenabschnitte", "Detailnetz-Strassenabschnitte.csv"),
            (base_url + "detailnetz_strassenabschnitte", "Detailnetz-Strassenabschnitte.shp.zip"),
            (base_url + "detailnetz_strassenabschnitte", "Detailnetz-Strassenabschnitte.geojson"),
            (base_url + "detailnetz_strassenabschnitte", "Detailnetz-Strassenabschnitte.kml"),
            (base_url + "detailnetz_strassenabschnitte", "Detailnetz-Strassenabschnitte.sqlite"),
            # Flure
            (base_url + "flure", "flure.csv"),
            (base_url + "flure", "flure.gml"),
            (base_url + "flure", "flure.csv"),
            (base_url + "flure", "flure.shp.zip"),
            (base_url + "flure", "flure.geojson"),
            (base_url + "flure", "flure.kml"),
            (base_url + "flure", "flure.sqlite"),
            # Gebäude
            (base_url + "gebaeude", "gebaeude.csv"),
            (base_url + "gebaeude", "gebaeude.shp.zip"),
            (base_url + "gebaeude", "gebaeude.sqlite"),
            # Ortsteile
            (base_url + "ortsteile", "lor_ortsteile.csv"),
            (base_url + "ortsteile", "lor_ortsteile.gml"),
            (base_url + "ortsteile", "lor_ortsteile.csv"),
            (base_url + "ortsteile", "lor_ortsteile.shp.zip"),
            (base_url + "ortsteile", "lor_ortsteile.geojson"),
            (base_url + "ortsteile", "lor_ortsteile.kml"),
            (base_url + "ortsteile", "lor_ortsteile.sqlite"),
            # Postleitzahlgebiete
            (base_url + "plz", "plz.csv"),
            (base_url + "plz", "plz.gml"),
            (base_url + "plz", "plz.csv"),
            (base_url + "plz", "plz.shp.zip"),
            (base_url + "plz", "plz.geojson"),
            (base_url + "plz", "plz.kml"),
            (base_url + "plz", "plz.sqlite"),
            # Straßenflächen
            (base_url + "strassenflaechen", "strassenflaechen.csv"),
            (base_url + "strassenflaechen", "strassenflaechen.gml"),
            (base_url + "strassenflaechen", "strassenflaechen.csv"),
            (base_url + "strassenflaechen", "strassenflaechen.shp.zip"),
            (base_url + "strassenflaechen", "strassenflaechen.geojson"),
            (base_url + "strassenflaechen", "strassenflaechen.kml"),
            (base_url + "strassenflaechen", "strassenflaechen.sqlite"),
            # Teilverkehrszellen
            (base_url + "teil_verkehrszellen", "teil_verkehrszellen.csv"),
            (base_url + "teil_verkehrszellen", "teil_verkehrszellen.gml"),
            (base_url + "teil_verkehrszellen", "teil_verkehrszellen.csv"),
            (base_url + "teil_verkehrszellen", "teil_verkehrszellen.shp.zip"),
            (base_url + "teil_verkehrszellen", "teil_verkehrszellen.geojson"),
            (base_url + "teil_verkehrszellen", "teil_verkehrszellen.kml"),
            (base_url + "teil_verkehrszellen", "teil_verkehrszellen.sqlite"),
            # Verkehrszellen
            (base_url + "verkehrszellen", "verkehrszellen.csv"),
            (base_url + "verkehrszellen", "verkehrszellen.gml"),
            (base_url + "verkehrszellen", "verkehrszellen.csv"),
            (base_url + "verkehrszellen", "verkehrszellen.shp.zip"),
            (base_url + "verkehrszellen", "verkehrszellen.geojson"),
            (base_url + "verkehrszellen", "verkehrszellen.kml"),
            (base_url + "verkehrszellen", "verkehrszellen.sqlite"),
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
