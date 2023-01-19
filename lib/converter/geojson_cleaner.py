import copy
import json
import os
from pathlib import Path

from tracking_decorator import TrackingDecorator


def clean_geometry(geojson):
    changed = False

    for feature in geojson["features"]:
        polygons = feature["geometry"]["coordinates"]

        if len(polygons) > 1:
            polygon_max = [[]]
            for polygon in polygons:
                if len(polygon[0]) > len(polygon_max[0]):
                    polygon_max = polygon

            feature["geometry"]["coordinates"] = polygon_max
            changed = True

    return changed


#
# Main
#

class GeojsonCleaner:

    @TrackingDecorator.track_time
    def run(self, logger, source_path, results_path, clean=False, quiet=False):

        for path in Path(source_path).glob("*.geojson"):

            file_name = path.name

            if file_name in ["bezirksgrenzen.geojson", "lor_prognoseraeume.geojson", "lor_prognoseraeume_2021.geojson",
                             "lor_bezirksregionen.geojson", "lor_bezirksregionen_2021.geojson",
                             "lor_planungsraeume.geojson", "lor_planungsraeume_2021.geojson"]:
                source_file_path = os.path.join(source_path, file_name)
                results_file_path = os.path.join(results_path, file_name)

                with open(source_file_path, "r") as geojson_file:
                    geojson = json.load(geojson_file)

                changed = clean_geometry(geojson)

                if changed:
                    with open(results_file_path, "w") as geojson_file:
                        json.dump(geojson, geojson_file)

                        if not quiet:
                            logger.log_line(f"✓ Clean {results_file_path}")
                else:
                    if not quiet:
                        logger.log_line(f"✓ Already cleaned {results_file_path}")
