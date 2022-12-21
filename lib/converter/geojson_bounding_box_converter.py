import itertools
import json
import os
from pathlib import Path

from tracking_decorator import TrackingDecorator


def flatten_list(complex_list):
    while not (type(complex_list[0][0]) == float and type(complex_list[0][1]) == float):
        complex_list = list(itertools.chain(*complex_list))

    return complex_list


def extend_by_bounding_box(geojson, clean=False):
    for feature in geojson["features"]:
        if "bounding_box" not in feature["properties"] or clean:
            xmin = None
            ymin = None
            xmax = None
            ymax = None

            geometry = feature["geometry"]
            coordinates = geometry["coordinates"]

            for coordinate in flatten_list(coordinates):

                x = coordinate[0]
                y = coordinate[1]

                if xmin == None or x < xmin:
                    xmin = x
                if ymin == None or y < ymin:
                    ymin = y
                if xmax == None or x > xmax:
                    xmax = x
                if ymax == None or y > ymax:
                    ymax = y

            feature["properties"]["bounding_box"] = [xmin, ymin, xmax, ymax]

    return geojson


target_projection_number = "4326"


#
# Main
#

class GeojsonBoundingBoxConverter:

    @TrackingDecorator.track_time
    def run(self, logger, source_path, results_path, clean=False, quiet=False):

        for path in Path(results_path).glob("*.geojson"):

            file_name = path.name
            source_file_path = os.path.join(source_path, file_name)
            results_file_path = os.path.join(results_path, file_name)

            with open(source_file_path, "r") as geojson_file:
                geojson = json.load(geojson_file)
                projection = str(geojson["crs"]["properties"]["name"])
                projection_number = projection.split(":")[-1]

                if projection_number == target_projection_number or projection_number == "CRS84":
                    geojson_with_bounding_box = extend_by_bounding_box(geojson, clean)

                    with open(results_file_path, "w") as geojson_file:
                        json.dump(geojson_with_bounding_box, geojson_file)

                if not quiet:
                    logger.log_line(f"✓ Convert {file_name}")
