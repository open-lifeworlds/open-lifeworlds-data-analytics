import json
import numbers
import os
from pathlib import Path

import pyproj
from tqdm import tqdm
from tracking_decorator import TrackingDecorator


def convert_to_polar(geojson, target_projection_number, source_projection, target_projection):
    projected_features = []

    for feature in tqdm(iterable=geojson["features"], desc="Convert features", unit="feature"):
        projected_features.append(project_feature(feature, source_projection, target_projection))

    geojson["features"] = projected_features
    geojson["crs"]["properties"]["name"] = f"urn:ogc:def:crs:EPSG::{target_projection_number}"

    return geojson


def project_feature(feature, source_projection, target_projection):
    if not "geometry" in feature or not "coordinates" in feature["geometry"]:
        return None

    converted_coordinates = project_coords(feature["geometry"]["coordinates"], source_projection, target_projection)
    feature["geometry"]["coordinates"] = converted_coordinates
    return feature


def project_coords(coords, source_projection, target_projection):
    if len(coords) < 1:
        return []

    if isinstance(coords[0], numbers.Number):
        lon, lat = coords
        converted_lon, converted_lat = pyproj.transform(source_projection, target_projection, lon, lat)
        return [converted_lon, converted_lat]

    converted_coordinates = []
    for coord in coords:
        converted_coordinates.append(project_coords(coord, source_projection, target_projection))
    return converted_coordinates


target_projection_number = "4326"


#
# Main
#

class GeojsonCopier:

    @TrackingDecorator.track_time
    def run(self, logger, source_path, results_path, clean=False, quiet=False):

        for path in Path(source_path).glob("*.geojson"):

            file_name = path.name

            if file_name in ["bezirksgrenzen.geojson", "lor_prognoseraeume.geojson", "lor_prognoseraeume_2021.geojson",
                             "lor_bezirksregionen.geojson", "lor_bezirksregionen_2021.geojson",
                             "lor_planungsraeume.geojson", "lor_planungsraeume_2021.geojson"]:
                source_file_path = os.path.join(source_path, file_name)
                results_file_path = os.path.join(results_path, file_name)

                # Check if result needs to be generated
                if clean or not os.path.exists(results_file_path):
                    with open(source_file_path, "r") as geojson_file:
                        geojson = json.load(geojson_file)

                    with open(results_file_path, "w") as geojson_file:
                        json.dump(geojson, geojson_file)

                        if not quiet:
                            logger.log_line(f"✓ Copy {results_file_path}")
                else:
                    logger.log_line(f"✓ Already exists {results_file_path}")