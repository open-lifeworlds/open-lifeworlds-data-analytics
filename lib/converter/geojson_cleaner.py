import json
import os
from collections import Sequence
from itertools import chain, count
from pathlib import Path

from tracking_decorator import TrackingDecorator


def unify_properties(geojson):
    changed = False

    for feature in geojson["features"]:
        properties = feature["properties"]

        id = None
        name = None
        area = None

        # Iterate over potential ID properties
        for id_property in ["Gemeinde_schluessel", "broker Dow", "PGR_ID", "BZR_ID", "PLR_ID"]:
            if id_property in properties:
                id = properties[id_property]

                if id_property == "Gemeinde_schluessel":
                    id = id[1:]

                properties.pop(id_property, None)

        # Iterate over potential name properties
        for name_property in ["Gemeinde_name", "PROGNOSERA", "PGR_NAME", "BEZIRKSREG", "BZR_NAME", "PLANUNGSRA",
                              "PLR_NAME"]:
            if name_property in properties:
                name = properties[name_property]
                properties.pop(name_property, None)

        # Iterate over potential area properties
        for area_property in ["FLAECHENGR", "GROESSE_m2", "GROESSE_M2"]:
            if area_property in properties:
                area = properties[area_property]
                properties.pop(area_property, None)

        if id is not None:
            properties["id"] = id
            changed = True
        if name is not None:
            properties["name"] = name
            changed = True
        if area is not None:
            properties["area"] = area
            changed = True

    return changed


def clean_geometry(logger, geojson, quiet):
    changed = False

    for feature in geojson["features"]:
        id = feature["properties"]["id"]
        name = feature["properties"]["name"]
        polygons = feature["geometry"]["coordinates"]

        # Check if there is more than one polygon
        if len(polygons) > 1:
            polygon_max = [[]]

            for polygon in polygons:
                polygon_elements_count = len(polygon[0])
                polygon_elements_count_max = len(polygon_max[0])

                if polygon_elements_count > polygon_elements_count_max:
                    polygon_max = polygon

            feature["geometry"]["coordinates"] = [polygon_max]
            changed = True

            if not quiet:
                logger.log_line(f"⚠ Clean geometry of {id} {name}")

        # Sanity-check geometry
        if get_depth(feature["geometry"]["coordinates"]) != 4 and not quiet:
            logger.log_line(f"⚠ Invalid geometry of {id} {name}")

    return changed


# Thanks https://stackoverflow.com/a/6040217/2992219
def get_depth(seq):
    for level in count():
        if not seq:
            return level
        seq = list(chain.from_iterable(s for s in seq if isinstance(s, Sequence)))


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

                changed = False
                changed |= unify_properties(geojson)
                changed |= clean_geometry(logger, geojson, quiet)

                if changed:
                    with open(results_file_path, "w", encoding="utf-8") as geojson_file:
                        json.dump(geojson, geojson_file, ensure_ascii=False)

                        if not quiet:
                            logger.log_line(f"✓ Clean {results_file_path}")
                else:
                    if not quiet:
                        logger.log_line(f"✓ Already cleaned {results_file_path}")
