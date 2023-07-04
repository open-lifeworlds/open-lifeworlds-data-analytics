import json
import os

from tracking_decorator import TrackingDecorator

file_path = os.path.realpath(__file__)
script_path = os.path.dirname(file_path)


def read_json_file(file_path):
    with open(file_path, "r") as geojson_file:
        return json.load(geojson_file)


def get_building_group_bounding_box(building_group):
    xmin = None
    ymin = None
    xmax = None
    ymax = None

    for building in building_group["buildings"]:
        if "boundingBox" in building:
            bounding_box = building["boundingBox"]
            building_xmin = bounding_box[0]
            building_ymin = bounding_box[1]
            building_xmax = bounding_box[2]
            building_ymax = bounding_box[3]

            if xmin == None or building_xmin < xmin:
                xmin = building_xmin
            if ymin == None or building_ymin < ymin:
                ymin = building_ymin
            if xmax == None or building_xmax > xmax:
                xmax = building_xmax
            if ymax == None or building_ymax > ymax:
                ymax = building_ymax

    if not xmin is None and not ymin is None and not xmax is None and not ymax is None:
        return [xmin, ymin, xmax, ymax]
    else:
        return None


#
# Main
#

class GesobauBoundingBoxRetriever:

    @TrackingDecorator.track_time
    def run(self, logger, source_path, results_path, clean=False, quiet=False):
        # Make results path
        os.makedirs(os.path.join(results_path), exist_ok=True)

        # Set path
        gesobau_mv_file = os.path.join(source_path, "gesobau_mv.json")

        # Read json file
        gesobau_mv = read_json_file(gesobau_mv_file)

        # Iterate over Gesobau apartments + privatization objects + buildings without residential use
        for building_group in gesobau_mv["gesobauApartments"] + gesobau_mv["privatizationObjects"] + gesobau_mv["buildingsWithoutResidentialUse"]:

            # Check if bounding box needs to be calculated
            if clean or "boundingBox" not in building_group:

                # Calculate bounding box
                bounding_box = get_building_group_bounding_box(building_group)

                if bounding_box is not None:
                    building_group["boundingBox"] = bounding_box

                    if not quiet:
                        logger.log_line(f"✓ Add bounding box to {building_group['id']}")
                else:
                    if not quiet:
                        logger.log_line(f"✗️ No bounding box in {building_group['id']}")

            else:
                if not quiet:
                    logger.log_line(f"✓ Already added bounding box to {building_group['id']}")

        # Write json file
        with open(gesobau_mv_file, "w") as json_file:
            json.dump(gesobau_mv, json_file, indent=2)
