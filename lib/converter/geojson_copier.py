import json
import os
from pathlib import Path

from tracking_decorator import TrackingDecorator


#
# Main
#

class GeojsonCopier:

    @TrackingDecorator.track_time
    def run(self, logger, source_path, results_path, clean=False, quiet=False):
        # Make results path
        os.makedirs(os.path.join(results_path), exist_ok=True)

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
