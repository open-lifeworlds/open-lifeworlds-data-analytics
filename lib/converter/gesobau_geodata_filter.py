import json
import os
from pathlib import Path

from tracking_decorator import TrackingDecorator


#
# Main
#

class GesobauGeodataFilter:

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

                with open(source_file_path, "r") as geojson_file:
                    geojson = json.load(geojson_file)

                    # Filter features
                    filtered_features = []
                    for feature in geojson['features']:
                        properties = feature['properties']
                        if "Gemeinde_schluessel" in properties and properties["Gemeinde_schluessel"] in [
                            # bezirksgrenzen.geojson
                            "012"  # Reinickendorf
                        ] or "broker Dow" in properties and properties["broker Dow"] in [
                            # lor_prognoseraeume.geojson
                            "1230"  # Waidmannslust
                        ] or "PGR_ID" in properties and properties["PGR_ID"] in [
                            # lor_prognoseraeume_2021.geojson
                            "1260"  # Märkisches Viertel
                        ] or "broker Dow" in properties and properties["broker Dow"] in [
                            # lor_bezirksregionen.geojson
                            "123021"  # MV 1 - Märkisches Viertel
                        ] or "BZR_ID" in properties and properties["BZR_ID"] in [
                            # lor_bezirksregionen_2021.geojson
                            "126011",  # MV Nord
                            "126012"  # MV Süd
                        ] or "broker Dow" in properties and properties["broker Dow"] in [
                            # lor_planungsraeume.geojson
                            "12302108",  # Märkisches Zentrum
                            "12302109",  # Treuenbrietzener Straße
                            "12302110",  # Dannenwalder Weg
                            "12302107"  # Schorfheidestraße
                        ] or "PLR_ID" in properties and properties["PLR_ID"] in [
                            # lor_planungsraeume_2021.geojson
                            "12601134",  # Märkisches Zentrum
                            "12601133",  # Treuenbrietzener Straße
                            "12601235",  # Dannenwalder Weg
                            "12601236"  # Schorfheidestraße
                        ]:
                            filtered_features.append(feature)
                    geojson["features"] = filtered_features

                    with open(results_file_path, "w") as geojson_file:
                        json.dump(geojson, geojson_file)

                    if not quiet:
                        logger.log_line(f"✓ Filter {file_name} ({len(filtered_features)} features remaining)")
