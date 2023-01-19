import getopt
import os
import sys

file_path = os.path.realpath(__file__)
script_path = os.path.dirname(file_path)

# Make library available in path
library_paths = [
    os.path.join(script_path, "lib"),
    os.path.join(script_path, "lib", "log"),
    os.path.join(script_path, "lib", "loader"),
    os.path.join(script_path, "lib", "converter"),
    os.path.join(script_path, "lib", "blender"),
    os.path.join(script_path, "lib", "aggregator"),
]

for p in library_paths:
    if not (p in sys.path):
        sys.path.insert(0, p)

# Import library classes
from logger_facade import LoggerFacade
from odis_geodata_loader import OdisGeoDataLoader
from lor_statistics_data_loader import LorStatisticsDataLoader
from lor_senate_data_loader import LorSenateDataLoader
from geojson_copier import GeojsonCopier
from geojson_cleaner import GeojsonCleaner
from geojson_projection_converter import GeojsonProjectionConverter
from geojson_bounding_box_converter import GeojsonBoundingBoxConverter
from data_blender import DataBlender
from data_aggregator import DataAggregator
from tracking_decorator import TrackingDecorator


#
# Main
#

@TrackingDecorator.track_time
def main(argv):
    # Set default values
    clean = False
    quiet = False

    # Read command line arguments
    try:
        opts, args = getopt.getopt(argv, "hcqp:", ["help", "clean", "quiet"])
    except getopt.GetoptError:
        print("main.py --help --clean --quiet")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print("main.py")
            print("--help                           show this help")
            print("--clean                          clean intermediate results before start")
            print("--quiet                          do not log outputs")
            sys.exit()
        elif opt in ("-c", "--clean"):
            clean = True
        elif opt in ("-q", "--quiet"):
            quiet = True

    # Set paths
    data_path = os.path.join(script_path, "data")
    raw_path = os.path.join(script_path, "raw")
    statistics_population_path = os.path.join(raw_path, "lor-statistics-population")

    # Initialize logger
    logger = LoggerFacade(data_path, console=True, file=False)

    # Download data
    OdisGeoDataLoader().run(logger, os.path.join(raw_path, "lor-odis-geo"), clean=clean, quiet=quiet)
    LorSenateDataLoader().run(logger, os.path.join(raw_path, "lor-senate"), clean=clean, quiet=quiet)

    # Download statistics data
    LorStatisticsDataLoader().run(logger, os.path.join(raw_path, statistics_population_path), clean=clean, quiet=quiet)

    # Convert data
    GeojsonCopier().run(logger, os.path.join(raw_path, "lor-odis-geo"), data_path, clean=True, quiet=quiet)
    GeojsonCleaner().run(logger, data_path, data_path, clean=clean, quiet=quiet)
    GeojsonProjectionConverter().run(logger, data_path, data_path, clean=clean, quiet=quiet)
    GeojsonBoundingBoxConverter().run(logger, data_path, data_path, clean=clean, quiet=quiet)

    # Blend data into geojson
    DataBlender().run(logger, data_path=data_path, statistics_path=statistics_population_path, results_path=data_path,
                      clean=True, quiet=quiet)

    # Aggregate data
    DataAggregator().run(logger, data_path=data_path, statistics_path=statistics_population_path, results_path=data_path, clean=True, quiet=quiet)


if __name__ == "__main__":
    main(sys.argv[1:])
