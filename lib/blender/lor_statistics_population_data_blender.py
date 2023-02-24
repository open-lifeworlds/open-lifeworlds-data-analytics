import copy
import json
import os

import pandas as pd
from tracking_decorator import TrackingDecorator


def read_csv_file(file_path):
    if "None" not in file_path:
        with open(file_path, "r") as csv_file:
            return pd.read_csv(csv_file)
    else:
        return None


def read_geojson_file(file_path):
    with open(file_path, "r") as geojson_file:
        return json.load(geojson_file)


def write_geojson_file(logger, file_path, statistic_name, geojson_content, clean, quiet):
    if not os.path.exists(file_path) or clean:
        file_name = os.path.basename(file_path)

        with open(file_path, "w") as geojson_file:
            json.dump(geojson_content, geojson_file)

            if not quiet:
                logger.log_line(f"✓ Blend data from {statistic_name} into {file_name}")


def write_json_file(logger, file_path, statistic_name, json_content, clean, quiet):
    if not os.path.exists(file_path) or clean:
        file_name = os.path.basename(file_path)

        with open(file_path, "w") as json_file:
            json.dump(json_content, json_file)

            if not quiet:
                logger.log_line(f"✓ Aggregate data from {statistic_name} into {file_name}")


pre_2020_statistics = [
    "SB_A01-06-00_2015h01_BE",
    "SB_A01-06-00_2015h02_BE",
    "SB_A01-16-00_2016h01_BE",
    "SB_A01-16-00_2016h02_BE",
    "SB_A01-16-00_2017h01_BE",
    "SB_A01-16-00_2017h02_BE",
    "SB_A01-16-00_2018h01_BE",
    "SB_A01-16-00_2018h02_BE",
    "SB_A01-16-00_2019h01_BE",
    "SB_A01-16-00_2019h02_BE",
]
exactly_2020_statistics = [
    "SB_A01-16-00_2020h02_BE"
]
post_2020_statistics = [
    "SB_A01-16-00_2021h01_BE",
    "SB_A01-16-00_2021h02_BE",
    "SB_A01-16-00_2022h01_BE"
]


def extend_districts(logger, statistics, year, half_year,
                     statistic, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
                     geojson, id_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        district_id = feature_id

        # Filter statistics
        statistic_t1_filtered = statistic_t1.loc[(statistic_t1["bezirk"] == int(district_id))]
        statistic_t2_filtered = statistic_t2.loc[(statistic_t2["bezirk"] == int(district_id))]
        statistic_t3_filtered = statistic_t3.loc[(statistic_t3["bezirk"] == int(district_id))]
        statistic_t4_filtered = statistic_t4.loc[(statistic_t4["bezirk"] == int(district_id))]

        # Check for missing data
        if statistic_t1_filtered.shape[0] == 0 or statistic_t2_filtered.shape[0] == 0 or \
                statistic_t3_filtered.shape[0] == 0 or statistic_t4_filtered.shape[0] == 0 or \
                int(statistic_t1_filtered["insgesamt_anzahl"].sum()) == 0 or \
                int(statistic_t1_filtered["darunter_mit_migrationshintergrund_anzahl"].sum()) == 0:
            logger.log_line(f"✗️ No data in {statistic} for district={district_id}")
            continue

        if district_id == "01":
            area_sqkm = 39.34
        elif district_id == "02":
            area_sqkm = 20.36
        elif district_id == "03":
            area_sqkm = 103.10
        elif district_id == "04":
            area_sqkm = 59.76
        elif district_id == "05":
            area_sqkm = 91.74
        elif district_id == "06":
            area_sqkm = 102.40
        elif district_id == "07":
            area_sqkm = 52.93
        elif district_id == "08":
            area_sqkm = 44.89
        elif district_id == "09":
            area_sqkm = 167.41
        elif district_id == "10":
            area_sqkm = 61.77
        elif district_id == "11":
            area_sqkm = 52.02
        elif district_id == "12":
            area_sqkm = 89.19
        else:
            area_sqkm = None

        # Blend data
        feature = blend_data(
            feature=feature, area_sqkm=area_sqkm,
            statistic_t1=statistic_t1_filtered,
            statistic_t2=statistic_t2_filtered,
            statistic_t3=statistic_t3_filtered,
            statistic_t4=statistic_t4_filtered
        )

        # Build structure
        if year not in statistics:
            statistics[year] = {}
        if half_year not in statistics[year]:
            statistics[year][half_year] = {}

        # Add properties
        statistics[year][half_year][feature_id] = feature["properties"]

    return geojson_extended


def extend_forecast_areas(logger, statistics, year, half_year,
                          statistic, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
                          geojson, id_property, area_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        district_id, forecast_area_id, district_region_id, planning_area_id = build_ids(feature_id)
        area_sqm = feature["properties"][area_property]
        area_sqkm = area_sqm / 1_000_000

        # Filter statistics
        statistic_t1_filtered = statistic_t1.loc[
            (statistic_t1["bezirk"] == int(district_id)) &
            (statistic_t1["prognoseraum"] == int(forecast_area_id))]
        statistic_t2_filtered = statistic_t2.loc[
            (statistic_t2["bezirk"] == int(district_id)) &
            (statistic_t2["prognoseraum"] == int(forecast_area_id))]
        statistic_t3_filtered = statistic_t3.loc[
            (statistic_t3["bezirk"] == int(district_id)) &
            (statistic_t3["prognoseraum"] == int(forecast_area_id))]
        statistic_t4_filtered = statistic_t4.loc[
            (statistic_t4["bezirk"] == int(district_id)) &
            (statistic_t4["prognoseraum"] == int(forecast_area_id))]

        # Check for missing data
        if statistic_t1_filtered.shape[0] == 0 or statistic_t2_filtered.shape[0] == 0 or \
                statistic_t3_filtered.shape[0] == 0 or statistic_t4_filtered.shape[0] == 0 or \
                statistic_t1_filtered["insgesamt_anzahl"].sum() == 0 or \
                statistic_t1_filtered["darunter_mit_migrationshintergrund_anzahl"].sum() == 0:
            logger.log_line(
                f"✗️ No data in {statistic} for district={district_id}, forecast area={forecast_area_id}")
            continue

        # Blend data
        blend_data(
            feature=feature, area_sqkm=area_sqkm,
            statistic_t1=statistic_t1_filtered,
            statistic_t2=statistic_t2_filtered,
            statistic_t3=statistic_t3_filtered,
            statistic_t4=statistic_t4_filtered
        )

        # Build structure
        if year not in statistics:
            statistics[year] = {}
        if half_year not in statistics[year]:
            statistics[year][half_year] = {}

        # Add properties
        statistics[year][half_year][feature_id] = feature["properties"]

    return geojson_extended


def extend_district_regions(logger, statistics, year, half_year,
                            statistic, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
                            geojson, id_property, area_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        district_id, forecast_area_id, district_region_id, planning_area_id = build_ids(feature_id)
        area_sqm = feature["properties"][area_property]
        area_sqkm = area_sqm / 1_000_000

        # Filter statistics
        statistic_t1_filtered = statistic_t1.loc[
            (statistic_t1["bezirk"] == int(district_id)) &
            (statistic_t1["prognoseraum"] == int(forecast_area_id)) &
            (statistic_t1["bezirksregion"] == int(district_region_id))]
        statistic_t2_filtered = statistic_t2.loc[
            (statistic_t2["bezirk"] == int(district_id)) &
            (statistic_t2["prognoseraum"] == int(forecast_area_id)) &
            (statistic_t2["bezirksregion"] == int(district_region_id))]
        statistic_t3_filtered = statistic_t3.loc[
            (statistic_t3["bezirk"] == int(district_id)) &
            (statistic_t3["prognoseraum"] == int(forecast_area_id)) &
            (statistic_t3["bezirksregion"] == int(district_region_id))]
        statistic_t4_filtered = statistic_t4.loc[
            (statistic_t4["bezirk"] == int(district_id)) &
            (statistic_t4["prognoseraum"] == int(forecast_area_id)) &
            (statistic_t4["bezirksregion"] == int(district_region_id))]

        # Check for missing data
        if statistic_t1_filtered.shape[0] == 0 or statistic_t2_filtered.shape[0] == 0 or \
                statistic_t3_filtered.shape[0] == 0 or statistic_t4_filtered.shape[0] == 0 or \
                statistic_t1_filtered["insgesamt_anzahl"].sum() == 0 or \
                statistic_t1_filtered["darunter_mit_migrationshintergrund_anzahl"].sum() == 0:
            logger.log_line(
                f"✗️ No data in {statistic} for district={district_id}, forecast area={forecast_area_id}, "
                f"district_region_id={district_region_id}")
            continue

        # Blend data
        feature = blend_data(
            feature=feature, area_sqkm=area_sqkm,
            statistic_t1=statistic_t1_filtered,
            statistic_t2=statistic_t2_filtered,
            statistic_t3=statistic_t3_filtered,
            statistic_t4=statistic_t4_filtered
        )

        # Build structure
        if year not in statistics:
            statistics[year] = {}
        if half_year not in statistics[year]:
            statistics[year][half_year] = {}

        # Add properties
        statistics[year][half_year][feature_id] = feature["properties"]

    return geojson_extended


def extend_planning_areas(logger, statistics, year, half_year,
                          statistic, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
                          geojson, id_property, area_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        district_id, forecast_area_id, district_region_id, planning_area_id = build_ids(feature_id)
        area_sqm = feature["properties"][area_property]
        area_sqkm = area_sqm / 1_000_000

        # Filter statistics
        statistic_t1_filtered = statistic_t1.loc[
            (statistic_t1["bezirk"] == int(district_id)) &
            (statistic_t1["prognoseraum"] == int(forecast_area_id)) &
            (statistic_t1["bezirksregion"] == int(district_region_id)) &
            (statistic_t1["planungsraum"] == int(planning_area_id))]
        statistic_t2_filtered = statistic_t2.loc[
            (statistic_t2["bezirk"] == int(district_id)) &
            (statistic_t2["prognoseraum"] == int(forecast_area_id)) &
            (statistic_t2["bezirksregion"] == int(district_region_id)) &
            (statistic_t2["planungsraum"] == int(planning_area_id))]
        statistic_t3_filtered = statistic_t3.loc[
            (statistic_t3["bezirk"] == int(district_id)) &
            (statistic_t3["prognoseraum"] == int(forecast_area_id)) &
            (statistic_t3["bezirksregion"] == int(district_region_id)) &
            (statistic_t3["planungsraum"] == int(planning_area_id))]
        statistic_t4_filtered = statistic_t4.loc[
            (statistic_t4["bezirk"] == int(district_id)) &
            (statistic_t4["prognoseraum"] == int(forecast_area_id)) &
            (statistic_t4["bezirksregion"] == int(district_region_id)) &
            (statistic_t4["planungsraum"] == int(planning_area_id))]

        # Check for missing data
        if statistic_t1_filtered.shape[0] == 0 or statistic_t2_filtered.shape[0] == 0 or \
                statistic_t3_filtered.shape[0] == 0 or statistic_t4_filtered.shape[0] == 0 or \
                statistic_t1_filtered["insgesamt_anzahl"].sum() == 0 or \
                statistic_t1_filtered["darunter_mit_migrationshintergrund_anzahl"].sum() == 0:
            logger.log_line(
                f"✗️ No data in {statistic} for district={district_id}, forecast area={forecast_area_id}, "
                f"district_region_id={district_region_id}, planning_area_id={planning_area_id}")
            continue

        # Blend data
        feature = blend_data(
            feature=feature, area_sqkm=area_sqkm,
            statistic_t1=statistic_t1_filtered,
            statistic_t2=statistic_t2_filtered,
            statistic_t3=statistic_t3_filtered,
            statistic_t4=statistic_t4_filtered
        )

        # Build structure
        if year not in statistics:
            statistics[year] = {}
        if half_year not in statistics[year]:
            statistics[year][half_year] = {}

        # Add properties
        statistics[year][half_year][feature_id] = feature["properties"]

    return geojson_extended


def build_ids(combined_id):
    return combined_id[0:2], combined_id[2:4], combined_id[4:6], combined_id[6:8]


def blend_data(feature, area_sqkm, statistic_t1, statistic_t2, statistic_t3, statistic_t4):
    # Lookup data
    inhabitants = statistic_t1["insgesamt_anzahl"].sum()

    # Add new properties
    add_prop_with_modifiers(feature, "inhabitants", statistic_t1, "insgesamt_anzahl", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background", statistic_t1,
                            "darunter_mit_migrationshintergrund_anzahl", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_germans", statistic_t1, "deutsche_zusammen_anzahl", inhabitants,
                            area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_germans_without_migration_background", statistic_t1,
                            "deutsche_ohne_migrationshintergrund_anzahl", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_germans_with_migration_background", statistic_t1,
                            "deutsche_mit_migrationshintergrund_anzahl", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_foreigners", statistic_t1, "auslaender_anzahl", inhabitants,
                            area_sqkm)

    add_prop_with_modifiers(feature, "inhabitants_age_below_6", statistic_t2, "alter_unter_6", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_age_6_15", statistic_t2, "alter_6-15", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_age_15_18", statistic_t2, "alter_15-18", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_age_18_27", statistic_t2, "alter_18-27", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_age_27_45", statistic_t2, "alter_27-45", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_age_45_55", statistic_t2, "alter_45-55", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_age_55_65", statistic_t2, "alter_55-65", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_age_above_65", statistic_t2, "alter_65_und_mehr", inhabitants,
                            area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_female", statistic_t2, "weiblich", inhabitants, area_sqkm)

    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_age_below_6", statistic_t3, "alter_unter_6",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_age_6_15", statistic_t3, "alter_6-15",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_age_15_18", statistic_t3, "alter_15-18",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_age_18_27", statistic_t3, "alter_18-27",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_age_27_45", statistic_t3, "alter_27-45",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_age_45_55", statistic_t3, "alter_45-55",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_age_55_65", statistic_t3, "alter_55-65",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_age_above_65", statistic_t3,
                            "alter_65_und_mehr", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_with_migration_background_female", statistic_t3, "weiblich",
                            inhabitants, area_sqkm)

    add_prop_with_modifiers(feature, "inhabitants_from_european_union", statistic_t4, "europaeische_union", inhabitants,
                            area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_france", statistic_t4, "frankreich", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_greece", statistic_t4, "griechenland", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_italy", statistic_t4, "spanien", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_austria", statistic_t4, "oesterreich", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_spain", statistic_t4, "spanien", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_poland", statistic_t4, "polen", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_bulgaria", statistic_t4, "bulgarien", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_rumania", statistic_t4, "rumaenien", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_croatia", statistic_t4, "kroatien", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_united_kingdom", statistic_t4, "vereinigtes_koenigreich",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_former_yugoslavia", statistic_t4, "ehemaliges_jugoslawien",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_bosnia_herzegovina", statistic_t4, "bosnien_und_herzegowina",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_serbia", statistic_t4, "serbien", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_former_soviet_union", statistic_t4, "ehemalige_sowjetunion",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_russia", statistic_t4, "russische_foederation", inhabitants,
                            area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_ukraine", statistic_t4, "ukraine", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_kazakhstan", statistic_t4, "kasachstan", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_islamic_countries", statistic_t4, "islamische_laender",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_turkey", statistic_t4, "tuerkei", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_iran", statistic_t4, "iran", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_arabic_countries", statistic_t4, "arabische_laender",
                            inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_lebanon", statistic_t4, "libanon", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_syria", statistic_t4, "syrien", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_vietnam", statistic_t4, "vietnam", inhabitants, area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_united_states", statistic_t4, "vereinigte_staaten", inhabitants,
                            area_sqkm)
    add_prop_with_modifiers(feature, "inhabitants_from_undefined", statistic_t4,
                            "nicht_eindeutig_zuordenbar_ohne_angabe", inhabitants, area_sqkm)

    return feature


def add_prop(feature, property_name, statistics, statistics_property_name):
    if statistics_property_name in statistics:
        try:
            feature["properties"][f"{property_name}"] = float(statistics[statistics_property_name])
        except ValueError:
            feature["properties"][f"{property_name}"] = 0


def add_prop_with_modifiers(feature, property_name, statistics, statistics_property_name, inhabitants, total_area_sqkm):
    if statistics_property_name in statistics:
        try:
            feature["properties"][f"{property_name}"] = float(statistics[statistics_property_name].sum())
            if inhabitants is not None:
                feature["properties"][f"{property_name}_percentage"] = round(
                    float(statistics[statistics_property_name].sum()) / inhabitants * 100, 2)
            if total_area_sqkm is not None:
                feature["properties"][f"{property_name}_per_sqkm"] = round(
                    float(statistics[statistics_property_name].sum()) / total_area_sqkm)
        except ValueError:
            feature["properties"][f"{property_name}"] = 0

            if inhabitants is not None:
                feature["properties"][f"{property_name}_percentage"] = 0
            if total_area_sqkm is not None:
                feature["properties"][f"{property_name}_per_sqkm"] = 0


key_figure_group = "population"


#
# Main
#

class LorStatisticsPopulationDataBlender:

    @TrackingDecorator.track_time
    def run(self, logger, data_path, statistics_path, results_path, clean=False, quiet=False):
        # Make results path
        os.makedirs(os.path.join(results_path), exist_ok=True)

        # Statistics
        statistics_lor_districts = {}
        statistics_lor_forecast_areas = {}
        statistics_lor_district_regions = {}
        statistics_lor_planning_areas = {}

        # Load geojson
        geojson_lor_districts = read_geojson_file(os.path.join(data_path, "bezirksgrenzen.geojson"))
        geojson_lor_forecast_areas = read_geojson_file(os.path.join(data_path, "lor_prognoseraeume.geojson"))
        geojson_lor_district_regions = read_geojson_file(os.path.join(data_path, "lor_bezirksregionen.geojson"))
        geojson_lor_planning_areas = read_geojson_file(os.path.join(data_path, "lor_planungsraeume.geojson"))

        # Iterate over statistics
        for statistic in pre_2020_statistics:
            year = statistic.split(sep="_")[2].split(sep="h")[0]
            half_year = statistic.split(sep="_")[2].split(sep="h")[1]

            # Load statistics
            statistic_t1 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T1.csv"))
            statistic_t2 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T2.csv"))
            statistic_t3 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T3.csv"))
            statistic_t4 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T4.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                geojson=geojson_lor_districts
            )

            # Extend forecast areas
            geojson_lor_forecast_areas_extended = extend_forecast_areas(
                logger=logger,
                statistics=statistics_lor_forecast_areas,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                area_property="area",
                geojson=geojson_lor_forecast_areas
            )

            # Extend district regions
            geojson_lor_district_regions_extended = extend_district_regions(
                logger=logger,
                statistics=statistics_lor_district_regions,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_district_regions,
                id_property="id",
                area_property="area"
            )

            # Extend planning areas
            geojson_lor_planning_areas_extended = extend_planning_areas(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_planning_areas,
                id_property="id",
                area_property="area"
            )

            # Write geojson files
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"bezirksgrenzen_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"bezirksgrenzen_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_districts_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_prognoseraeume_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_prognoseraeume_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_forecast_areas_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_bezirksregionen_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_bezirksregionen_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_district_regions_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_planungsraeume_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_planungsraeume_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_planning_areas_extended,
                clean=clean,
                quiet=quiet
            )

        # Iterate over statistics
        for statistic in exactly_2020_statistics:
            year = statistic.split(sep="_")[2].split(sep="h")[0]
            half_year = statistic.split(sep="_")[2].split(sep="h")[1]

            # Load statistics
            statistic_t1 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T1a.csv"))
            statistic_t2 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T2a.csv"))
            statistic_t3 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T3a.csv"))
            statistic_t4 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T4a.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                geojson=geojson_lor_districts
            )

            # Extend forecast areas
            geojson_lor_forecast_areas_extended = extend_forecast_areas(
                logger=logger,
                statistics=statistics_lor_forecast_areas,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                area_property="area",
                geojson=geojson_lor_forecast_areas
            )

            # Extend district regions
            geojson_lor_district_regions_extended = extend_district_regions(
                logger=logger,
                statistics=statistics_lor_district_regions,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_district_regions,
                id_property="id",
                area_property="area"
            )

            # Extend planning areas
            geojson_lor_planning_areas_extended = extend_planning_areas(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_planning_areas,
                id_property="id",
                area_property="area"
            )

            # Write geojson files
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"bezirksgrenzen_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"bezirksgrenzen_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_districts_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_prognoseraeume_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_prognoseraeume_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_forecast_areas_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_bezirksregionen_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_bezirksregionen_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_district_regions_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_planungsraeume_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_planungsraeume_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_planning_areas_extended,
                clean=clean,
                quiet=quiet
            )

        # Load geojson
        geojson_lor_districts = read_geojson_file(os.path.join(data_path, "bezirksgrenzen.geojson"))
        geojson_lor_forecast_areas = read_geojson_file(os.path.join(data_path, "lor_prognoseraeume_2021.geojson"))
        geojson_lor_district_regions = read_geojson_file(os.path.join(data_path, "lor_bezirksregionen_2021.geojson"))
        geojson_lor_planning_areas = read_geojson_file(os.path.join(data_path, "lor_planungsraeume_2021.geojson"))

        # Iterate over statistics
        for statistic in exactly_2020_statistics:
            year = statistic.split(sep="_")[2].split(sep="h")[0]
            half_year = statistic.split(sep="_")[2].split(sep="h")[1]

            # Load statistics
            statistic_t1 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T1b.csv"))
            statistic_t2 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T2b.csv"))
            statistic_t3 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T3b.csv"))
            statistic_t4 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T4b.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                geojson=geojson_lor_districts
            )

            # Extend forecast areas
            geojson_lor_forecast_areas_extended = extend_forecast_areas(
                logger=logger,
                statistics=statistics_lor_forecast_areas,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                area_property="area",
                geojson=geojson_lor_forecast_areas
            )

            # Extend district regions
            geojson_lor_district_regions_extended = extend_district_regions(
                logger=logger,
                statistics=statistics_lor_district_regions,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_district_regions,
                id_property="id",
                area_property="area"
            )

            # Extend planning areas
            geojson_lor_planning_areas_extended = extend_planning_areas(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_planning_areas,
                id_property="id",
                area_property="area"
            )

            # Write geojson files
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"bezirksgrenzen_{key_figure_group}_{year}_{half_year}_new_format.geojson"),
                statistic_name=f"bezirksgrenzen_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_districts_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_prognoseraeume_{key_figure_group}_{year}_{half_year}_new_format.geojson"),
                statistic_name=f"lor_prognoseraeume_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_forecast_areas_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_bezirksregionen_{key_figure_group}_{year}_{half_year}_new_format.geojson"),
                statistic_name=f"lor_bezirksregionen_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_district_regions_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_planungsraeume_{key_figure_group}_{year}_{half_year}_new_format.geojson"),
                statistic_name=f"lor_planungsraeume_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_planning_areas_extended,
                clean=clean,
                quiet=quiet
            )

        # Iterate over statistics
        for statistic in post_2020_statistics:
            year = statistic.split(sep="_")[2].split(sep="h")[0]
            half_year = statistic.split(sep="_")[2].split(sep="h")[1]

            # Load statistics
            statistic_t1 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T1.csv"))
            statistic_t2 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T2.csv"))
            statistic_t3 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T3.csv"))
            statistic_t4 = read_csv_file(os.path.join(statistics_path, f"{statistic}_T4.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                geojson=geojson_lor_districts
            )

            # Extend forecast areas
            geojson_lor_forecast_areas_extended = extend_forecast_areas(
                logger=logger,
                statistics=statistics_lor_forecast_areas,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                area_property="area",
                geojson=geojson_lor_forecast_areas
            )

            # Extend district regions
            geojson_lor_district_regions_extended = extend_district_regions(
                logger=logger,
                statistics=statistics_lor_district_regions,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_district_regions,
                id_property="id",
                area_property="area"
            )

            # Extend planning areas
            geojson_lor_planning_areas_extended = extend_planning_areas(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic=statistic,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_planning_areas,
                id_property="id",
                area_property="area"
            )

            # Write geojson files
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path, f"bezirksgrenzen_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"bezirksgrenzen_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_districts_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_prognoseraeume_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_prognoseraeume_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_forecast_areas_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_bezirksregionen_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_bezirksregionen_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_district_regions_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_planungsraeume_{key_figure_group}_{year}_{half_year}.geojson"),
                statistic_name=f"lor_planungsraeume_{key_figure_group}_{year}_{half_year}",
                geojson_content=geojson_lor_planning_areas_extended,
                clean=clean,
                quiet=quiet
            )

        # Write json file
        write_json_file(
            logger=logger,
            file_path=os.path.join(results_path, f"bezirksgrenzen_{key_figure_group}_statistics.json"),
            statistic_name=f"bezirksgrenzen_population",
            json_content=statistics_lor_districts,
            clean=clean,
            quiet=quiet
        )
        write_json_file(
            logger=logger,
            file_path=os.path.join(results_path, f"lor_prognoseraeume_{key_figure_group}_statistics.json"),
            statistic_name=f"lor_prognoseraeume_population",
            json_content=statistics_lor_forecast_areas,
            clean=clean,
            quiet=quiet
        )
        write_json_file(
            logger=logger,
            file_path=os.path.join(results_path, f"lor_bezirksregionen_{key_figure_group}_statistics.json"),
            statistic_name=f"lor_bezirksregionen_population",
            json_content=statistics_lor_district_regions,
            clean=clean,
            quiet=quiet
        )
        write_json_file(
            logger=logger,
            file_path=os.path.join(results_path, f"lor_planungsraeume_{key_figure_group}_statistics.json"),
            statistic_name=f"lor_planungsraeume_population",
            json_content=statistics_lor_planning_areas,
            clean=clean,
            quiet=quiet
        )
