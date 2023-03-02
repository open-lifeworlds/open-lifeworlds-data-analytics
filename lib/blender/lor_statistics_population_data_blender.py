import copy
import json
import os
import statistics as stats

import pandas as pd
from tracking_decorator import TrackingDecorator

statistic_t1_fields = {
    "inhabitants": "insgesamt_anzahl",
    "inhabitants_with_migration_background": "darunter_mit_migrationshintergrund_anzahl",
    "inhabitants_germans": "deutsche_zusammen_anzahl",
    "inhabitants_germans_without_migration_background": "deutsche_ohne_migrationshintergrund_anzahl",
    "inhabitants_germans_with_migration_background": "deutsche_mit_migrationshintergrund_anzahl",
    "inhabitants_foreigners": "auslaender_anzahl"
}

statistic_t2_fields = {
    "inhabitants_age_below_6": "alter_unter_6",
    "inhabitants_age_6_15": "alter_6-15",
    "inhabitants_age_15_18": "alter_15-18",
    "inhabitants_age_18_27": "alter_18-27",
    "inhabitants_age_27_45": "alter_27-45",
    "inhabitants_age_45_55": "alter_45-55",
    "inhabitants_age_55_65": "alter_55-65",
    "inhabitants_age_above_65": "alter_65_und_mehr",
    "inhabitants_female": "weiblich"
}

statistic_t3_fields = {
    "inhabitants_with_migration_background_age_below_6": "alter_unter_6",
    "inhabitants_with_migration_background_age_6_15": "alter_6-15",
    "inhabitants_with_migration_background_age_15_18": "alter_15-18",
    "inhabitants_with_migration_background_age_18_27": "alter_18-27",
    "inhabitants_with_migration_background_age_27_45": "alter_27-45",
    "inhabitants_with_migration_background_age_45_55": "alter_45-55",
    "inhabitants_with_migration_background_age_55_65": "alter_55-65",
    "inhabitants_with_migration_background_age_above_65": "alter_65_und_mehr",
    "inhabitants_with_migration_background_female": "weiblich",
}

statistic_t4_fields = {
    "inhabitants_from_european_union": "europaeische_union",
    "inhabitants_from_france": "frankreich",
    "inhabitants_from_greece": "griechenland",
    "inhabitants_from_italy": "spanien",
    "inhabitants_from_austria": "oesterreich",
    "inhabitants_from_spain": "spanien",
    "inhabitants_from_poland": "polen",
    "inhabitants_from_bulgaria": "bulgarien",
    "inhabitants_from_rumania": "rumaenien",
    "inhabitants_from_croatia": "kroatien",
    "inhabitants_from_united_kingdom": "vereinigtes_koenigreich",
    "inhabitants_from_former_yugoslavia": "ehemaliges_jugoslawien",
    "inhabitants_from_bosnia_herzegovina": "bosnien_und_herzegowina",
    "inhabitants_from_serbia": "serbien",
    "inhabitants_from_former_soviet_union": "ehemalige_sowjetunion",
    "inhabitants_from_russia": "russische_foederation",
    "inhabitants_from_ukraine": "ukraine",
    "inhabitants_from_kazakhstan": "kasachstan",
    "inhabitants_from_islamic_countries": "islamische_laender",
    "inhabitants_from_turkey": "tuerkei",
    "inhabitants_from_iran": "iran",
    "inhabitants_from_arabic_countries": "arabische_laender",
    "inhabitants_from_lebanon": "libanon",
    "inhabitants_from_syria": "syrien",
    "inhabitants_from_vietnam": "vietnam",
    "inhabitants_from_united_states": "vereinigte_staaten",
    "inhabitants_from_undefined": "nicht_eindeutig_zuordenbar_ohne_angabe",
}


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
                     statistic_name, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
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
            logger.log_line(f"✗️ No data in {statistic_name} for district={district_id}")
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

    # Calculate average and median values
    for year, half_years in statistics.items():
        for half_year, feature_ids in half_years.items():
            values = {}

            for feature_id, properties in feature_ids.items():
                for property_name, property_value in properties.items():
                    if property_name in list(statistic_t1_fields.keys()) + list(statistic_t2_fields.keys()) + list(
                            statistic_t3_fields.keys()) + list(statistic_t4_fields.keys()):
                        if property_name not in values:
                            values[property_name] = []
                        values[property_name].append(property_value)

            statistics[year][half_year]["average"] = {key: stats.mean(lst) for key, lst in values.items()}
            statistics[year][half_year]["median"] = {key: stats.median(lst) for key, lst in values.items()}

    return geojson_extended, statistics


def extend_forecast_areas(logger, statistics, year, half_year,
                          statistic_name, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
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
                f"✗️ No data in {statistic_name} for district={district_id}, forecast area={forecast_area_id}")
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

    # Calculate average and median values
    for year, half_years in statistics.items():
        for half_year, feature_ids in half_years.items():
            values = {}

            for feature_id, properties in feature_ids.items():
                for property_name, property_value in properties.items():
                    if property_name in list(statistic_t1_fields.keys()) + list(statistic_t2_fields.keys()) + list(
                            statistic_t3_fields.keys()) + list(statistic_t4_fields.keys()):
                        if property_name not in values:
                            values[property_name] = []
                        values[property_name].append(property_value)

            statistics[year][half_year]["average"] = {key: stats.mean(lst) for key, lst in values.items()}
            statistics[year][half_year]["median"] = {key: stats.median(lst) for key, lst in values.items()}

    return geojson_extended, statistics


def extend_district_regions(logger, statistics, year, half_year,
                            statistic_name, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
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
                f"✗️ No data in {statistic_name} for district={district_id}, forecast area={forecast_area_id}, "
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

    # Calculate average and median values
    for year, half_years in statistics.items():
        for half_year, feature_ids in half_years.items():
            values = {}

            for feature_id, properties in feature_ids.items():
                for property_name, property_value in properties.items():
                    if property_name in list(statistic_t1_fields.keys()) + list(statistic_t2_fields.keys()) + list(
                            statistic_t3_fields.keys()) + list(statistic_t4_fields.keys()):
                        if property_name not in values:
                            values[property_name] = []
                        values[property_name].append(property_value)

            statistics[year][half_year]["average"] = {key: stats.mean(lst) for key, lst in values.items()}
            statistics[year][half_year]["median"] = {key: stats.median(lst) for key, lst in values.items()}

    return geojson_extended, statistics


def extend_planning_areas(logger, statistics, year, half_year,
                          statistic_name, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
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
                f"✗️ No data in {statistic_name} for district={district_id}, forecast area={forecast_area_id}, "
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

    # Calculate average and median values
    for year, half_years in statistics.items():
        for half_year, feature_ids in half_years.items():
            values = {}

            for feature_id, properties in feature_ids.items():
                for property_name, property_value in properties.items():
                    if property_name in list(statistic_t1_fields.keys()) + list(statistic_t2_fields.keys()) + list(
                            statistic_t3_fields.keys()) + list(statistic_t4_fields.keys()):
                        if property_name not in values:
                            values[property_name] = []
                        values[property_name].append(property_value)

            statistics[year][half_year]["average"] = {key: stats.mean(lst) for key, lst in values.items()}
            statistics[year][half_year]["median"] = {key: stats.median(lst) for key, lst in values.items()}

    return geojson_extended, statistics


def build_ids(combined_id):
    return combined_id[0:2], combined_id[2:4], combined_id[4:6], combined_id[6:8]


def blend_data(feature, area_sqkm, statistic_t1, statistic_t2, statistic_t3, statistic_t4):
    # Lookup data
    inhabitants = statistic_t1["insgesamt_anzahl"].sum()

    # Add new properties
    for target_field_name, source_field_name in statistic_t1_fields.items():
        add_prop_with_modifiers(feature, target_field_name, statistic_t1, source_field_name, inhabitants, area_sqkm)
    for target_field_name, source_field_name in statistic_t2_fields.items():
        add_prop_with_modifiers(feature, target_field_name, statistic_t2, source_field_name, inhabitants, area_sqkm)
    for target_field_name, source_field_name in statistic_t3_fields.items():
        add_prop_with_modifiers(feature, target_field_name, statistic_t3, source_field_name, inhabitants, area_sqkm)
    for target_field_name, source_field_name in statistic_t4_fields.items():
        add_prop_with_modifiers(feature, target_field_name, statistic_t4, source_field_name, inhabitants, area_sqkm)

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
        for statistic_name in pre_2020_statistics:
            year = statistic_name.split(sep="_")[2].split(sep="h")[0]
            half_year = statistic_name.split(sep="_")[2].split(sep="h")[1]

            # Load statistics
            statistic_t1 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T1.csv"))
            statistic_t2 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T2.csv"))
            statistic_t3 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T3.csv"))
            statistic_t4 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T4.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
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
                statistic_name=statistic_name,
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
                statistic_name=statistic_name,
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
                statistic_name=statistic_name,
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
        for statistic_name in exactly_2020_statistics:
            year = statistic_name.split(sep="_")[2].split(sep="h")[0]
            half_year = statistic_name.split(sep="_")[2].split(sep="h")[1]

            # Load statistics
            statistic_t1 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T1a.csv"))
            statistic_t2 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T2a.csv"))
            statistic_t3 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T3a.csv"))
            statistic_t4 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T4a.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
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
                statistic_name=statistic_name,
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
                statistic_name=statistic_name,
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
                statistic_name=statistic_name,
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
        for statistic_name in exactly_2020_statistics:
            year = statistic_name.split(sep="_")[2].split(sep="h")[0]
            half_year = statistic_name.split(sep="_")[2].split(sep="h")[1]

            # Load statistics
            statistic_t1 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T1b.csv"))
            statistic_t2 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T2b.csv"))
            statistic_t3 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T3b.csv"))
            statistic_t4 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T4b.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
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
                statistic_name=statistic_name,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                area_property="area",
                geojson=geojson_lor_forecast_areas
            )

            # Extend district regions
            geojson_lor_district_regions_extended, statistics_lor_district_regions = extend_district_regions(
                logger=logger,
                statistics=statistics_lor_district_regions,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_district_regions,
                id_property="id",
                area_property="area"
            )

            # Extend planning areas
            geojson_lor_planning_areas_extended, statistics_lor_planning_areas = extend_planning_areas(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
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
        for statistic_name in post_2020_statistics:
            year = statistic_name.split(sep="_")[2].split(sep="h")[0]
            half_year = statistic_name.split(sep="_")[2].split(sep="h")[1]

            # Load statistics
            statistic_t1 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T1.csv"))
            statistic_t2 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T2.csv"))
            statistic_t3 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T3.csv"))
            statistic_t4 = read_csv_file(os.path.join(statistics_path, f"{statistic_name}_T4.csv"))

            # Extend districts
            geojson_lor_districts_extended, statistics_lor_districts = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                geojson=geojson_lor_districts
            )

            # Extend forecast areas
            geojson_lor_forecast_areas_extended, statistics_lor_forecast_areas = extend_forecast_areas(
                logger=logger,
                statistics=statistics_lor_forecast_areas,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                id_property="id",
                area_property="area",
                geojson=geojson_lor_forecast_areas
            )

            # Extend district regions
            geojson_lor_district_regions_extended, statistics_lor_district_regions = extend_district_regions(
                logger=logger,
                statistics=statistics_lor_district_regions,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
                statistic_t1=statistic_t1,
                statistic_t2=statistic_t2,
                statistic_t3=statistic_t3,
                statistic_t4=statistic_t4,
                geojson=geojson_lor_district_regions,
                id_property="id",
                area_property="area"
            )

            # Extend planning areas
            geojson_lor_planning_areas_extended, statistics_lor_planning_areas = extend_planning_areas(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic_name=statistic_name,
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
