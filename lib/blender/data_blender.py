import copy
import json
import os

import pandas as pd
from tracking_decorator import TrackingDecorator


def read_csv_file(file_path):
    with open(file_path, "r") as csv_file:
        return pd.read_csv(csv_file)


def read_geojson_file(file_path):
    with open(file_path, "r") as geojson_file:
        return json.load(geojson_file)


def write_geojson_file(file_path, geojson):
    with open(file_path, "w") as geojson_file:
        return json.dump(geojson, geojson_file)


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


def extend_districts(logger, results_path, result_file_name,
                     statistic, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
                     geojson, id_property, area_property, clean, quiet):
    geojson_extended_file = os.path.join(results_path, result_file_name)
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    if clean or not os.path.exists(geojson_extended_file):
        for feature in geojson_extended["features"]:
            id = feature["properties"][id_property]
            district_id = id

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

            if district_id == "001":
                area_sqkm = 39.34
            elif district_id == "002":
                area_sqkm = 20.36
            elif district_id == "003":
                area_sqkm = 103.10
            elif district_id == "004":
                area_sqkm = 59.76
            elif district_id == "005":
                area_sqkm = 91.74
            elif district_id == "006":
                area_sqkm = 102.40
            elif district_id == "007":
                area_sqkm = 52.93
            elif district_id == "008":
                area_sqkm = 44.89
            elif district_id == "009":
                area_sqkm = 167.41
            elif district_id == "010":
                area_sqkm = 61.77
            elif district_id == "011":
                area_sqkm = 52.02
            elif district_id == "012":
                area_sqkm = 89.19
            else:
                area_sqkm = None

            # Blend data
            blend_data(
                feature=feature, area_sqkm=area_sqkm,
                statistic_t1=statistic_t1_filtered,
                statistic_t2=statistic_t2_filtered,
                statistic_t3=statistic_t3_filtered,
                statistic_t4=statistic_t4_filtered
            )

        write_geojson_file(geojson_extended_file, geojson_extended)

        if not quiet:
            logger.log_line(f"✓ Blend data from {statistic} into {result_file_name}")


def extend_forecast_areas(logger, results_path, result_file_name,
                          statistic, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
                          geojson, id_property, area_property, clean, quiet):
    geojson_extended_file = os.path.join(results_path, result_file_name)
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    if clean or not os.path.exists(geojson_extended_file):
        for feature in geojson_extended["features"]:
            id = feature["properties"][id_property]
            district_id, forecast_area_id, district_region_id, planning_area_id = build_ids(id)
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

        write_geojson_file(geojson_extended_file, geojson_extended)

        if not quiet:
            logger.log_line(f"✓ Blend data from {statistic} into {result_file_name}")


def extend_district_regions(logger, results_path, result_file_name,
                            statistic, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
                            geojson, id_property, area_property, clean, quiet):
    geojson_extended_file = os.path.join(results_path, result_file_name)
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    if clean or not os.path.exists(geojson_extended_file):
        for feature in geojson_extended["features"]:
            id = feature["properties"][id_property]
            district_id, forecast_area_id, district_region_id, planning_area_id = build_ids(id)
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
                    f"✗️ No data in {statistic} for district={district_id}, forecast area={forecast_area_id}, district_region_id={district_region_id}")
                continue

            # Blend data
            blend_data(
                feature=feature, area_sqkm=area_sqkm,
                statistic_t1=statistic_t1_filtered,
                statistic_t2=statistic_t2_filtered,
                statistic_t3=statistic_t3_filtered,
                statistic_t4=statistic_t4_filtered
            )

        write_geojson_file(geojson_extended_file, geojson_extended)

        if not quiet:
            logger.log_line(f"✓ Blend data from {statistic} into {result_file_name}")


def extend_planning_areas(logger, results_path, result_file_name,
                          statistic, statistic_t1, statistic_t2, statistic_t3, statistic_t4,
                          geojson, id_property, area_property, clean, quiet):
    geojson_extended_file = os.path.join(results_path, result_file_name)
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    if clean or not os.path.exists(geojson_extended_file):
        for feature in geojson_extended["features"]:
            id = feature["properties"][id_property]
            district_id, forecast_area_id, district_region_id, planning_area_id = build_ids(id)
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
                    f"✗️ No data in {statistic} for district={district_id}, forecast area={forecast_area_id}, district_region_id={district_region_id}, planning_area_id={planning_area_id}")
                continue

            # Blend data
            blend_data(
                feature=feature, area_sqkm=area_sqkm,
                statistic_t1=statistic_t1_filtered,
                statistic_t2=statistic_t2_filtered,
                statistic_t3=statistic_t3_filtered,
                statistic_t4=statistic_t4_filtered
            )

        write_geojson_file(geojson_extended_file, geojson_extended)

        if not quiet:
            logger.log_line(f"✓ Blend data from {statistic} into {result_file_name}")


def build_ids(combined_id):
    return combined_id[0:2], combined_id[2:4], combined_id[4:6], combined_id[6:8]


def blend_data(feature, area_sqkm, statistic_t1, statistic_t2, statistic_t3, statistic_t4):
    # Lookup data
    inhabitants = statistic_t1["insgesamt_anzahl"].sum()
    inhabitants_with_migration_background = statistic_t1["darunter_mit_migrationshintergrund_anzahl"].sum()
    inhabitants_germans = statistic_t1["deutsche_zusammen_anzahl"].sum()
    inhabitants_germans_without_migration_background = statistic_t1["deutsche_ohne_migrationshintergrund_anzahl"].sum()
    inhabitants_germans_with_migration_background = statistic_t1["deutsche_mit_migrationshintergrund_anzahl"].sum()
    inhabitants_foreigners = statistic_t1["auslaender_anzahl"].sum()

    inhabitants_age_below_6 = statistic_t2["alter_unter_6"].sum()
    inhabitants_age_6_15 = statistic_t2["alter_6-15"].sum()
    inhabitants_age_15_18 = statistic_t2["alter_15-18"].sum()
    inhabitants_age_18_27 = statistic_t2["alter_18-27"].sum()
    inhabitants_age_27_45 = statistic_t2["alter_27-45"].sum()
    inhabitants_age_45_55 = statistic_t2["alter_45-55"].sum()
    inhabitants_age_55_65 = statistic_t2["alter_55-65"].sum()
    inhabitants_age_above_65 = statistic_t2["alter_65_und_mehr"].sum()
    inhabitants_female = statistic_t2["weiblich"].sum()

    inhabitants_with_migration_background_age_below_6 = statistic_t3["alter_unter_6"].sum()
    inhabitants_with_migration_background_age_6_15 = statistic_t3["alter_6-15"].sum()
    inhabitants_with_migration_background_age_15_18 = statistic_t3["alter_15-18"].sum()
    inhabitants_with_migration_background_age_18_27 = statistic_t3["alter_18-27"].sum()
    inhabitants_with_migration_background_age_27_45 = statistic_t3["alter_27-45"].sum()
    inhabitants_with_migration_background_age_45_55 = statistic_t3["alter_45-55"].sum()
    inhabitants_with_migration_background_age_55_65 = statistic_t3["alter_55-65"].sum()
    inhabitants_with_migration_background_age_above_65 = statistic_t3["alter_65_und_mehr"].sum()
    inhabitants_with_migration_background_female = statistic_t3["weiblich"].sum()

    inhabitants_from_european_union = statistic_t4["europaeische_union"].sum()
    inhabitants_from_france = statistic_t4["frankreich"].sum()
    inhabitants_from_greece = statistic_t4["griechenland"].sum()
    inhabitants_from_italy = statistic_t4["italien"].sum()
    inhabitants_from_austria = statistic_t4["oesterreich"].sum()
    inhabitants_from_spain = statistic_t4["spanien"].sum()
    inhabitants_from_poland = statistic_t4["polen"].sum()
    inhabitants_from_bulgaria = statistic_t4["bulgarien"].sum()
    inhabitants_from_rumania = statistic_t4["rumaenien"].sum()
    inhabitants_from_croatia = statistic_t4["kroatien"].sum()
    inhabitants_from_united_kingdom = statistic_t4["vereinigtes_koenigreich"].sum()
    inhabitants_from_former_yugoslavia = statistic_t4["ehemaliges_jugoslawien"].sum()
    inhabitants_from_bosnia_herzegovina = statistic_t4["bosnien_und_herzegowina"].sum()
    inhabitants_from_serbia = statistic_t4["serbien"].sum()
    inhabitants_from_former_soviet_union = statistic_t4["ehemalige_sowjetunion"].sum()
    inhabitants_from_russia = statistic_t4["russische_foederation"].sum()
    inhabitants_from_ukraine = statistic_t4["ukraine"].sum()
    inhabitants_from_kazakhstan = statistic_t4["kasachstan"].sum()
    inhabitants_from_islamic_countries = statistic_t4["islamische_laender"].sum()
    inhabitants_from_turkey = statistic_t4["tuerkei"].sum()
    inhabitants_from_iran = statistic_t4["iran"].sum()
    inhabitants_from_arabic_countries = statistic_t4["arabische_laender"].sum()
    inhabitants_from_lebanon = statistic_t4["libanon"].sum()
    inhabitants_from_syria = statistic_t4["syrien"].sum()
    inhabitants_from_vietnam = statistic_t4["vietnam"].sum()
    inhabitants_from_united_states = statistic_t4["vereinigte_staaten"].sum()
    inhabitants_from_undefined = statistic_t4["nicht_eindeutig_zuordenbar_ohne_angabe"].sum()

    # Add new properties
    add_prop(feature, "inhabitants", inhabitants, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background", inhabitants_with_migration_background, inhabitants,
             area_sqkm)
    add_prop(feature, "inhabitants_germans", inhabitants_germans, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_germans_without_migration_background",
             inhabitants_germans_without_migration_background, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_germans_with_migration_background",
             inhabitants_germans_with_migration_background, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_foreigners", inhabitants_foreigners, inhabitants, area_sqkm)

    add_prop(feature, "inhabitants_age_below_6", inhabitants_age_below_6, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_age_6_15", inhabitants_age_6_15, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_age_15_18", inhabitants_age_15_18, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_age_18_27", inhabitants_age_18_27, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_age_27_45", inhabitants_age_27_45, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_age_45_55", inhabitants_age_45_55, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_age_55_65", inhabitants_age_55_65, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_age_above_65", inhabitants_age_above_65, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_female", inhabitants_female, inhabitants, area_sqkm)

    add_prop(feature, "inhabitants_with_migration_background_age_below_6",
             inhabitants_with_migration_background_age_below_6, inhabitants_with_migration_background, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background_age_6_15",
             inhabitants_with_migration_background_age_6_15, inhabitants_with_migration_background, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background_age_15_18",
             inhabitants_with_migration_background_age_15_18, inhabitants_with_migration_background, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background_age_18_27",
             inhabitants_with_migration_background_age_18_27, inhabitants_with_migration_background, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background_age_27_45",
             inhabitants_with_migration_background_age_27_45, inhabitants_with_migration_background, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background_age_45_55",
             inhabitants_with_migration_background_age_45_55, inhabitants_with_migration_background, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background_age_55_65",
             inhabitants_with_migration_background_age_55_65, inhabitants_with_migration_background, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background_age_above_65",
             inhabitants_with_migration_background_age_above_65, inhabitants_with_migration_background, area_sqkm)
    add_prop(feature, "inhabitants_with_migration_background_female", inhabitants_with_migration_background_female,
             inhabitants_with_migration_background, area_sqkm)

    add_prop(feature, "inhabitants_from_european_union", inhabitants_from_european_union, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_france", inhabitants_from_france, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_greece", inhabitants_from_greece, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_italy", inhabitants_from_italy, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_austria", inhabitants_from_austria, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_spain", inhabitants_from_spain, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_poland", inhabitants_from_poland, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_bulgaria", inhabitants_from_bulgaria, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_rumania", inhabitants_from_rumania, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_croatia", inhabitants_from_croatia, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_united_kingdom", inhabitants_from_united_kingdom, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_former_yugoslavia", inhabitants_from_former_yugoslavia, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_bosnia_herzegovina", inhabitants_from_bosnia_herzegovina, inhabitants,
             area_sqkm)
    add_prop(feature, "inhabitants_from_serbia", inhabitants_from_serbia, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_former_soviet_union", inhabitants_from_former_soviet_union, inhabitants,
             area_sqkm)
    add_prop(feature, "inhabitants_from_russia", inhabitants_from_russia, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_ukraine", inhabitants_from_ukraine, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_kazakhstan", inhabitants_from_kazakhstan, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_islamic_countries", inhabitants_from_islamic_countries, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_turkey", inhabitants_from_turkey, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_iran", inhabitants_from_iran, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_arabic_countries", inhabitants_from_arabic_countries, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_lebanon", inhabitants_from_lebanon, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_syria", inhabitants_from_syria, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_vietnam", inhabitants_from_vietnam, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_united_states", inhabitants_from_united_states, inhabitants, area_sqkm)
    add_prop(feature, "inhabitants_from_undefined", inhabitants_from_undefined, inhabitants, area_sqkm)


def add_prop(feature, property_name, property_value, total_count, total_area_sqkm):
    feature["properties"][f"{property_name}"] = int(property_value)
    feature["properties"][f"{property_name}_percentage"] = round(property_value / total_count * 100, 2)

    if total_area_sqkm is not None:
        feature["properties"][f"{property_name}_per_sqkm"] = round(property_value / total_area_sqkm)


#
# Main
#

class DataBlender:

    @TrackingDecorator.track_time
    def run(self, logger, data_path, statistics_path, results_path, clean=False, quiet=False):

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
            extend_districts(logger=logger,
                             results_path=results_path,
                             result_file_name=f"bezirksgrenzen_population_{year}_{half_year}.geojson",
                             statistic=statistic,
                             statistic_t1=statistic_t1,
                             statistic_t2=statistic_t2,
                             statistic_t3=statistic_t3,
                             statistic_t4=statistic_t4,
                             id_property="Gemeinde_schluessel",
                             area_property=None,
                             geojson=geojson_lor_districts,
                             clean=clean,
                             quiet=quiet)

            # Extend forecast areas
            extend_forecast_areas(logger=logger,
                                  results_path=results_path,
                                  result_file_name=f"lor_prognoseraeume_population_{year}_{half_year}.geojson",
                                  statistic=statistic,
                                  statistic_t1=statistic_t1,
                                  statistic_t2=statistic_t2,
                                  statistic_t3=statistic_t3,
                                  statistic_t4=statistic_t4,
                                  id_property="broker Dow",
                                  area_property="FLAECHENGR",
                                  geojson=geojson_lor_forecast_areas,
                                  clean=clean,
                                  quiet=quiet)

            # Extend district regions
            extend_district_regions(logger=logger,
                                    results_path=results_path,
                                    result_file_name=f"lor_bezirksregionen_population_{year}_{half_year}.geojson",
                                    statistic=statistic,
                                    statistic_t1=statistic_t1,
                                    statistic_t2=statistic_t2,
                                    statistic_t3=statistic_t3,
                                    statistic_t4=statistic_t4,
                                    geojson=geojson_lor_district_regions,
                                    id_property="broker Dow",
                                    area_property="FLAECHENGR",
                                    clean=clean,
                                    quiet=quiet)

            # Extend planning areas
            extend_planning_areas(logger=logger,
                                  results_path=results_path,
                                  result_file_name=f"lor_planungsraeume_population_{year}_{half_year}.geojson",
                                  statistic=statistic,
                                  statistic_t1=statistic_t1,
                                  statistic_t2=statistic_t2,
                                  statistic_t3=statistic_t3,
                                  statistic_t4=statistic_t4,
                                  geojson=geojson_lor_planning_areas,
                                  id_property="broker Dow",
                                  area_property="FLAECHENGR",
                                  clean=clean,
                                  quiet=quiet)

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
            extend_districts(logger=logger,
                             results_path=results_path,
                             result_file_name=f"bezirksgrenzen_population_{year}_{half_year}.geojson",
                             statistic=statistic,
                             statistic_t1=statistic_t1,
                             statistic_t2=statistic_t2,
                             statistic_t3=statistic_t3,
                             statistic_t4=statistic_t4,
                             id_property="Gemeinde_schluessel",
                             area_property=None,
                             geojson=geojson_lor_districts,
                             clean=clean,
                             quiet=quiet)

            # Extend forecast areas
            extend_forecast_areas(logger=logger,
                                  results_path=results_path,
                                  result_file_name=f"lor_prognoseraeume_population_{year}_{half_year}.geojson",
                                  statistic=statistic,
                                  statistic_t1=statistic_t1,
                                  statistic_t2=statistic_t2,
                                  statistic_t3=statistic_t3,
                                  statistic_t4=statistic_t4,
                                  id_property="broker Dow",
                                  area_property="FLAECHENGR",
                                  geojson=geojson_lor_forecast_areas,
                                  clean=clean,
                                  quiet=quiet)

            # Extend district regions
            extend_district_regions(logger=logger,
                                    results_path=results_path,
                                    result_file_name=f"lor_bezirksregionen_population_{year}_{half_year}.geojson",
                                    statistic=statistic,
                                    statistic_t1=statistic_t1,
                                    statistic_t2=statistic_t2,
                                    statistic_t3=statistic_t3,
                                    statistic_t4=statistic_t4,
                                    geojson=geojson_lor_district_regions,
                                    id_property="broker Dow",
                                    area_property="FLAECHENGR",
                                    clean=clean,
                                    quiet=quiet)

            # Extend planning areas
            extend_planning_areas(logger=logger,
                                  results_path=results_path,
                                  result_file_name=f"lor_planungsraeume_population_{year}_{half_year}.geojson",
                                  statistic=statistic,
                                  statistic_t1=statistic_t1,
                                  statistic_t2=statistic_t2,
                                  statistic_t3=statistic_t3,
                                  statistic_t4=statistic_t4,
                                  geojson=geojson_lor_planning_areas,
                                  id_property="broker Dow",
                                  area_property="FLAECHENGR",
                                  clean=clean,
                                  quiet=quiet)

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
            extend_districts(logger=logger,
                             results_path=results_path,
                             result_file_name=f"bezirksgrenzen_population_{year}_{half_year}_new_format.geojson",
                             statistic=statistic,
                             statistic_t1=statistic_t1,
                             statistic_t2=statistic_t2,
                             statistic_t3=statistic_t3,
                             statistic_t4=statistic_t4,
                             id_property="Gemeinde_schluessel",
                             area_property=None,
                             geojson=geojson_lor_districts,
                             clean=clean,
                             quiet=quiet)

            # Extend forecast areas
            extend_forecast_areas(logger=logger,
                                  results_path=results_path,
                                  result_file_name=f"lor_prognoseraeume_population_{year}_{half_year}_new_format.geojson",
                                  statistic=statistic,
                                  statistic_t1=statistic_t1,
                                  statistic_t2=statistic_t2,
                                  statistic_t3=statistic_t3,
                                  statistic_t4=statistic_t4,
                                  id_property="PGR_ID",
                                  area_property="GROESSE_M2",
                                  geojson=geojson_lor_forecast_areas,
                                  clean=clean,
                                  quiet=quiet)

            # Extend district regions
            extend_district_regions(logger=logger,
                                    results_path=results_path,
                                    result_file_name=f"lor_bezirksregionen_population_{year}_{half_year}_new_format.geojson",
                                    statistic=statistic,
                                    statistic_t1=statistic_t1,
                                    statistic_t2=statistic_t2,
                                    statistic_t3=statistic_t3,
                                    statistic_t4=statistic_t4,
                                    geojson=geojson_lor_district_regions,
                                    id_property="BZR_ID",
                                    area_property="GROESSE_m2",
                                    clean=clean,
                                    quiet=quiet)

            # Extend planning areas
            extend_planning_areas(logger=logger,
                                  results_path=results_path,
                                  result_file_name=f"lor_planungsraeume_population_{year}_{half_year}_new_format.geojson",
                                  statistic=statistic,
                                  statistic_t1=statistic_t1,
                                  statistic_t2=statistic_t2,
                                  statistic_t3=statistic_t3,
                                  statistic_t4=statistic_t4,
                                  geojson=geojson_lor_planning_areas,
                                  id_property="PLR_ID",
                                  area_property="GROESSE_M2",
                                  clean=clean,
                                  quiet=quiet)

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
            extend_districts(logger=logger,
                             results_path=results_path,
                             result_file_name=f"bezirksgrenzen_population_{year}_{half_year}.geojson",
                             statistic=statistic,
                             statistic_t1=statistic_t1,
                             statistic_t2=statistic_t2,
                             statistic_t3=statistic_t3,
                             statistic_t4=statistic_t4,
                             id_property="Gemeinde_schluessel",
                             area_property=None,
                             geojson=geojson_lor_districts,
                             clean=clean,
                             quiet=quiet)

            # Extend forecast areas
            extend_forecast_areas(logger=logger,
                                  results_path=results_path,
                                  result_file_name=f"lor_prognoseraeume_population_{year}_{half_year}.geojson",
                                  statistic=statistic,
                                  statistic_t1=statistic_t1,
                                  statistic_t2=statistic_t2,
                                  statistic_t3=statistic_t3,
                                  statistic_t4=statistic_t4,
                                  id_property="PGR_ID",
                                  area_property="GROESSE_M2",
                                  geojson=geojson_lor_forecast_areas,
                                  clean=clean,
                                  quiet=quiet)

            # Extend district regions
            extend_district_regions(logger=logger,
                                    results_path=results_path,
                                    result_file_name=f"lor_bezirksregionen_population_{year}_{half_year}.geojson",
                                    statistic=statistic,
                                    statistic_t1=statistic_t1,
                                    statistic_t2=statistic_t2,
                                    statistic_t3=statistic_t3,
                                    statistic_t4=statistic_t4,
                                    geojson=geojson_lor_district_regions,
                                    id_property="BZR_ID",
                                    area_property="GROESSE_m2",
                                    clean=clean,
                                    quiet=quiet)

            # Extend planning areas
            extend_planning_areas(logger=logger,
                                  results_path=results_path,
                                  result_file_name=f"lor_planungsraeume_population_{year}_{half_year}.geojson",
                                  statistic=statistic,
                                  statistic_t1=statistic_t1,
                                  statistic_t2=statistic_t2,
                                  statistic_t3=statistic_t3,
                                  statistic_t4=statistic_t4,
                                  geojson=geojson_lor_planning_areas,
                                  id_property="PLR_ID",
                                  area_property="GROESSE_M2",
                                  clean=clean,
                                  quiet=quiet)
