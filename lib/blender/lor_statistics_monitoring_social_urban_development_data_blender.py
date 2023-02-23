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
    [
        "1-sdi_mss2013",
        "2-1-indexind_anteile_plr_mss2013",
        "2-2-indexind_anteile_bzr_mss2013",
        "2-3-indexind_anteile_bezirke_mss2013",
        "3-indexind_z_wertemss2013",
        "4-1-kontextind_anteile_plr_mss2013",
        "4-2-kontextind_anteile_bzr_mss2013",
        "4-3-kontextind_anteile_bezirke_mss2013"
    ],
    [
        "1-sdi_mss2015",
        "2-1-indexind_anteile_plr_mss2015",
        "2-2-indexind_anteile_bzr_mss2015",
        "2-3-indexind_anteile_bezirke_mss2015",
        "3-indexind_z_wertemss2015",
        "4-1-kontextind_anteile_plr_mss2015",
        "4-2-kontextind_anteile_bzr_mss2015",
        "4-3-kontextind_anteile_bezirke_mss2015"
    ],
    [
        "1-sdi_mss2017",
        "2-1-indexind_anteile_plr_mss2017",
        None,
        None,
        "3-indexind_z_wertemss2017",
        "4-1-kontextind_anteile_plr_mss2017",
        "4-2-kontextind_anteile_bzr_mss2017",
        "4-3-kontextind_anteile_bezirke_mss2017"
    ],
    [
        "1-sdi_mss2019",
        "2-1-indexind_anteile_plr_mss2019",
        "2-2-indexind_anteile_bzr_mss2019",
        "2-3-indexind_anteile_bezirke_mss2019",
        "3-indexind_z_wertemss2019",
        "4.1.kontextind_anteile_plr_mss2019",
        "4.2.kontextind_anteile_bzr_mss2019",
        "4.3.kontextind_anteile_bezirke_mss2019"
    ]
]
post_2020_statistics = [
    [
        "tabelle_1_gesamtindex_soziale_ungleichheit_sdi_mss_2021",
        "tabelle_2-1_index-indikatoren_anteilswerte_auf_planungsraum-ebene_mss_2021",
        "tabelle_2-2_index-indikatoren_anteilswerte_auf_bezirksregionen-ebene_mss_2021",
        "tabelle_2-3_index-indikatoren_auf_ebene_der_bezirke_mss_2021",
        "tabelle_3_index-indikatoren_z-werte_mss_2021",
        "tabelle_4-1_kontext-indikatoren_anteile_plr_mss_2021",
        "tabelle_4-1-1_kontext-indikatoren_anteile_plr_mss_2021",
        "tabelle_4-2_kontext-indikatoren_anteile_bzr_mss_2021",
        "tabelle_4-2-1_kontext-indikatoren_anteile_bzr_mss_2021",
        "tabelle_4-3_kontext-indikatoren_anteile_bezirke_mss_2021",
        "tabelle_4-3-1_kontext-indikatoren_anteile_bezirke_mss_2021",
    ]
]


def extend_districts(logger, statistics, year, half_year,
                     statistic, statistic_2_3, statistic_4_3,
                     geojson, id_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        district_id = feature_id

        # Filter statistics
        statistic_2_3_filtered = statistic_2_3.loc[
            (statistic_2_3["nummer"] == int(feature_id))] if statistic_2_3 is not None else None
        statistic_4_3_filtered = statistic_4_3.loc[
            (statistic_4_3["nummer"] == int(feature_id))] if statistic_4_3 is not None else None

        # Check for missing data
        if statistic_2_3_filtered is None or statistic_2_3_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_2_3_filtered) for id={feature_id}")
            continue
        if statistic_4_3_filtered is None or statistic_4_3_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_3_filtered) for id={feature_id}")
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
        feature = blend_district_data(
            feature=feature, area_sqkm=area_sqkm,
            statistic_2_3_filtered=statistic_2_3_filtered,
            statistic_4_3_filtered=statistic_4_3_filtered
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
                            statistic, statistic_2_2, statistic_4_2,
                            geojson, id_property, area_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        area_sqm = feature["properties"][area_property]
        area_sqkm = area_sqm / 1_000_000

        # Filter statistics
        statistic_2_2_filtered = statistic_2_2.loc[
            (statistic_2_2["nummer"] == int(feature_id))] if statistic_2_2 is not None else None
        statistic_4_2_filtered = statistic_4_2.loc[
            (statistic_4_2["nummer"] == int(feature_id))] if statistic_4_2 is not None else None

        # Check for missing data
        if statistic_2_2_filtered is None or statistic_2_2_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_2_2_filtered) for id={feature_id}")
            continue
        if statistic_4_2_filtered is None or statistic_4_2_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_2_filtered) for id={feature_id}")
            continue

        # Blend data
        feature = blend_district_region_data(
            feature=feature, area_sqkm=area_sqkm,
            statistic_2_2_filtered=statistic_2_2_filtered,
            statistic_4_2_filtered=statistic_4_2_filtered
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
                          statistic, statistic_1, statistic_2_1, statistic_3, statistic_4_1,
                          geojson, id_property, area_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        area_sqm = feature["properties"][area_property]
        area_sqkm = area_sqm / 1_000_000

        # Filter statistics
        statistic_1_filtered = statistic_1.loc[
            (statistic_1["nummer"] == int(feature_id))] if statistic_1 is not None else None
        statistic_2_1_filtered = statistic_2_1.loc[
            (statistic_2_1["nummer"] == int(feature_id))] if statistic_2_1 is not None else None
        statistic_3_filtered = statistic_3.loc[
            (statistic_3["nummer"] == int(feature_id))] if statistic_3 is not None else None
        statistic_4_1_filtered = statistic_4_1.loc[
            (statistic_4_1["nummer"] == int(feature_id))] if statistic_4_1 is not None else None

        # Check for missing data
        if statistic_1_filtered is None or statistic_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_1_filtered) for id={feature_id}")
            continue
        if statistic_2_1_filtered is None or statistic_2_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_2_1_filtered) for id={feature_id}")
            continue
        if statistic_3_filtered is None or statistic_3_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_3_filtered) for id={feature_id}")
            continue
        if statistic_4_1_filtered is None or statistic_4_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_1_filtered) for id={feature_id}")
            continue

        # Blend data
        feature = blend_planning_area_data(
            feature=feature, area_sqkm=area_sqkm,
            statistic_1_filtered=statistic_1_filtered,
            statistic_2_1_filtered=statistic_2_1_filtered,
            statistic_3_filtered=statistic_3_filtered,
            statistic_4_1_filtered=statistic_4_1_filtered
        )

        # Build structure
        if year not in statistics:
            statistics[year] = {}
        if half_year not in statistics[year]:
            statistics[year][half_year] = {}

        # Add properties
        statistics[year][half_year][feature_id] = feature["properties"]

    return geojson_extended


def extend_districts_post_2020(logger, statistics, year, half_year,
                               statistic, statistic_2_3, statistic_4_3, statistic_4_3_1,
                               geojson, id_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        district_id = feature_id

        # Filter statistics
        statistic_2_3_filtered = statistic_2_3.loc[
            (statistic_2_3["nummer"] == int(feature_id))] if statistic_2_3 is not None else None
        statistic_4_3_filtered = statistic_4_3.loc[
            (statistic_4_3["nummer"] == int(feature_id))] if statistic_4_3 is not None else None
        statistic_4_3_1_filtered = statistic_4_3_1.loc[
            (statistic_4_3_1["nummer"] == int(feature_id))] if statistic_4_3_1 is not None else None

        # Check for missing data
        if statistic_2_3_filtered is None or statistic_2_3_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_2_3_filtered) for id={feature_id}")
            continue
        if statistic_4_3_filtered is None or statistic_4_3_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_3_filtered) for id={feature_id}")
            continue
        if statistic_4_3_1_filtered is None or statistic_4_3_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_3_1_filtered) for id={feature_id}")
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
        feature = blend_district_data(
            feature=feature, area_sqkm=area_sqkm,
            statistic_2_3_filtered=statistic_2_3_filtered,
            statistic_4_3_filtered=statistic_4_3_filtered
        )

        # Build structure
        if year not in statistics:
            statistics[year] = {}
        if half_year not in statistics[year]:
            statistics[year][half_year] = {}

        # Add properties
        statistics[year][half_year][feature_id] = feature["properties"]

    return geojson_extended


def extend_district_regions_post_2020(logger, statistics, year, half_year,
                                      statistic, statistic_2_2, statistic_4_2, statistic_4_2_1,
                                      geojson, id_property, area_property):
    geojson_extended = copy.deepcopy(geojson)

    # Check if file needs to be created
    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        area_sqm = feature["properties"][area_property]
        area_sqkm = area_sqm / 1_000_000

        # Filter statistics
        statistic_2_2_filtered = statistic_2_2.loc[
            (statistic_2_2["nummer"] == int(feature_id))] if statistic_2_2 is not None else None
        statistic_4_2_filtered = statistic_4_2.loc[
            (statistic_4_2["nummer"] == int(feature_id))] if statistic_4_2 is not None else None
        statistic_4_2_1_filtered = statistic_4_2_1.loc[
            (statistic_4_2_1["nummer"] == int(feature_id))] if statistic_4_2_1 is not None else None

        # Check for missing data
        if statistic_2_2_filtered is None or statistic_2_2_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_2_2_filtered) for id={feature_id}")
            continue
        if statistic_4_2_filtered is None or statistic_4_2_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_2_filtered) for id={feature_id}")
            continue
        if statistic_4_2_1_filtered is None or statistic_4_2_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_2_1_filtered) for id={feature_id}")
            continue

        # Blend data
        feature = blend_district_region_data_post_2020(
            feature=feature, area_sqkm=area_sqkm,
            statistic_2_2_filtered=statistic_2_2_filtered,
            statistic_4_2_filtered=statistic_4_2_filtered,
            statistic_4_2_1_filtered=statistic_4_2_1_filtered
        )

        # Build structure
        if year not in statistics:
            statistics[year] = {}
        if half_year not in statistics[year]:
            statistics[year][half_year] = {}

        # Add properties
        statistics[year][half_year][feature_id] = feature["properties"]

    return geojson_extended


def extend_planning_areas_post_2020(logger, statistics, year, half_year,
                                    statistic, statistic_1, statistic_2_1, statistic_3, statistic_4_1, statistic_4_1_1,
                                    geojson, id_property, area_property):
    geojson_extended = copy.deepcopy(geojson)

    for feature in geojson_extended["features"]:
        feature_id = feature["properties"][id_property]
        area_sqm = feature["properties"][area_property]
        area_sqkm = area_sqm / 1_000_000

        # Filter statistics
        statistic_1_filtered = statistic_1.loc[
            (statistic_1["nummer"] == int(feature_id))] if statistic_1 is not None else None
        statistic_2_1_filtered = statistic_2_1.loc[
            (statistic_2_1["nummer"] == int(feature_id))] if statistic_2_1 is not None else None
        statistic_3_filtered = statistic_3.loc[
            (statistic_3["nummer"] == int(feature_id))] if statistic_3 is not None else None
        statistic_4_1_filtered = statistic_4_1.loc[
            (statistic_4_1["nummer"] == int(feature_id))] if statistic_4_1 is not None else None
        statistic_4_1_1_filtered = statistic_4_1_1.loc[
            (statistic_4_1_1["nummer"] == int(feature_id))] if statistic_4_1_1 is not None else None

        # Check for missing data
        if statistic_1_filtered is None or statistic_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_1_filtered) for id={feature_id}")
            continue
        if statistic_2_1_filtered is None or statistic_2_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_2_1_filtered) for id={feature_id}")
            continue
        if statistic_3_filtered is None or statistic_3_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_3_filtered) for id={feature_id}")
            continue
        if statistic_4_1_filtered is None or statistic_4_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_1_filtered) for id={feature_id}")
            continue
        if statistic_4_1_1_filtered is None or statistic_4_1_1_filtered.shape[0] == 0:
            logger.log_line(f"✗️ No data in {statistic} (statistic_4_1_1_filtered) for id={feature_id}")
            continue

        # Blend data
        feature = blend_planning_area_data_post_2020(
            feature=feature, area_sqkm=area_sqkm,
            statistic_1_filtered=statistic_1_filtered,
            statistic_2_1_filtered=statistic_2_1_filtered,
            statistic_3_filtered=statistic_3_filtered,
            statistic_4_1_filtered=statistic_4_1_filtered,
            statistic_4_1_1_filtered=statistic_4_1_1_filtered
        )

        # Build structure
        if year not in statistics:
            statistics[year] = {}
        if half_year not in statistics[year]:
            statistics[year][half_year] = {}

        # Add properties
        statistics[year][half_year][feature_id] = feature["properties"]

    return geojson_extended


def blend_district_data(feature, area_sqkm, statistic_2_3_filtered, statistic_4_3_filtered):
    # Add new properties
    add_prop_with_modifiers(feature, "inhabitants", statistic_2_3_filtered, "einwohner", area_sqkm)
    add_prop(feature, "s1_percentage_unemployed", statistic_2_3_filtered, "s1_anteil_arbeitslose")
    add_prop(feature, "s2_percentage_long_term_unemployed", statistic_2_3_filtered, "s2_anteil_langzeitarbeitslose")
    add_prop(feature, "s3_percentage_transfer_payments_recipients", statistic_2_3_filtered,
             "s3_anteil_transferbezieher")
    add_prop(feature, "s4_percentage_transfer_payments_recipients_below_15_years", statistic_2_3_filtered,
             "s4_anteil_transferbezieher_unter_15")
    add_prop(feature, "d1_percentage_unemployed", statistic_2_3_filtered, "d1_anteil_arbeitslose")
    add_prop(feature, "d2_percentage_long_term_unemployed", statistic_2_3_filtered, "d2_anteil_langzeitarbeitslose")
    add_prop(feature, "d3_percentage_transfer_payments_recipients", statistic_2_3_filtered,
             "d3_anteil_transferbezieher")
    add_prop(feature, "d4_percentage_transfer_payments_recipients_below_15_years", statistic_4_3_filtered,
             "d4_anteil_transferbezieher_unter_15")
    add_prop(feature, "k01_youth_unemployment", statistic_4_3_filtered, "k01_jugendarbeitslosigkeit")
    add_prop(feature, "k02_single_parent_households", statistic_4_3_filtered, "k02_alleinerziehende_haushalte")
    add_prop(feature, "k03_old_age_poverty", statistic_4_3_filtered, "k03_altersarmut")
    add_prop(feature, "k04_children_with_migration_background", statistic_4_3_filtered,
             "k04_kinder_und_jugendliche_mit_migrationshintergrund")
    add_prop(feature, "k05_inhabitants_with_migration_background", statistic_4_3_filtered,
             "k05_einwohnerinnen_und_einwohner_mit_migrationshintergrund")
    add_prop(feature, "k16_foreigners", statistic_4_3_filtered, "k16_auslaenderinnen_und_auslaender")
    add_prop(feature, "k06_change_proportion_of_foreigner", statistic_4_3_filtered, "k06_veraenderung_auslaenderanteil")
    add_prop(feature, "k17_non_eu_foreigners", statistic_4_3_filtered, "k17_nicht_eu_auslaenderinnen_und_auslaender")
    add_prop(feature, "k07_foreign_transfer_recipients", statistic_4_3_filtered, "k07_auslaendische_transferbezieher")
    add_prop(feature, "k08_urban_apartments", statistic_4_3_filtered, "k08_staedtische_wohnungen")
    add_prop(feature, "k14_living_rooms", statistic_4_3_filtered, "k14_wohnraeume")
    add_prop(feature, "k15_living_space", statistic_4_3_filtered, "k15_wohnflaeche")
    add_prop(feature, "k09_simple_residential_area", statistic_4_3_filtered, "k09_einfache_wohnlage")
    add_prop(feature, "k10_duration_of_residence_over_5_years", statistic_4_3_filtered, "k10_wohndauer_ueber_5_jahre")
    add_prop(feature, "k11_migration_volume", statistic_4_3_filtered, "k11_wanderungsvolumen")
    add_prop(feature, "k12_balance_of_migration", statistic_4_3_filtered, "k12_wanderungssaldo")
    add_prop(feature, "k13_balance_of_migration_of_children_below_6", statistic_4_3_filtered,
             "k13_wanderungssaldo_von_kindern_unter_6_jahren")

    return feature


def blend_district_region_data(feature, area_sqkm, statistic_2_2_filtered, statistic_4_2_filtered):
    # Add new properties
    add_prop_with_modifiers(feature, "inhabitants", statistic_2_2_filtered, "einwohner", area_sqkm)
    add_prop(feature, "s1_percentage_unemployed", statistic_2_2_filtered, "s1_anteil_arbeitslose")
    add_prop(feature, "s2_percentage_long_term_unemployed", statistic_2_2_filtered, "s2_anteil_langzeitarbeitslose")
    add_prop(feature, "s3_percentage_transfer_payments_recipients", statistic_2_2_filtered,
             "s3_anteil_transferbezieher")
    add_prop(feature, "s4_percentage_transfer_payments_recipients_below_15_years", statistic_2_2_filtered,
             "s4_anteil_transferbezieher_unter_15")
    add_prop(feature, "d1_percentage_unemployed", statistic_2_2_filtered, "d1_anteil_arbeitslose")
    add_prop(feature, "d2_percentage_long_term_unemployed", statistic_2_2_filtered, "d2_anteil_langzeitarbeitslose")
    add_prop(feature, "d3_percentage_transfer_payments_recipients", statistic_2_2_filtered,
             "d3_anteil_transferbezieher")
    add_prop(feature, "d4_percentage_transfer_payments_recipients_below_15_years", statistic_2_2_filtered,
             "d4_anteil_transferbezieher_unter_15")
    add_prop(feature, "k01_youth_unemployment", statistic_4_2_filtered, "k01_jugendarbeitslosigkeit")
    add_prop(feature, "k02_single_parent_households", statistic_4_2_filtered, "k02_alleinerziehende_haushalte")
    add_prop(feature, "k03_old_age_poverty", statistic_4_2_filtered, "k03_altersarmut")
    add_prop(feature, "k04_children_with_migration_background", statistic_4_2_filtered,
             "k04_kinder_und_jugendliche_mit_migrationshintergrund")
    add_prop(feature, "k05_inhabitants_with_migration_background", statistic_4_2_filtered,
             "k05_einwohnerinnen_und_einwohner_mit_migrationshintergrund")
    add_prop(feature, "k16_foreigners", statistic_4_2_filtered, "k16_auslaenderinnen_und_auslaender")
    add_prop(feature, "k06_change_proportion_of_foreigner", statistic_4_2_filtered, "k06_veraenderung_auslaenderanteil")
    add_prop(feature, "k17_non_eu_foreigners", statistic_4_2_filtered, "k17_nicht_eu_auslaenderinnen_und_auslaender")
    add_prop(feature, "k07_foreign_transfer_recipients", statistic_4_2_filtered, "k07_auslaendische_transferbezieher")
    add_prop(feature, "k08_urban_apartments", statistic_4_2_filtered, "k08_staedtische_wohnungen")
    add_prop(feature, "k14_living_rooms", statistic_4_2_filtered, "k14_wohnraeume")
    add_prop(feature, "k15_living_space", statistic_4_2_filtered, "k15_wohnflaeche")
    add_prop(feature, "k09_simple_residential_area", statistic_4_2_filtered, "k09_einfache_wohnlage")
    add_prop(feature, "k10_duration_of_residence_over_5_years", statistic_4_2_filtered, "k10_wohndauer_ueber_5_jahre")
    add_prop(feature, "k11_migration_volume", statistic_4_2_filtered, "k11_wanderungsvolumen")
    add_prop(feature, "k12_balance_of_migration", statistic_4_2_filtered, "k12_wanderungssaldo")
    add_prop(feature, "k13_balance_of_migration_of_children_below_6", statistic_4_2_filtered,
             "k13_wanderungssaldo_von_kindern_unter_6_jahren")

    return feature


def blend_planning_area_data(feature, area_sqkm, statistic_1_filtered, statistic_2_1_filtered, statistic_3_filtered,
                             statistic_4_1_filtered):
    # Add new properties
    add_prop_with_modifiers(feature, "inhabitants", statistic_1_filtered, "einwohner", area_sqkm)
    add_prop(feature, "status_index", statistic_1_filtered, "status_index")
    add_prop(feature, "dynamics_index", statistic_1_filtered, "dynamik_index")
    add_prop(feature, "s1_percentage_unemployed", statistic_2_1_filtered, "s1_anteil_arbeitslose")
    add_prop(feature, "s2_percentage_long_term_unemployed", statistic_2_1_filtered, "s2_anteil_langzeitarbeitslose")
    add_prop(feature, "s3_percentage_transfer_payments_recipients", statistic_2_1_filtered,
             "s3_anteil_transferbezieher")
    add_prop(feature, "s4_percentage_transfer_payments_recipients_below_15_years", statistic_2_1_filtered,
             "s4_anteil_transferbezieher_unter_15")
    add_prop(feature, "d1_percentage_unemployed", statistic_2_1_filtered, "d1_anteil_arbeitslose")
    add_prop(feature, "d2_percentage_long_term_unemployed", statistic_2_1_filtered, "d2_anteil_langzeitarbeitslose")
    add_prop(feature, "d3_percentage_transfer_payments_recipients", statistic_2_1_filtered,
             "d3_anteil_transferbezieher")
    add_prop(feature, "d4_percentage_transfer_payments_recipients_below_15_years", statistic_2_1_filtered,
             "d4_anteil_transferbezieher_unter_15")
    add_prop(feature, "z_s1_percentage_unemployed", statistic_3_filtered, "z_s1_anteil_arbeitslose")
    add_prop(feature, "z_s2_percentage_long_term_unemployed", statistic_3_filtered, "z_s2_anteil_langzeitarbeitslose")
    add_prop(feature, "z_s3_percentage_transfer_payments_recipients", statistic_3_filtered,
             "z_s3_anteil_transferbezieher")
    add_prop(feature, "z_s4_percentage_transfer_payments_recipients_below_15_years", statistic_3_filtered,
             "z_s4_anteil_transferbezieher_unter_15")
    add_prop(feature, "z_d1_percentage_unemployed", statistic_3_filtered, "z_d1_anteil_arbeitslose")
    add_prop(feature, "z_d2_percentage_long_term_unemployed", statistic_3_filtered, "z_d2_anteil_langzeitarbeitslose")
    add_prop(feature, "z_d3_percentage_transfer_payments_recipients", statistic_3_filtered,
             "z_d3_anteil_transferbezieher")
    add_prop(feature, "z_d4_percentage_transfer_payments_recipients_below_15_years", statistic_3_filtered,
             "z_d4_anteil_transferbezieher_unter_15")
    add_prop(feature, "k01_youth_unemployment", statistic_4_1_filtered, "k01_jugendarbeitslosigkeit")
    add_prop(feature, "k02_single_parent_households", statistic_4_1_filtered, "k02_alleinerziehende_haushalte")
    add_prop(feature, "k03_old_age_poverty", statistic_4_1_filtered, "k03_altersarmut")
    add_prop(feature, "k04_children_with_migration_background", statistic_4_1_filtered,
             "k04_kinder_und_jugendliche_mit_migrationshintergrund")
    add_prop(feature, "k05_inhabitants_with_migration_background", statistic_4_1_filtered,
             "k05_einwohnerinnen_und_einwohner_mit_migrationshintergrund")
    add_prop(feature, "k16_foreigners", statistic_4_1_filtered, "k16_auslaenderinnen_und_auslaender")
    add_prop(feature, "k06_change_proportion_of_foreigner", statistic_4_1_filtered, "k06_veraenderung_auslaenderanteil")
    add_prop(feature, "k17_non_eu_foreigners", statistic_4_1_filtered, "k17_nicht_eu_auslaenderinnen_und_auslaender")
    add_prop(feature, "k07_foreign_transfer_recipients", statistic_4_1_filtered, "k07_auslaendische_transferbezieher")
    add_prop(feature, "k08_urban_apartments", statistic_4_1_filtered, "k08_staedtische_wohnungen")
    add_prop(feature, "k14_living_rooms", statistic_4_1_filtered, "k14_wohnraeume")
    add_prop(feature, "k15_living_space", statistic_4_1_filtered, "k15_wohnflaeche")
    add_prop(feature, "k09_simple_residential_area", statistic_4_1_filtered, "k09_einfache_wohnlage")
    add_prop(feature, "k10_duration_of_residence_over_5_years", statistic_4_1_filtered, "k10_wohndauer_ueber_5_jahre")
    add_prop(feature, "k11_migration_volume", statistic_4_1_filtered, "k11_wanderungsvolumen")
    add_prop(feature, "k12_balance_of_migration", statistic_4_1_filtered, "k12_wanderungssaldo")
    add_prop(feature, "k13_balance_of_migration_of_children_below_6", statistic_4_1_filtered,
             "k13_wanderungssaldo_von_kindern_unter_6_jahren")

    return feature


def blend_district_data_post_2020(feature, area_sqkm, statistic_2_3_filtered, statistic_4_3_filtered,
                                  statistic_4_3_1_filtered):
    # Add new properties
    add_prop_with_modifiers(feature, "inhabitants", statistic_2_3_filtered, "einwohner", area_sqkm)
    add_prop(feature, "s1_percentage_unemployed", statistic_2_3_filtered, "s1_anteil_arbeitslose")
    add_prop(feature, "s2_percentage_long_term_unemployed", statistic_2_3_filtered, "s2_anteil_langzeitarbeitslose")
    add_prop(feature, "s3_percentage_transfer_payments_recipients", statistic_2_3_filtered,
             "s3_anteil_transferbezieher")
    add_prop(feature, "s4_percentage_transfer_payments_recipients_below_15_years", statistic_2_3_filtered,
             "s4_anteil_transferbezieher_unter_15")
    add_prop(feature, "d1_percentage_unemployed", statistic_2_3_filtered, "d1_anteil_arbeitslose")
    add_prop(feature, "d2_percentage_long_term_unemployed", statistic_2_3_filtered, "d2_anteil_langzeitarbeitslose")
    add_prop(feature, "d3_percentage_transfer_payments_recipients", statistic_2_3_filtered,
             "d3_anteil_transferbezieher")
    add_prop(feature, "d4_percentage_transfer_payments_recipients_below_15_years", statistic_4_3_filtered,
             "d4_anteil_transferbezieher_unter_15")
    add_prop(feature, "k01_youth_unemployment", statistic_4_3_filtered, "k01_jugendarbeitslosigkeit")
    add_prop(feature, "k02_single_parent_households", statistic_4_3_filtered, "k02_alleinerziehende_haushalte")
    add_prop(feature, "k03_old_age_poverty", statistic_4_3_filtered, "k03_altersarmut")
    add_prop(feature, "k04_children_with_migration_background", statistic_4_3_filtered,
             "k04_kinder_und_jugendliche_mit_migrationshintergrund")
    add_prop(feature, "k05_inhabitants_with_migration_background", statistic_4_3_filtered,
             "k05_einwohnerinnen_und_einwohner_mit_migrationshintergrund")
    add_prop(feature, "k16_foreigners", statistic_4_3_filtered, "k16_auslaenderinnen_und_auslaender")
    add_prop(feature, "k06_change_proportion_of_foreigner", statistic_4_3_filtered, "k06_veraenderung_auslaenderanteil")
    add_prop(feature, "k17_non_eu_foreigners", statistic_4_3_filtered, "k17_nicht_eu_auslaenderinnen_und_auslaender")
    add_prop(feature, "k07_foreign_transfer_recipients", statistic_4_3_filtered, "k07_auslaendische_transferbezieher")
    add_prop(feature, "k08_urban_apartments", statistic_4_3_1_filtered, "k08_staedtische_wohnungen")
    add_prop(feature, "k14_living_rooms", statistic_4_3_1_filtered, "k14_wohnraeume")
    add_prop(feature, "k15_living_space", statistic_4_3_1_filtered, "k15_wohnflaeche")
    add_prop(feature, "k09_simple_residential_area", statistic_4_3_filtered, "k09_einfache_wohnlage")
    add_prop(feature, "k10_duration_of_residence_over_5_years", statistic_4_3_filtered, "k10_wohndauer_ueber_5_jahre")
    add_prop(feature, "k11_migration_volume", statistic_4_3_filtered, "k11_wanderungsvolumen")
    add_prop(feature, "k12_balance_of_migration", statistic_4_3_filtered, "k12_wanderungssaldo")
    add_prop(feature, "k13_balance_of_migration_of_children_below_6", statistic_4_3_filtered,
             "k13_wanderungssaldo_von_kindern_unter_6_jahren")

    return feature


def blend_district_region_data_post_2020(feature, area_sqkm, statistic_2_2_filtered, statistic_4_2_filtered,
                                         statistic_4_2_1_filtered):
    # Add new properties
    add_prop_with_modifiers(feature, "inhabitants", statistic_2_2_filtered, "einwohner", area_sqkm)
    add_prop(feature, "s1_percentage_unemployed", statistic_2_2_filtered, "s1_anteil_arbeitslose")
    add_prop(feature, "s2_percentage_long_term_unemployed", statistic_2_2_filtered, "s2_anteil_langzeitarbeitslose")
    add_prop(feature, "s3_percentage_transfer_payments_recipients", statistic_2_2_filtered,
             "s3_anteil_transferbezieher")
    add_prop(feature, "s4_percentage_transfer_payments_recipients_below_15_years", statistic_2_2_filtered,
             "s4_anteil_transferbezieher_unter_15")
    add_prop(feature, "d1_percentage_unemployed", statistic_2_2_filtered, "d1_anteil_arbeitslose")
    add_prop(feature, "d2_percentage_long_term_unemployed", statistic_2_2_filtered, "d2_anteil_langzeitarbeitslose")
    add_prop(feature, "d3_percentage_transfer_payments_recipients", statistic_2_2_filtered,
             "d3_anteil_transferbezieher")
    add_prop(feature, "d4_percentage_transfer_payments_recipients_below_15_years", statistic_2_2_filtered,
             "d4_anteil_transferbezieher_unter_15")
    add_prop(feature, "k01_youth_unemployment", statistic_4_2_filtered, "k01_jugendarbeitslosigkeit")
    add_prop(feature, "k02_single_parent_households", statistic_4_2_filtered, "k02_alleinerziehende_haushalte")
    add_prop(feature, "k03_old_age_poverty", statistic_4_2_filtered, "k03_altersarmut")
    add_prop(feature, "k04_children_with_migration_background", statistic_4_2_filtered,
             "k04_kinder_und_jugendliche_mit_migrationshintergrund")
    add_prop(feature, "k05_inhabitants_with_migration_background", statistic_4_2_filtered,
             "k05_einwohnerinnen_und_einwohner_mit_migrationshintergrund")
    add_prop(feature, "k16_foreigners", statistic_4_2_filtered, "k16_auslaenderinnen_und_auslaender")
    add_prop(feature, "k06_change_proportion_of_foreigner", statistic_4_2_filtered, "k06_veraenderung_auslaenderanteil")
    add_prop(feature, "k17_non_eu_foreigners", statistic_4_2_filtered, "k17_nicht_eu_auslaenderinnen_und_auslaender")
    add_prop(feature, "k07_foreign_transfer_recipients", statistic_4_2_filtered, "k07_auslaendische_transferbezieher")
    add_prop(feature, "k08_urban_apartments", statistic_4_2_1_filtered, "k08_staedtische_wohnungen")
    add_prop(feature, "k14_living_rooms", statistic_4_2_1_filtered, "k14_wohnraeume")
    add_prop(feature, "k15_living_space", statistic_4_2_1_filtered, "k15_wohnflaeche")
    add_prop(feature, "k09_simple_residential_area", statistic_4_2_filtered, "k09_einfache_wohnlage")
    add_prop(feature, "k10_duration_of_residence_over_5_years", statistic_4_2_filtered, "k10_wohndauer_ueber_5_jahre")
    add_prop(feature, "k11_migration_volume", statistic_4_2_filtered, "k11_wanderungsvolumen")
    add_prop(feature, "k12_balance_of_migration", statistic_4_2_filtered, "k12_wanderungssaldo")
    add_prop(feature, "k13_balance_of_migration_of_children_below_6", statistic_4_2_filtered,
             "k13_wanderungssaldo_von_kindern_unter_6_jahren")

    return feature


def blend_planning_area_data_post_2020(feature, area_sqkm, statistic_1_filtered, statistic_2_1_filtered,
                                       statistic_3_filtered, statistic_4_1_filtered, statistic_4_1_1_filtered):
    # Add new properties
    add_prop_with_modifiers(feature, "inhabitants", statistic_1_filtered, "einwohner", area_sqkm)
    add_prop(feature, "status_index", statistic_1_filtered, "status_index")
    add_prop(feature, "dynamics_index", statistic_1_filtered, "dynamik_index")
    add_prop(feature, "s1_percentage_unemployed", statistic_2_1_filtered, "s1_anteil_arbeitslose")
    add_prop(feature, "s2_percentage_long_term_unemployed", statistic_2_1_filtered, "s2_anteil_langzeitarbeitslose")
    add_prop(feature, "s3_percentage_transfer_payments_recipients", statistic_2_1_filtered,
             "s3_anteil_transferbezieher")
    add_prop(feature, "s4_percentage_transfer_payments_recipients_below_15_years", statistic_2_1_filtered,
             "s4_anteil_transferbezieher_unter_15")
    add_prop(feature, "d1_percentage_unemployed", statistic_2_1_filtered, "d1_anteil_arbeitslose")
    add_prop(feature, "d2_percentage_long_term_unemployed", statistic_2_1_filtered, "d2_anteil_langzeitarbeitslose")
    add_prop(feature, "d3_percentage_transfer_payments_recipients", statistic_2_1_filtered,
             "d3_anteil_transferbezieher")
    add_prop(feature, "d4_percentage_transfer_payments_recipients_below_15_years", statistic_2_1_filtered,
             "d4_anteil_transferbezieher_unter_15")
    add_prop(feature, "z_s1_percentage_unemployed", statistic_3_filtered, "z_s1_anteil_arbeitslose")
    add_prop(feature, "z_s2_percentage_long_term_unemployed", statistic_3_filtered, "z_s2_anteil_langzeitarbeitslose")
    add_prop(feature, "z_s3_percentage_transfer_payments_recipients", statistic_3_filtered,
             "z_s3_anteil_transferbezieher")
    add_prop(feature, "z_s4_percentage_transfer_payments_recipients_below_15_years", statistic_3_filtered,
             "z_s4_anteil_transferbezieher_unter_15")
    add_prop(feature, "z_d1_percentage_unemployed", statistic_3_filtered, "z_d1_anteil_arbeitslose")
    add_prop(feature, "z_d2_percentage_long_term_unemployed", statistic_3_filtered, "z_d2_anteil_langzeitarbeitslose")
    add_prop(feature, "z_d3_percentage_transfer_payments_recipients", statistic_3_filtered,
             "z_d3_anteil_transferbezieher")
    add_prop(feature, "z_d4_percentage_transfer_payments_recipients_below_15_years", statistic_3_filtered,
             "z_d4_anteil_transferbezieher_unter_15")
    add_prop(feature, "k01_youth_unemployment", statistic_4_1_filtered, "k01_jugendarbeitslosigkeit")
    add_prop(feature, "k02_single_parent_households", statistic_4_1_filtered, "k02_alleinerziehende_haushalte")
    add_prop(feature, "k03_old_age_poverty", statistic_4_1_filtered, "k03_altersarmut")
    add_prop(feature, "k04_children_with_migration_background", statistic_4_1_filtered,
             "k04_kinder_und_jugendliche_mit_migrationshintergrund")
    add_prop(feature, "k05_inhabitants_with_migration_background", statistic_4_1_filtered,
             "k05_einwohnerinnen_und_einwohner_mit_migrationshintergrund")
    add_prop(feature, "k16_foreigners", statistic_4_1_filtered, "k16_auslaenderinnen_und_auslaender")
    add_prop(feature, "k06_change_proportion_of_foreigner", statistic_4_1_filtered, "k06_veraenderung_auslaenderanteil")
    add_prop(feature, "k17_non_eu_foreigners", statistic_4_1_filtered, "k17_nicht_eu_auslaenderinnen_und_auslaender")
    add_prop(feature, "k07_foreign_transfer_recipients", statistic_4_1_filtered, "k07_auslaendische_transferbezieher")
    add_prop(feature, "k08_urban_apartments", statistic_4_1_1_filtered, "k08_staedtische_wohnungen")
    add_prop(feature, "k14_living_rooms", statistic_4_1_1_filtered, "k14_wohnraeume")
    add_prop(feature, "k15_living_space", statistic_4_1_1_filtered, "k15_wohnflaeche")
    add_prop(feature, "k09_simple_residential_area", statistic_4_1_filtered, "k09_einfache_wohnlage")
    add_prop(feature, "k10_duration_of_residence_over_5_years", statistic_4_1_filtered, "k10_wohndauer_ueber_5_jahre")
    add_prop(feature, "k11_migration_volume", statistic_4_1_filtered, "k11_wanderungsvolumen")
    add_prop(feature, "k12_balance_of_migration", statistic_4_1_filtered, "k12_wanderungssaldo")
    add_prop(feature, "k13_balance_of_migration_of_children_below_6", statistic_4_1_filtered,
             "k13_wanderungssaldo_von_kindern_unter_6_jahren")

    return feature


def add_prop(feature, property_name, statistics, statistics_property_name):
    if statistics_property_name in statistics:
        try:
            feature["properties"][f"{property_name}"] = float(statistics[statistics_property_name])
        except ValueError:
            feature["properties"][f"{property_name}"] = 0


def add_prop_with_modifiers(feature, property_name, statistics, statistics_property_name, total_area_sqkm):
    if statistics_property_name in statistics:
        try:
            feature["properties"][f"{property_name}"] = float(statistics[statistics_property_name].sum())
            if total_area_sqkm is not None:
                feature["properties"][f"{property_name}_per_sqkm"] = round(
                    float(statistics[statistics_property_name].sum()) / total_area_sqkm)
        except ValueError:
            feature["properties"][f"{property_name}"] = 0

            if total_area_sqkm is not None:
                feature["properties"][f"{property_name}_per_sqkm"] = 0


#
# Main
#

class LorStatisticsMonitoringSocialUrbanDevelopmentDataBlender:

    @TrackingDecorator.track_time
    def run(self, logger, data_path, statistics_path, results_path, clean=False, quiet=False):
        # Make results path
        os.makedirs(os.path.join(results_path), exist_ok=True)

        # Statistics
        statistics_lor_districts = {}
        statistics_lor_district_regions = {}
        statistics_lor_planning_areas = {}

        # Load geojson
        geojson_lor_districts = read_geojson_file(os.path.join(data_path, "bezirksgrenzen.geojson"))
        geojson_lor_district_regions = read_geojson_file(os.path.join(data_path, "lor_bezirksregionen.geojson"))
        geojson_lor_planning_areas = read_geojson_file(os.path.join(data_path, "lor_planungsraeume.geojson"))

        # Iterate over statistics
        for statistic_bundle in pre_2020_statistics:
            year = statistic_bundle[0][-4:]
            half_year = "00"

            # Load statistics
            statistic_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[0]}.csv"))
            statistic_2_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[1]}.csv"))
            statistic_2_2 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[2]}.csv"))
            statistic_2_3 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[3]}.csv"))
            statistic_3 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[4]}.csv"))
            statistic_4_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[5]}.csv"))
            statistic_4_2 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[6]}.csv"))
            statistic_4_3 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[7]}.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts(
                logger=logger,
                statistics=statistics_lor_districts,
                year=year,
                half_year=half_year,
                statistic=f"bezirksgrenzen_monitoring_social_urban_development_{year}_{half_year}",
                statistic_2_3=statistic_2_3,
                statistic_4_3=statistic_4_3,
                id_property="id",
                geojson=geojson_lor_districts
            )

            # Extend district regions
            geojson_lor_district_regions_extended = extend_district_regions(
                logger=logger,
                statistics=statistics_lor_district_regions,
                year=year,
                half_year=half_year,
                statistic=f"lor_bezirksregionen_monitoring_social_urban_development_{year}_{half_year}",
                statistic_2_2=statistic_2_2,
                statistic_4_2=statistic_4_2,
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
                statistic=f"lor_planungsraeume_monitoring_social_urban_development_{year}_{half_year}",
                statistic_1=statistic_1,
                statistic_2_1=statistic_2_1,
                statistic_3=statistic_3,
                statistic_4_1=statistic_4_1,
                geojson=geojson_lor_planning_areas,
                id_property="id",
                area_property="area"
            )

            # Write geojson files
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_bezirksregionen_monitoring_social_urban_development_{year}_{half_year}.geojson"),
                statistic_name=f"bezirksgrenzen_monitoring_social_urban_development_{year}_{half_year}",
                geojson_content=geojson_lor_districts_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_bezirksregionen_monitoring_social_urban_development_{year}_{half_year}.geojson"),
                statistic_name=f"lor_bezirksregionen_monitoring_social_urban_development_{year}_{half_year}",
                geojson_content=geojson_lor_district_regions_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_planungsraeume_monitoring_social_urban_development_{year}_{half_year}.geojson"),
                statistic_name=f"lor_planungsraeume_monitoring_social_urban_development_{year}_{half_year}",
                geojson_content=geojson_lor_planning_areas_extended,
                clean=clean,
                quiet=quiet
            )

        # Load geojson
        geojson_lor_districts = read_geojson_file(os.path.join(data_path, "bezirksgrenzen.geojson"))
        geojson_lor_district_regions = read_geojson_file(os.path.join(data_path, "lor_bezirksregionen_2021.geojson"))
        geojson_lor_planning_areas = read_geojson_file(os.path.join(data_path, "lor_planungsraeume_2021.geojson"))

        # Iterate over statistics
        for statistic_bundle in post_2020_statistics:
            year = statistic_bundle[0][-4:]
            half_year = "00"

            # Load statistics
            statistic_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[0]}.csv"))
            statistic_2_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[1]}.csv"))
            statistic_2_2 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[2]}.csv"))
            statistic_2_3 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[3]}.csv"))
            statistic_3 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[4]}.csv"))
            statistic_4_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[5]}.csv"))
            statistic_4_1_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[6]}.csv"))
            statistic_4_2 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[7]}.csv"))
            statistic_4_2_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[8]}.csv"))
            statistic_4_3 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[9]}.csv"))
            statistic_4_3_1 = read_csv_file(os.path.join(statistics_path, f"{statistic_bundle[10]}.csv"))

            # Extend districts
            geojson_lor_districts_extended = extend_districts_post_2020(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic=f"bezirksgrenzen_monitoring_social_urban_development_{year}_{half_year}",
                statistic_2_3=statistic_2_3,
                statistic_4_3=statistic_4_3,
                statistic_4_3_1=statistic_4_3_1,
                id_property="id",
                geojson=geojson_lor_districts
            )

            # Extend district regions
            geojson_lor_district_regions_extended = extend_district_regions_post_2020(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic=f"lor_bezirksregionen_monitoring_social_urban_development_{year}_{half_year}",
                statistic_2_2=statistic_2_2,
                statistic_4_2=statistic_4_2,
                statistic_4_2_1=statistic_4_2_1,
                geojson=geojson_lor_district_regions,
                id_property="id",
                area_property="area"
            )

            # Extend planning areas
            geojson_lor_planning_areas_extended = extend_planning_areas_post_2020(
                logger=logger,
                statistics=statistics_lor_planning_areas,
                year=year,
                half_year=half_year,
                statistic=f"lor_planungsraeume_monitoring_social_urban_development_{year}_{half_year}",
                statistic_1=statistic_1,
                statistic_2_1=statistic_2_1,
                statistic_3=statistic_3,
                statistic_4_1=statistic_4_1,
                statistic_4_1_1=statistic_4_1_1,
                geojson=geojson_lor_planning_areas,
                id_property="id",
                area_property="area"
            )

            # Write geojson files
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_bezirksregionen_monitoring_social_urban_development_{year}_{half_year}.geojson"),
                statistic_name=f"bezirksgrenzen_monitoring_social_urban_development_{year}_{half_year}",
                geojson_content=geojson_lor_districts_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_bezirksregionen_monitoring_social_urban_development_{year}_{half_year}.geojson"),
                statistic_name=f"lor_bezirksregionen_monitoring_social_urban_development_{year}_{half_year}",
                geojson_content=geojson_lor_district_regions_extended,
                clean=clean,
                quiet=quiet
            )
            write_geojson_file(
                logger=logger,
                file_path=os.path.join(results_path,
                                       f"lor_planungsraeume_monitoring_social_urban_development_{year}_{half_year}.geojson"),
                statistic_name=f"lor_planungsraeume_monitoring_social_urban_development_{year}_{half_year}",
                geojson_content=geojson_lor_planning_areas_extended,
                clean=clean,
                quiet=quiet
            )

        # Write json file
        write_json_file(
            logger=logger,
            file_path=os.path.join(results_path, "bezirksgrenzen_monitoring_social_urban_development_statistics.json"),
            statistic_name=f"bezirksgrenzen_monitoring_social_urban_development",
            json_content=statistics_lor_districts,
            clean=clean,
            quiet=quiet
        )
        write_json_file(
            logger=logger,
            file_path=os.path.join(results_path,
                                   "lor_bezirksregionen_monitoring_social_urban_development_statistics.json"),
            statistic_name=f"lor_bezirksregionen_monitoring_social_urban_development",
            json_content=statistics_lor_district_regions,
            clean=clean,
            quiet=quiet
        )
        write_json_file(
            logger=logger,
            file_path=os.path.join(results_path,
                                   "lor_planungsraeume_monitoring_social_urban_development_statistics.json"),
            statistic_name=f"lor_planungsraeume_monitoring_social_urban_development",
            json_content=statistics_lor_planning_areas,
            clean=clean,
            quiet=quiet
        )
