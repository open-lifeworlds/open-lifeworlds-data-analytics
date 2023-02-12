import os

import pandas as pd
import requests
from tracking_decorator import TrackingDecorator


def download_file(logger, file_path, url, clean, quiet):
    # Check if result needs to be generated
    if clean or not os.path.exists(file_path):

        try:
            data = requests.get(url)
            if str(data.status_code).startswith("2"):
                with open(file_path, "wb") as file:
                    file.write(data.content)
                if not quiet:
                    logger.log_line(f"✓ Download {file_path}")
            elif not quiet:
                logger.log_line(f"✗️ Error: {str(data.status_code)}, url {url}")
        except Exception as e:
            logger.log_line(f"✗️ Exception: {str(e)}, url {url}")
            return None

    elif not quiet:
        logger.log_line(f"✓ Already exists {file_path}")


def convert_file_to_csv(logger, file_path, clean=False, quiet=False):
    if not os.path.exists(file_path):
        return

    file_name, file_extension = os.path.splitext(file_path)
    file_name_base = os.path.basename(file_name)
    year = file_name[-4:]

    if file_extension == ".xlsx":
        engine = "openpyxl"
    elif file_extension == ".xls":
        engine = None
    else:
        return

    try:
        if file_name_base.startswith("1-sdi_mss") \
                or file_name_base.startswith("tabelle_1_gesamtindex_soziale_ungleichheit_sdi_mss_"):

            sheets = [f"1.SDI_MSS{year}"]
            skiprows = 3
            names = ["nummer", "name", "einwohner", "status_index", "status_index_klasse", "dynamik_index",
                     "dynamik_index_klasse", "status_dynamik_index", "status_dynamik_index_klasse"]
        elif file_name_base.startswith("2-1-indexind_anteile_plr_mss") \
                or file_name_base.startswith("tabelle_2-1_index-indikatoren_anteilswerte_auf_planungsraum-ebene_mss_"):

            sheets = [f"2.1.IndexInd_Ant_PLR_MSS{year}"]
            skiprows = 8
            if year == "2019" or year == "2021":
                names = ["nummer", "name", "einwohner",
                     "s1_anteil_arbeitslose", "_",
                     "s3_anteil_transferbezieher", "s4_anteil_transferbezieher_unter_15",
                     "d1_anteil_arbeitslose", "_2",
                     "d3_anteil_transferbezieher", "d4_anteil_transferbezieher_unter_15"]
            else:
                names = ["nummer", "name", "einwohner",
                     "s1_anteil_arbeitslose", "s2_anteil_langzeitarbeitslose",
                     "s3_anteil_transferbezieher", "s4_anteil_transferbezieher_unter_15",
                     "d1_anteil_arbeitslose", "d2_anteil_langzeitarbeitsose",
                     "d3_anteil_transferbezieher", "d4_anteil_transferbezieher_unter_15"]
        else:
            sheets = []
            skiprows = 0
            names = []

        for sheet in sheets:

            # Define file path
            file_path_csv = f"{file_name}.csv"

            # Check if result needs to be generated
            if clean or not os.path.exists(file_path_csv):

                # Convert Excel file to csv
                pd.read_excel(file_path, engine=engine, sheet_name=sheet, skiprows=skiprows,
                              usecols=list(range(0, len(names))), names=names) \
                    .drop(columns=["_", "_2"], errors="ignore") \
                    .dropna() \
                    .to_csv(file_path_csv, index=False)

                if not quiet:
                    logger.log_line(f"✓ Convert {file_path_csv}")
            elif not quiet:
                logger.log_line(f"✓ Already exists {file_path_csv}")
    except Exception as e:
        logger.log_line(f"✗️ Exception: {str(e)}")


#
# Main
#

class LorStatisticsMonitoringSocialUrbanDevelopmentDataLoader:

    @TrackingDecorator.track_time
    def run(self, logger, results_path, clean=False, quiet=False):
        # Make results path
        os.makedirs(os.path.join(results_path), exist_ok=True)

        base_url = "https://www.berlin.de/sen/sbw/_assets/stadtdaten/stadtwissen/monitoring-soziale-stadtentwicklung/"

        # Build resources
        resources = []
        for year in [2013, 2015, 2017]:
            resources += [
                # 1. Gesamtindex Soziale Ungleichheit (Status/d1-Index)
                (base_url + f"bericht-{year}", f"1-sdi_mss{year}.xlsx"),
                # 2.1 Index-Indikatoren nach Status und d1 (Anteilswerte) auf Ebene der Planungsräume
                (base_url + f"bericht-{year}", f"2-1-indexind_anteile_plr_mss{year}.xlsx"),
                # 2.2 Index-Indikatoren nach Status und d1 (Anteilswerte) auf Ebene der Bezirksregionen
                (base_url + f"bericht-{year}", f"2-2-indexind_anteile_bzr_mss{year}.xlsx"),
                # 2.3 Index-Indikatoren nach Status und d1 (Anteilswerte) auf Ebene der Bezirke
                (base_url + f"bericht-{year}", f"2-3-indexind_anteile_bezirke_mss{year}.xlsx"),
                # 3 Index-Indikatoren nach Status und d1 – standardisierte Anteilswerte (z-Werte) auf Ebene der Planungsräume
                (base_url + f"bericht-{year}", f"3-indexind_z_wertemss{year}.xlsx"),
                # 4.1. Kontext-Indikatoren auf Ebene der Planungsräume
                (base_url + f"bericht-{year}", f"4-1-kontextind_anteile_plr_mss{year}.xlsx"),
                # 4.2. Kontext-Indikatoren auf Ebene der Bezirksregionen
                (base_url + f"bericht-{year}", f"4-2-kontextind_anteile_bzr_mss{year}.xlsx"),
                # 4.3. Kontext-Indikatoren auf Ebene der Bezirke
                (base_url + f"bericht-{year}", f"4-3-kontextind_anteile_bezirke_mss{year}.xlsx"),
            ]
        for year in [2019]:
            resources += [
                # 1. Gesamtindex Soziale Ungleichheit (Status/d1-Index)
                (base_url + f"bericht-{year}", f"1-sdi_mss{year}.xlsx"),
                # 2.1 Index-Indikatoren nach Status und d1 (Anteilswerte) auf Ebene der Planungsräume
                (base_url + f"bericht-{year}", f"2-1-indexind_anteile_plr_mss{year}.xlsx"),
                # 2.2 Index-Indikatoren nach Status und d1 (Anteilswerte) auf Ebene der Bezirksregionen
                (base_url + f"bericht-{year}", f"2-2-indexind_anteile_bzr_mss{year}.xlsx"),
                # 2.3 Index-Indikatoren nach Status und d1 (Anteilswerte) auf Ebene der Bezirke
                (base_url + f"bericht-{year}", f"2-3-indexind_anteile_bezirke_mss{year}.xlsx"),
                # 3 Index-Indikatoren nach Status und d1 – standardisierte Anteilswerte (z-Werte) auf Ebene der Planungsräume
                (base_url + f"bericht-{year}", f"3-indexind_z_wertemss{year}.xlsx"),
                # 4.1. Kontext-Indikatoren auf Ebene der Planungsräume
                (base_url + f"bericht-{year}", f"4.1.kontextind_anteile_plr_mss{year}.xlsx"),
                # 4.2. Kontext-Indikatoren auf Ebene der Bezirksregionen
                (base_url + f"bericht-{year}", f"4.2.kontextind_anteile_bzr_mss{year}.xlsx"),
                # 4.3. Kontext-Indikatoren auf Ebene der Bezirke
                (base_url + f"bericht-{year}", f"4.3.kontextind_anteile_bezirke_mss{year}.xlsx"),
            ]
        for year in [2021]:
            resources += [
                # 1. Gesamtindex Soziale Ungleichheit (Status/d1-Index)
                (base_url + f"bericht-{year}", f"tabelle_1_gesamtindex_soziale_ungleichheit_sdi_mss_{year}.xlsx"),
                # 2.1 Index-Indikatoren nach Status und d1 – Anteilswerte – auf Ebene der Bezirksregionen (143 BZR)
                (base_url + f"bericht-{year}",
                 f"tabelle_2-1_index-indikatoren_anteilswerte_auf_planungsraum-ebene_mss_{year}.xlsx"),
                # 2.2 Index-Indikatoren nach Status und d1 – Anteilswerte – auf Ebene der Bezirksregionen (143 BZR)
                (base_url + f"bericht-{year}",
                 f"tabelle_2-2_index-indikatoren_anteilswerte_auf_bezirksregionen-ebene_mss_{year}.xlsx"),
                # 2.3 Index-Indikatoren nach Status und d1 – Anteilswerte – auf Ebene der Bezirke
                (base_url + f"bericht-{year}", f"tabelle_2-3_index-indikatoren_auf_ebene_der_bezirke_mss_{year}.xlsx"),
                # 3 Index-Indikatoren nach Status und d1 – standardisierte Anteilswerte (z-Werte) – auf Ebene der Planungsräume (542 PLR)
                (base_url + f"bericht-{year}", f"tabelle_3_index-indikatoren_z-werte_mss_{year}.xlsx"),
                # 4.1. Kontext-Indikatoren (31.12.2020, Veränderung 31.12.2018 bis 31.12.2020) auf Ebene der Planungsräume
                (base_url + f"bericht-{year}", f"tabelle_4-1_kontext-indikatoren_anteile_plr_mss_{year}.xlsx"),
                # 4.2 Kontext-Indikatoren (31.12.2020, Veränderung 31.12.2018 bis 31.12.2020) auf Ebene der Bezirksregionen (143 BZR – die neuen LOR-Grenzen ab 2021)
                (base_url + f"bericht-{year}", f"tabelle_4-2_kontext-indikatoren_anteile_bzr_mss_{year}.xlsx"),
                # 4.3. Kontext-Indikatoren (31.12.2020, Veränderung 31.12.2018 bis 31.12. 2020) auf Ebene der Bezirke
                (base_url + f"bericht-{year}", f"tabelle_4-3_kontext-indikatoren_anteile_bezirke_mss_{year}.xlsx"),
                # 4.1.1. Kontext-Indikatoren K08, K14, K15 (31.12.2020) auf Ebene der Planungsräume (447 PLR – LOR-Grenzen bis 2018)
                (base_url + f"bericht-{year}", f"tabelle_4-1-1_kontext-indikatoren_anteile_plr_mss_{year}.xlsx"),
                # 4.2.1. Kontext-Indikatoren K08, K14, K15 (31.12.2020) auf Ebene der Bezirksregionen (138 BZR – LOR-Grenzen bis 2018)
                (base_url + f"bericht-{year}", f"tabelle_4-2-1_kontext-indikatoren_anteile_bzr_mss_{year}.xlsx"),
                # 4.3.1. Kontext-Indikatoren K08, K14, K15 (31.12.2020) auf Ebene der Bezirke
                (base_url + f"bericht-{year}", f"tabelle_4-3-1_kontext-indikatoren_anteile_bezirke_mss_{year}.xlsx"),
            ]

        for url, file_name in resources:
            # Define file path
            file_path = os.path.join(results_path, file_name)

            # Download file
            download_file(
                logger=logger,
                file_path=file_path,
                url=f"{url}/{file_name}",
                clean=clean,
                quiet=quiet
            )

            # Convert file to csv
            convert_file_to_csv(
                logger=logger,
                file_path=file_path,
                clean=clean,
                quiet=quiet
            )
