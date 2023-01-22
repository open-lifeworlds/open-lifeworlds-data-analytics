import os

import pandas as pd
import requests
from tracking_decorator import TrackingDecorator


def download_file(logger, file_path, url):
    try:
        data = requests.get(url)
        with open(file_path, 'wb') as file:
            file.write(data.content)
    except Exception as e:
        logger.log_line(f"✗️ Exception: {str(e)}")
        return None


def convert_file_to_csv(logger, file_path, clean=False, quiet=False):
    file_name, file_extension = os.path.splitext(file_path)

    if file_extension == ".xlsx" and "SB_A01-16-00_2020h01_BE" not in file_name:
        engine = "openpyxl"
    elif file_extension == ".xls":
        engine = None
    else:
        return

    try:
        sheets = ["Schlüssel 31.12.2020", "T1a", "T2a", "T3a", "T4a",
                  "Schlüssel 01.01.2021", "T1b", "T2b", "T3b", "T4b"] \
            if "SB_A01-16-00_2020h02_BE" in file_name else ["Schlüssel", "T1", "T2", "T3", "T4"]

        # Iterate over sheets
        for sheet in sheets:
            # Define file path
            file_path_csv = f"{file_name}_{sheet.replace(' ', '_')}.csv"

            if "Schlüssel" in sheet:
                skiprows = 3
                names = [
                    "bezirk", "prognoseraum", "bezirksregion", "planungsraum", "planungsraumname"
                ]
            elif "T1" in sheet:
                skiprows = 7
                names = [
                    "bezirk", "prognoseraum", "bezirksregion", "planungsraum",
                    "insgesamt_anzahl", "insgesamt_prozentual",
                    "darunter_mit_migrationshintergrund_anzahl", "darunter_mit_migrationshintergrund_prozentual",
                    "deutsche_zusammen_anzahl", "deutsche_zusammen_prozentual",
                    "deutsche_ohne_migrationshintergrund_anzahl", "deutsche_ohne_migrationshintergrund_prozentual",
                    "deutsche_mit_migrationshintergrund_anzahl", "deutsche_mit_migrationshintergrund_prozentual",
                    "auslaender_anzahl", "auslaender_prozentual"
                ]
            elif "T2" in sheet:
                skiprows = 4
                names = [
                    "bezirk", "prognoseraum", "bezirksregion", "planungsraum",
                    "insgesamt",
                    "alter_unter_6", "alter_6-15", "alter_15-18", "alter_18-27", "alter_27-45", "alter_45-55",
                    "alter_55-65", "alter_65_und_mehr",
                    "weiblich", "auslaender"
                ]
            elif "T3" in sheet:
                skiprows = 4
                names = [
                    "bezirk", "prognoseraum", "bezirksregion", "planungsraum",
                    "insgesamt",
                    "alter_unter_6", "alter_6-15", "alter_15-18", "alter_18-27", "alter_27-45", "alter_45-55",
                    "alter_55-65", "alter_65_und_mehr", "weiblich", "auslaender"
                ]
            elif "T4" in sheet:
                skiprows = 6
                names = [
                    "bezirk", "prognoseraum", "bezirksregion", "planungsraum",
                    "insgesamt",
                    "europaeische_union", "frankreich", "griechenland", "italien", "oesterreich", "spanien", "polen",
                    "bulgarien", "rumaenien", "kroatien", "vereinigtes_koenigreich", "ehemaliges_jugoslawien",
                    "bosnien_und_herzegowina", "serbien", "ehemalige_sowjetunion", "russische_foederation", "ukraine",
                    "kasachstan", "islamische_laender", "tuerkei", "iran", "arabische_laender", "libanon", "syrien",
                    "vietnam", "vereinigte_staaten", "nicht_eindeutig_zuordenbar_ohne_angabe"
                ]
            else:
                skiprows = 0
                names = []

            # Check if result needs to be generated
            if clean or not os.path.exists(file_path_csv):

                # Convert Excel file to csv
                pd.read_excel(file_path, engine=engine, sheet_name=sheet, skiprows=skiprows,
                              usecols=list(range(0, len(names))), names=names) \
                    .drop(columns="_", errors="ignore") \
                    .dropna() \
                    .astype({"bezirk": "int"}, errors="ignore") \
                    .astype({"prognoseraum": "int"}, errors="ignore") \
                    .astype({"bezirksregion": "int"}, errors="ignore") \
                    .astype({"planungsraum": "int"}, errors="ignore") \
                    .to_csv(file_path_csv, index=False)

                if not quiet:
                    logger.log_line(f"✓ Convert {file_path_csv}")
    except Exception as e:
        logger.log_line(f"✗️ Exception: {str(e)}")
        return None


#
# Main
#

class LorStatisticsPopulationDataLoader:

    @TrackingDecorator.track_time
    def run(self, logger, results_path, clean=False, quiet=False):
        # Make results path
        os.makedirs(os.path.join(results_path), exist_ok=True)

        base_url = "https://download.statistik-berlin-brandenburg.de/"

        for url, file_name in [
            # Einwohner und Einwohnerinnen in Berlin 1. Halbjahr 2022 (18.08.2022)
            (base_url + "e34ccbeade16c925/363cca7059d1", "SB_A01-16-00_2022h01_BE.xlsx"),
            (base_url + "b98816aeafe8d66b/cd67f203f7aa", "SB_A01-16-00_2022h01_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (04.02.2022)
            (base_url + "1d463bd3704c3925/631339d32c47", "SB_A01-16-00_2021h02_BE.xlsx"),
            (base_url + "0b2599a8ffe0c4eb/3cd53b6cff0c", "SB_A01-16-00_2021h02_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (22.09.2021)
            (base_url + "5b32adf9484d9541/d8123c9bb163", "SB_A01-16-00_2021h01_BE.xlsx"),
            (base_url + "143a871db3f8d5f1/34c69605e6b5", "SB_A01-16-00_2021h01_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (10.02.2021)
            (base_url + "ebfcd0da83f4fef4/474f2236e32a", "SB_A01-16-00_2020h02_BE.xlsx"),
            (base_url + "ffbcda9dc9cd780d/e7ab2379e8c3", "SB_A01-16-00_2020h02_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (26.08.2020)
            (base_url + "ebfcd0da83f4fef4/474f2236e32a", "SB_A01-16-00_2020h01_BE.xlsx"),
            (base_url + "c40ce943d9199e8c/5eaf633ec64b", "SB_A01-16-00_2020h01_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (07.04.2020)
            (base_url + "d608a907aa6e2840/21cc00c944e8", "SB_A01-16-00_2019h02_BE.xlsx"),
            (base_url + "fd512b92e6a56224/3ab042d3d13b", "SB_A01-16-00_2019h02_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (28.08.2019)
            (base_url + "b2d2041bc9db2321/3e296562b672", "SB_A01-16-00_2019h01_BE.xlsx"),
            (base_url + "2bc001d6810e92f4/870b7848bbb6", "SB_A01-16-00_2019h01_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (21.02.2019)
            (base_url + "dd88b0a7bf250690/228278f7a800", "SB_A01-16-00_2018h02_BE.xlsx"),
            (base_url + "d2894d4f00ffed9d/54cd64517b75", "SB_A01-16-00_2018h02_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (02.10.2018)
            (base_url + "cf430bdc7c4e8c0c/99b5f5848495", "SB_A01-16-00_2018h01_BE.xlsx"),
            (base_url + "6020d519e304c734/ea670bdc804b", "SB_A01-16-00_2018h01_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (27.02.2018)
            (base_url + "48f125ea299ead12/11beeeae68ae", "SB_A01-16-00_2017h02_BE.xlsx"),
            (base_url + "08f46a1d62884c50/9b446039e6ee", "SB_A01-16-00_2017h02_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (24.08.2017)
            (base_url + "41d7775d7bc68d18/497860ed6161", "SB_A01-16-00_2017h01_BE.xlsx"),
            (base_url + "4d776df55cfd540d/a9ce53f45c3a", "SB_A01-16-00_2017h01_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (09.03.2017)
            (base_url + "979c90dbc301de4d/1efcc43619ce", "SB_A01-16-00_2016h02_BE.xlsx"),
            (base_url + "a24ba77de2ac70b7/6bf1ec2db9b9", "SB_A01-16-00_2016h02_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (08.09.2016)
            (base_url + "421cc6dc52d652b6/3010b7e96ff5", "SB_A01-16-00_2016h01_BE.xlsx"),
            (base_url + "9f1948434bb2acf6/c0f5cec1899d", "SB_A01-16-00_2016h01_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (11.03.2016)
            (base_url + "2802e6ecb8d9fd52/85b4d6d11870", "SB_A01-06-00_2015h02_BE.xlsx"),
            (base_url + "32d80ce301045fcb/15c2f03d6ac8", "SB_A01-06-00_2015h02_BE.pdf"),
            # Einwohnerinnen und Einwohner im Land Berlin (01.03.2016)
            (base_url + "6f9755e94509ab97/a56498f8dbdc", "SB_A01-06-00_2015h01_BE.xlsx"),
            (base_url + "39635be33812cbc9/ab5676ed32d8", "SB_A01-06-00_2015h01_BE.pdf"),
        ]:

            # Define file path
            file_path = os.path.join(results_path, file_name)

            # Check if result needs to be generated
            if clean or not os.path.exists(file_path):

                download_file(
                    logger=logger,
                    file_path=file_path,
                    url=f"{url}/{file_name}"
                )

                if not quiet:
                    logger.log_line(f"✓ Download {file_path}")
            else:
                logger.log_line(f"✓ Already exists {file_path}")

            # Convert file to csv
            convert_file_to_csv(
                logger=logger,
                file_path=file_path,
                clean=clean,
                quiet=quiet
            )
