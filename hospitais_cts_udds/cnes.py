import os
import shutil

from extract.ftp import download_latest_cnes_dataset
from transform.hospitais_cts import get_transformed_df
from transform.municipios import get_transformed_city
from transform.etnias import get_transformed_etnia
from transform.unidades_de_dialise import get_transformed_udd
from utils.logger import Logger
from utils.unzip import unzip

EXTRACTION_DIR = "extracted"
TEMP_DIR = "temp/"


def main():
    logger = Logger()
    err = run(logger)
    if err:
        logger.info("Terminated due to error")
        logger.error(err)
        return

    logger.info("Finished without errors")


def run(logger):
    try:
        os.mkdir(TEMP_DIR)

        logger.info("Downloading latest archived CNES dataset from FTP server...")
        cnes_zip_file, version = download_latest_cnes_dataset(TEMP_DIR)

        logger.info(
            "Extracting archived CNES dataset to {}...".format(
                TEMP_DIR + EXTRACTION_DIR
            )
        )
        unzip(cnes_zip_file, TEMP_DIR + EXTRACTION_DIR)
        version = "202304" # parece que muda todo mês

        logger.info("Applying transformations...")
        PARAM = TEMP_DIR + EXTRACTION_DIR
        df = get_transformed_df(PARAM, version)
        df_city = get_transformed_city(PARAM, version)
        df_etnia = get_transformed_etnia(PARAM, version)
        df_udd = get_transformed_udd(PARAM, version)

        logger.info("Generating {}.csv...".format("etnias"))
        df_etnia.to_csv("etnias" + ".csv", index=False)

        logger.info("Generating {}.csv...".format("municipios"))
        df_city.to_csv("municipios" + ".csv", index=False)

        logger.info("Generating {}.csv...".format("hospitais"))
        df.to_csv("hospitais" + ".csv", index=False)

        logger.info("Generating {}.csv...".format("unidades de dialise"))
        df_udd.to_csv("udd" + ".csv", index=False)

        logger.info("Cleaning temp files and directories...")
        # shutil.rmtree(TEMP_DIR)
    except Exception as e:
        return e


main()
