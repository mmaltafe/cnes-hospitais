import pandas as pd
import warnings

PREFIX_ETNIA_TBL = "tbEtnia"

def get_transformed_etnia(files_dir, version):
    file_etnia = files_dir + "/" + PREFIX_ETNIA_TBL + version + ".csv"
    df_etnia = pd.read_csv(
        file_etnia,
        encoding='ISO-8859-1', delimiter=';'
    )

    df_etnia = df_etnia[
        [
            "DS_ETNIA",
        ]
    ]
    
    df_etnia = df_etnia.rename(columns={
        "DS_ETNIA": "nome",
    })

    return df_etnia