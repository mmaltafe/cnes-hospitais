import pandas as pd
import warnings

PREFIX_CITY_TBL = "tbMunicipio"

def get_transformed_city(files_dir, version):
    file_city = files_dir + "/" + PREFIX_CITY_TBL + version + ".csv"
    df_city = pd.read_csv(
        file_city,
        encoding='ISO-8859-1', delimiter=';'
    )

    df_city = (
        df_city.groupby(by="CO_MUNICIPIO")
        .agg(
            {
                "NO_MUNICIPIO": "last",
                "CO_SIGLA_ESTADO": "last",
            }
        )
        .reset_index()
    )

    df_city = df_city[
        [
            "NO_MUNICIPIO",
            "CO_SIGLA_ESTADO",
        ]
    ]
    
    df_city = df_city.rename(columns={
        "NO_MUNICIPIO": "nome",
        "CO_SIGLA_ESTADO": "sigla_uf",
    })

    return df_city.sort_values(by=["sigla_uf"])