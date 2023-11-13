import pandas as pd
import warnings

PREFIX_MAIN_TBL = "tbEstabelecimento"
PREFIX_CITY_TBL = "tbMunicipio"
PREFIX_DIALISE_TBL = "tbDialise"


def get_transformed_udd(files_dir, version):
    warnings.filterwarnings("ignore")

    file_main = files_dir + "/" + PREFIX_MAIN_TBL + version + ".csv"
    df_main = pd.read_csv(
        file_main,
        encoding='ISO-8859-1', delimiter=';',
        dtype={
            "CO_UNIDADE": str,
            "CO_CNES": str,
            "NU_CNPJ_MANTENEDORA": str,
            "CO_MUNICIPIO_GESTOR": str,
            "CO_CEP": str,
            "NU_TELEFONE": str,
        },
    )

    df_main = df_main.rename({"CO_MUNICIPIO_GESTOR": "CO_MUNICIPIO"}, axis=1)
    df_main["NO_EMAIL"] = df_main["NO_EMAIL"].str.lower()

    file_city = files_dir + "/" + PREFIX_CITY_TBL + version + ".csv"
    df_city = pd.read_csv(
        file_city,
        encoding='ISO-8859-1', delimiter=';',
        dtype={
            "CO_MUNICIPIO": str,
        },
    )
    df_city = df_city[
        [
            "CO_MUNICIPIO",
            "NO_MUNICIPIO",
            "CO_SIGLA_ESTADO",
        ]
    ]
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

    file_dialise = files_dir + "/" + PREFIX_DIALISE_TBL + version + ".csv"
    df_dialise = pd.read_csv(
        file_dialise,
        encoding='ISO-8859-1',
        delimiter=';',
        dtype={
            "CO_UNIDADE": str,
        },
    )
    valores_dialise = df_dialise["CO_UNIDADE"].unique()
    df_filtrado = df_main[df_main["CO_UNIDADE"].isin(valores_dialise)]

    df_merge = df_filtrado.merge(df_city, how="left", on="CO_MUNICIPIO")

    df_merge = df_merge.dropna(subset=["NO_EMAIL"])
    df_merge = df_merge.dropna(subset=["NU_TELEFONE"])
    df_merge = df_merge.dropna(subset=["CO_CNES"])

        
    df_merge = df_merge[
        [
            "NO_RAZAO_SOCIAL",
            "NO_FANTASIA",
            "NU_CNPJ_MANTENEDORA",
            "NU_CNPJ",
            "CO_CNES",
            "NO_LOGRADOURO",
            "NO_BAIRRO",
            "NU_ENDERECO",
            "NO_COMPLEMENTO",
            "CO_CEP",
            "NU_TELEFONE",
            "NO_EMAIL",
            "NO_MUNICIPIO",
            "CO_SIGLA_ESTADO",
        ]
    ]

    df_merge = df_merge.rename(
        {
            "NO_RAZAO_SOCIAL": "nome_social",
            "NO_FANTASIA": "nome_fantasia",
            "NU_CNPJ_MANTENEDORA": "cnpj_mantenedora",
            "NU_CNPJ": "cnpj",
            "CO_CNES": "cnes",
            "NO_LOGRADOURO": "logradouro",
            "NO_BAIRRO": "bairro",
            "NU_ENDERECO": "numero",
            "NO_COMPLEMENTO": "complemento",
            "CO_CEP": "cep",
            "NU_TELEFONE": "telefone",
            "NO_EMAIL": "email",
            "NO_MUNICIPIO": "municipio",
            "CO_SIGLA_ESTADO": "uf",
        },
        axis=1,
    )
    df_merge = df_merge.rename(columns={
        "nome_social": "nome_social",
        "nome_fantasia": "nome_fantasia",
        "cnpj_mantenedora": "cnpj_mantenedora",
        "cnpj": "cnpj",
        "cnes": "cnes",
        "logradouro": "logradouro",
        "bairro": "bairro",
        "numero": "numero",
        "complemento": "complemento",
        "cep": "cep",
        "telefone": "telefone",
        "email": "email",
        "municipio": "municipio",
        "uf": "uf",
    })
    return df_merge.sort_values(by=["uf"])