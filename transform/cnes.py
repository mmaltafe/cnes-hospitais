import pandas as pd
import warnings

from sqlalchemy import column

PREFIX_MAIN_TBL = "tbEstabelecimento"
PREFIX_BEDS_TBL = "rlEstabComplementar"
PREFIX_INSURANCE_TBL = "rlEstabAtendPrestConv"
PREFIX_CITY_TBL = "tbMunicipio"
PREFIX_TRANSPLANTE_TBL = "tbServicoEspecializado"
PREFIX_TRANSPLANTE_RL = "rlEstabServClass"
PREFIX_CLASS_TRANSPLANTE = "tbClassificacaoServico"

COD_GENERAL_HOSPITAL = 5
COD_SPECIALIZED_HOSPITAL = 7

COD_INSURANCE_OWN = 3
COD_INSURANCE_THIRD = 4
COD_INSURANCE_PRIVATE = 5
COD_INSURANCE_PUBLIC = 6

COD_TRANSPLANTE = 149


def get_transformed_df(files_dir, version):
    warnings.filterwarnings("ignore")

    file_main = files_dir + "/" + PREFIX_MAIN_TBL + version + ".csv"
    df_main = pd.read_csv(
        file_main,
        sep=";",
        dtype={
            "CO_UNIDADE": str,
            "CO_CNES": str,
            "NU_CNPJ_MANTENEDORA": str,
            "CO_MUNICIPIO_GESTOR": str,
            "CO_CEP": str,
            "NU_TELEFONE": str,
        },
    )
    df_main = df_main.drop(
        df_main[
            (df_main["TP_UNIDADE"] != COD_GENERAL_HOSPITAL)
            & (df_main["TP_UNIDADE"] != COD_SPECIALIZED_HOSPITAL)
        ].index
    )
    df_main = df_main[
        [
            "CO_UNIDADE",
            "CO_CNES",
            "NU_CNPJ_MANTENEDORA",
            "NO_RAZAO_SOCIAL",
            "NO_FANTASIA",
            "CO_MUNICIPIO_GESTOR",
            "CO_CEP",
            "NU_TELEFONE",
            "NO_EMAIL",
        ]
    ]
    df_main = df_main.rename({"CO_MUNICIPIO_GESTOR": "CO_MUNICIPIO"}, axis=1)
    df_main["NO_EMAIL"] = df_main["NO_EMAIL"].str.lower()

    file_city = files_dir + "/" + PREFIX_CITY_TBL + version + ".csv"
    df_city = pd.read_csv(
        file_city,
        sep=";",
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

    file_transplante = files_dir + "/" + PREFIX_TRANSPLANTE_RL + version + ".csv"
    df_transplante = pd.read_csv(file_transplante, sep=";")
    df_transplante = df_transplante[df_transplante["CO_SERVICO"] == COD_TRANSPLANTE]
    df_transplante = df_transplante.rename(
        {"CO_CLASSIFICACAO": "CO_CLASSIFICACAO_SERVICO"}, axis=1
    )

    file_transplante = files_dir + "/" + PREFIX_CLASS_TRANSPLANTE + version + ".csv"
    df_class_transplante = pd.read_csv(file_transplante, sep=";")
    df_class_transplante = df_class_transplante[
        df_class_transplante["CO_SERVICO_ESPECIALIZADO"] == COD_TRANSPLANTE
    ]
    df_transplante = df_transplante.merge(
        df_class_transplante, how="left", on="CO_CLASSIFICACAO_SERVICO"
    )

    df_merge = df_main.merge(df_transplante, how="left", on="CO_UNIDADE")
    df_merge = df_merge.merge(df_city, how="left", on="CO_MUNICIPIO")

    df_merge = df_merge.dropna(subset=["CO_SERVICO"])

    df_merge = df_merge.rename(
        {
            "CO_CNES": "Código CNES",
            "NU_CNPJ_MANTENEDORA": "CNPJ",
            "NO_RAZAO_SOCIAL": "Razão Social",
            "NO_FANTASIA": "Nome Fantasia",
            "CO_CEP": "CEP",
            "NU_TELEFONE": "Telefone",
            "NO_EMAIL": "Email",
            "NO_MUNICIPIO": "Município",
            "CO_SIGLA_ESTADO": "UF",
            "DS_CLASSIFICACAO_SERVICO": "Tipo",
        },
        axis=1,
    )
    df_merge = df_merge[
        [
            "nome_fantasia",
            "razao_social",
            "cnpj",
            "cnes",
            "municipio",
            "uf",
            "cep",
            "telefone",
            "email",
            "tipo",
        ]
    ]
    return df_merge.sort_values(by=["UF"])
