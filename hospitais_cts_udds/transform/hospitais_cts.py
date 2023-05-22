import pandas as pd
import warnings

PREFIX_MAIN_TBL = "tbEstabelecimento"
PREFIX_CITY_TBL = "tbMunicipio"
PREFIX_TRANSPLANTE_RL = "rlEstabServClass"
PREFIX_CLASS_TRANSPLANTE = "tbClassificacaoServico"

COD_GENERAL_HOSPITAL = 5
COD_SPECIALIZED_HOSPITAL = 7

COD_TRANSPLANTE = 149


def get_transformed_df(files_dir, version):
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
    
    file_transplante = files_dir + "/" + PREFIX_TRANSPLANTE_RL + version + ".csv"
    df_transplante = pd.read_csv(file_transplante, encoding='ISO-8859-1', delimiter=';')
    df_transplante = df_transplante[df_transplante["CO_SERVICO"] == COD_TRANSPLANTE]
    df_transplante = df_transplante.rename(
        {"CO_CLASSIFICACAO": "CO_CLASSIFICACAO_SERVICO"}, axis=1
    )

    file_transplante = files_dir + "/" + PREFIX_CLASS_TRANSPLANTE + version + ".csv"
    df_class_transplante = pd.read_csv(file_transplante, encoding='ISO-8859-1', delimiter=';')
    df_class_transplante = df_class_transplante[
        df_class_transplante["CO_SERVICO_ESPECIALIZADO"] == COD_TRANSPLANTE
    ]
    df_transplante = df_transplante.merge(
        df_class_transplante, how="left", on="CO_CLASSIFICACAO_SERVICO"
    )
    
    df_cts = df_main[df_main["CO_UNIDADE"].isin(df_transplante["CO_UNIDADE"])]

    df_main = df_main[
        (df_main["TP_UNIDADE"] == COD_GENERAL_HOSPITAL)
        | (df_main["TP_UNIDADE"] == COD_SPECIALIZED_HOSPITAL)
    ]

    df_merge = pd.concat([df_main, df_cts])
    df_merge = df_merge.merge(df_city, how="left", on="CO_MUNICIPIO")


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