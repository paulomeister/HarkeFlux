import pandas as pd

from transform import transform_data


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "DELITO": [
                "ACCESO ABUSIVO A UN SISTEMA INFORMATICO AGRAVADO",
                "HURTO POR MEDIOS INFORMATICOS",
            ],
            "AÑO_DENUNCIA": ["2020", "SIN DATO"],
            "AÑO_ENTRADA": ["2021", "2022"],
            "AÑO_HECHOS": ["2019", "2020"],
            "TOTAL_PROCESOS": ["10", "5"],
            "PAÍS_HECHO": ["COLOMBIA", "SIN DATO"],
            "MUNICIPIO_HECHO": ["CALI", "SIN DATO"],
            "SECCIONAL": ["CALI", "SIN DATO"],
            "DEPARTAMENTO_HECHO": ["VALLE", "SIN DATO"],
            "LEY": ["599", "SIN DATO"],
            "GRUPO_DELITO": ["DELITOS INFORMATICOS", "DELITOS INFORMATICOS"],
            "CONSUMADO": ["NO APLICA", "NO APLICA"],
        }
    )


def test_transform_base_and_business_rules() -> None:
    transformed = transform_data(_sample_df())

    assert "GRUPO_DELITO" not in transformed.columns
    assert "CONSUMADO" not in transformed.columns

    required_cols = {
        "ANO_DENUNCIA",
        "ANO_ENTRADA",
        "ANO_HECHOS",
        "TOTAL_PROCESOS",
        "ANO_DENUNCIA_NUM",
        "ANO_ENTRADA_NUM",
        "ANO_HECHOS_NUM",
        "TOTAL_PROCESOS_NUM",
        "ES_AGRAVADO",
        "TIEMPO_DENUNCIA",
        "ES_INCOMPLETO",
        "PAIS_HECHO",
    }
    assert required_cols.issubset(set(transformed.columns))

    assert transformed.loc[0, "ES_AGRAVADO"] == "SI"
    assert transformed.loc[1, "ES_AGRAVADO"] == "NO"

    assert transformed.loc[0, "TIEMPO_DENUNCIA"] == 1
    assert pd.isna(transformed.loc[1, "TIEMPO_DENUNCIA"])

    assert transformed.loc[0, "ES_INCOMPLETO"] == "NO"
    assert transformed.loc[1, "ES_INCOMPLETO"] == "SI"


def test_parallel_numeric_columns_keep_sin_dato_in_original() -> None:
    transformed = transform_data(_sample_df())

    assert transformed.loc[1, "ANO_DENUNCIA"] == "SIN DATO"
    assert pd.isna(transformed.loc[1, "ANO_DENUNCIA_NUM"])
    assert str(transformed["ANO_DENUNCIA_NUM"].dtype) == "Int64"

