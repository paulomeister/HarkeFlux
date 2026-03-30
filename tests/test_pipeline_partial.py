from pathlib import Path

import pandas as pd

import pipeline


def _raw_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "CRIMINALIDAD": ["SI", "NO"],
            "ES_ARCHIVO": ["NO", "SI"],
            "ES_PRECLUSIÓN": ["NO", "NO"],
            "ESTADO": ["ACTIVO", "ACTIVO"],
            "ETAPA_CASO": ["INVESTIGACION", "INVESTIGACION"],
            "LEY": ["599", "SIN DATO"],
            "PAÍS_HECHO": ["COLOMBIA", "SIN DATO"],
            "DEPARTAMENTO_HECHO": ["VALLE", "SIN DATO"],
            "MUNICIPIO_HECHO": ["CALI", "SIN DATO"],
            "SECCIONAL": ["CALI", "SIN DATO"],
            "AÑO_HECHOS": ["2019", "2020"],
            "AÑO_ENTRADA": ["2021", "2022"],
            "AÑO_DENUNCIA": ["2020", "SIN DATO"],
            "DELITO": ["DELITO AGRAVADO", "DELITO BASE"],
            "GRUPO_DELITO": ["DELITOS INFORMATICOS", "DELITOS INFORMATICOS"],
            "CONSUMADO": ["NO APLICA", "NO APLICA"],
            "TOTAL_PROCESOS": ["10", "5"],
        }
    )


def test_pipeline_partial_run_without_real_db(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "raw.csv"
    output_path = tmp_path / "processed.csv"
    _raw_df().to_csv(source_path, index=False)

    captured = {}

    def fake_load_data(df, table_name, if_exists, schema=None, chunk_size=2000, dotenv_path=None):
        captured["rows"] = len(df)
        captured["table_name"] = table_name
        captured["if_exists"] = if_exists
        return len(df)

    monkeypatch.setattr(pipeline, "load_data", fake_load_data)

    pipeline.main(
        source_path=Path(source_path),
        processed_output_path=Path(output_path),
        table_name="delitos_processed",
        if_exists="replace",
    )

    assert output_path.exists()
    processed = pd.read_csv(output_path)
    assert len(processed) == 2
    assert captured["rows"] == 2
    assert captured["table_name"] == "delitos_processed"
    assert captured["if_exists"] == "replace"

