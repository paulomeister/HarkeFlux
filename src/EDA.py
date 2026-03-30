import os
import pandas as pd
import matplotlib.pyplot as plt


def analisis_exploratorio(df):
    print("INICIANDO EDA...\n")

    os.makedirs("data/processed", exist_ok=True)

    print("INFORMACION GENERAL:")
    print(df.info())

    print("\nDIMENSIONES DEL DATASET:")
    print(df.shape)

    print("\nCOLUMNAS DISPONIBLES:")
    print(df.columns.tolist())

    # ESTADISTICAS DESCRIPTIVAS
    print("\nESTADISTICAS NUMERICAS:")
    print(df.describe(include=["number"]))

    print("\nESTADISTICAS CATEGORICAS:")
    stats_categoricas = df.describe(include=["object", "string"])
    print(stats_categoricas)

    # VALORES NULOS
    print("\nVALORES NULOS POR COLUMNA:")
    nulos = df.isnull().sum().sort_values(ascending=False)
    print(nulos)

    # DUPLICADOS
    duplicados = df.duplicated().sum()
    print(f"\nFILAS DUPLICADAS: {duplicados}")

    # DATOS INCOMPLETOS
    valores_incompletos = ["SIN DATO", "NO APLICA"]
    incompletos = df.isin(valores_incompletos).sum()

    print("\nCAMPOS CON DATOS INCOMPLETOS:")
    print(incompletos.sort_values(ascending=False))

    # ANALISIS DE UBICACIONES
    if "DEPARTAMENTO_HECHO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        resumen_departamentos = (
            df.groupby("DEPARTAMENTO_HECHO", dropna=False)["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
        )

        print("\nTOP 10 DEPARTAMENTOS CON MAS CASOS:")
        print(resumen_departamentos.head(10))

    if "MUNICIPIO_HECHO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        resumen_municipios = (
            df.groupby("MUNICIPIO_HECHO", dropna=False)["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
        )

        print("\nTOP 10 MUNICIPIOS CON MAS CASOS:")
        print(resumen_municipios.head(10))

    if "SECCIONAL" in df.columns and "TOTAL_PROCESOS" in df.columns:
        resumen_seccional = (
            df.groupby("SECCIONAL", dropna=False)["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
        )

        print("\nTOP 10 SECCIONALES CON MAS CASOS:")
        print(resumen_seccional.head(10))

    # ANALISIS AVANZADO
    # 1. PORCENTAJE POR TIPO DE DELITO
    if "DELITO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        total_general = df["TOTAL_PROCESOS"].sum()

        porcentaje_delito = (
            df.groupby("DELITO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        porcentaje_delito["PORCENTAJE"] = (
            porcentaje_delito["TOTAL_PROCESOS"] / total_general * 100
        ).round(2)

        print("\nPORCENTAJE DE DELITOS POR TIPO:")
        print(porcentaje_delito.head(10))

    # 2. CONCENTRACION GEOGRAFICA POR DEPARTAMENTO
    if "DEPARTAMENTO_HECHO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        total_general = df["TOTAL_PROCESOS"].sum()

        concentracion_departamento = (
            df.groupby("DEPARTAMENTO_HECHO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        concentracion_departamento["PORCENTAJE"] = (
            concentracion_departamento["TOTAL_PROCESOS"] / total_general * 100
        ).round(2)

        print("\nCONCENTRACION GEOGRAFICA POR DEPARTAMENTO:")
        print(concentracion_departamento.head(10))

    # 3. CRECIMIENTO ANUAL TOTAL
    if "AÑO_HECHOS" in df.columns and "TOTAL_PROCESOS" in df.columns:
        crecimiento_anual = (
            df.groupby("AÑO_HECHOS")["TOTAL_PROCESOS"]
            .sum()
            .sort_index()
            .reset_index()
        )
        crecimiento_anual["CRECIMIENTO_PORCENTUAL"] = (
            crecimiento_anual["TOTAL_PROCESOS"].pct_change() * 100
        ).round(2)

        print("\nCRECIMIENTO ANUAL TOTAL:")
        print(crecimiento_anual)

    # 4. CRECIMIENTO POR DEPARTAMENTO
    if "AÑO_HECHOS" in df.columns and "DEPARTAMENTO_HECHO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        crecimiento_departamento = (
            df.groupby(["DEPARTAMENTO_HECHO", "AÑO_HECHOS"])["TOTAL_PROCESOS"]
            .sum()
            .reset_index()
            .sort_values(["DEPARTAMENTO_HECHO", "AÑO_HECHOS"])
        )

        crecimiento_departamento["CRECIMIENTO_PORCENTUAL"] = (
            crecimiento_departamento.groupby("DEPARTAMENTO_HECHO")["TOTAL_PROCESOS"]
            .pct_change() * 100
        ).round(2)

        top_crecimiento = (
            crecimiento_departamento[crecimiento_departamento["AÑO_HECHOS"].between(2023, 2025)]
            .groupby("DEPARTAMENTO_HECHO")["CRECIMIENTO_PORCENTUAL"]
            .mean()
            .sort_values(ascending=False)
            .head(10)
        )

        print("\nTOP 10 DEPARTAMENTOS CON MAYOR CRECIMIENTO PROMEDIO (2023-2025):")
        print(top_crecimiento)

    # 5. DIFERENCIA ENTRE AÑO_HECHOS Y AÑO_DENUNCIA
    if "AÑO_HECHOS" in df.columns and "AÑO_DENUNCIA" in df.columns:
        denuncia_aux = pd.to_numeric(df["AÑO_DENUNCIA"], errors="coerce")
        diferencia_denuncia = denuncia_aux - df["AÑO_HECHOS"]

        resumen_denuncia = pd.DataFrame({
            "promedio_diferencia": [round(diferencia_denuncia.mean(), 2)],
            "mediana_diferencia": [round(diferencia_denuncia.median(), 2)],
            "max_diferencia": [diferencia_denuncia.max()],
            "min_diferencia": [diferencia_denuncia.min()]
        })

        print("\nRESUMEN DIFERENCIA ENTRE AÑO_HECHOS Y AÑO_DENUNCIA:")
        print(resumen_denuncia)

    # 6. CASOS ACTIVOS VS INACTIVOS
    if "ESTADO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        estado_resumen = (
            df.groupby("ESTADO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        total_estados = estado_resumen["TOTAL_PROCESOS"].sum()
        estado_resumen["PORCENTAJE"] = (
            estado_resumen["TOTAL_PROCESOS"] / total_estados * 100
        ).round(2)

        print("\nCASOS POR ESTADO:")
        print(estado_resumen)

    # 7. CASOS POR ETAPA
    if "ETAPA_CASO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        etapa_resumen = (
            df.groupby("ETAPA_CASO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        total_etapas = etapa_resumen["TOTAL_PROCESOS"].sum()
        etapa_resumen["PORCENTAJE"] = (
            etapa_resumen["TOTAL_PROCESOS"] / total_etapas * 100
        ).round(2)

        print("\nCASOS POR ETAPA:")
        print(etapa_resumen)

    # 8. CRUCE DELITO VS DEPARTAMENTO
    if "DELITO" in df.columns and "DEPARTAMENTO_HECHO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        cruce_delito_departamento = pd.pivot_table(
            df,
            values="TOTAL_PROCESOS",
            index="DEPARTAMENTO_HECHO",
            columns="DELITO",
            aggfunc="sum",
            fill_value=0
        )

        print("\nCRUCE DELITO VS DEPARTAMENTO:")
        print(cruce_delito_departamento.head())

    # 9. CRUCE SECCIONAL VS ESTADO
    if "SECCIONAL" in df.columns and "ESTADO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        cruce_seccional_estado = pd.pivot_table(
            df,
            values="TOTAL_PROCESOS",
            index="SECCIONAL",
            columns="ESTADO",
            aggfunc="sum",
            fill_value=0
        )

        print("\nCRUCE SECCIONAL VS ESTADO:")
        print(cruce_seccional_estado.head())

    # 10. CAMPOS INCOMPLETOS POR DEPARTAMENTO
    if "DEPARTAMENTO_HECHO" in df.columns:
        df_incompletos = df.copy()

        columnas_a_evaluar = [col for col in df.columns if col != "DEPARTAMENTO_HECHO"]

        for col in columnas_a_evaluar:
            df_incompletos[col] = df_incompletos[col].isin(valores_incompletos).astype(int)

        incompletos_departamento = df_incompletos.groupby("DEPARTAMENTO_HECHO")[columnas_a_evaluar].sum()
        incompletos_departamento["TOTAL_INCOMPLETOS"] = incompletos_departamento.sum(axis=1)
        incompletos_departamento = incompletos_departamento.sort_values(
            by="TOTAL_INCOMPLETOS", ascending=False
        )

        print("\nDEPARTAMENTOS CON MAYOR CANTIDAD DE DATOS INCOMPLETOS:")
        print(incompletos_departamento[["TOTAL_INCOMPLETOS"]].head(10))

    # GRAFICAS
    # GRAFICA 1: EVOLUCION EN EL TIEMPO
    if "AÑO_HECHOS" in df.columns and "TOTAL_PROCESOS" in df.columns:
        datos_tiempo = (
            df[df["AÑO_HECHOS"].notna()]
            .groupby("AÑO_HECHOS")["TOTAL_PROCESOS"]
            .sum()
            .sort_index()
        )

        plt.figure(figsize=(10, 6))
        plt.plot(datos_tiempo.index, datos_tiempo.values, marker="o")

        for x, y in zip(datos_tiempo.index, datos_tiempo.values):
            plt.text(x, y, str(int(y)), ha="center", va="bottom")

        plt.xlabel("Año")
        plt.ylabel("Total procesos")
        plt.title("Evolucion de delitos por año")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig("data/processed/tendencia_tiempo.png")
        plt.close()

    # GRAFICA 2: TOP 10 DEPARTAMENTOS
    if "DEPARTAMENTO_HECHO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        top_deptos = (
            df.groupby("DEPARTAMENTO_HECHO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )

        plt.figure(figsize=(12, 6))
        ax = top_deptos.plot(kind="bar")

        for i, v in enumerate(top_deptos.values):
            ax.text(i, v, str(int(v)), ha="center", va="bottom")

        plt.title("Top 10 departamentos con mas delitos")
        plt.xlabel("Departamento")
        plt.ylabel("Total procesos")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig("data/processed/top_departamentos.png")
        plt.close()

    # GRAFICA 3: TOP 10 MUNICIPIOS
    if "MUNICIPIO_HECHO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        top_municipios = (
            df.groupby("MUNICIPIO_HECHO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )

        plt.figure(figsize=(12, 6))
        ax = top_municipios.plot(kind="bar")

        for i, v in enumerate(top_municipios.values):
            ax.text(i, v, str(int(v)), ha="center", va="bottom")

        plt.title("Top 10 municipios con mas delitos")
        plt.xlabel("Municipio")
        plt.ylabel("Total procesos")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig("data/processed/top_municipios.png")
        plt.close()

    # GRAFICA 4: TOP 10 DELITOS CON CODIGO Y TABLA
    if "DELITO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        top_delitos_df = (
            df.groupby("DELITO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )

        top_delitos_df["CODIGO"] = [f"D{i+1}" for i in range(len(top_delitos_df))]

        print("\nTOP 10 DELITOS INFORMATICOS:")
        print(top_delitos_df[["CODIGO", "DELITO", "TOTAL_PROCESOS"]])

        fig, (ax1, ax2) = plt.subplots(
            1, 2, figsize=(18, 8), gridspec_kw={"width_ratios": [1.2, 2.8]}
        )

        # Grafica
        ax1.barh(top_delitos_df["CODIGO"], top_delitos_df["TOTAL_PROCESOS"])
        ax1.invert_yaxis()
        ax1.set_title("Top 10 delitos informaticos")
        ax1.set_xlabel("Total procesos")
        ax1.set_ylabel("Codigo")

        for i, v in enumerate(top_delitos_df["TOTAL_PROCESOS"]):
            ax1.text(v, i, str(int(v)), va="center")

        # Tabla
        ax2.axis("off")
        tabla_texto = ax2.table(
            cellText=top_delitos_df[["CODIGO", "DELITO"]].values,
            colLabels=["Codigo", "Delito"],
            cellLoc="left",
            loc="center"
        )

        tabla_texto.auto_set_font_size(False)
        tabla_texto.set_fontsize(9)
        tabla_texto.scale(1, 2)

        plt.tight_layout()
        plt.savefig("data/processed/top_delitos.png")
        plt.close()

    # GRAFICA 5: TOP 10 SECCIONALES
    if "SECCIONAL" in df.columns and "TOTAL_PROCESOS" in df.columns:
        top_seccionales = (
            df.groupby("SECCIONAL")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )

        plt.figure(figsize=(12, 6))
        ax = top_seccionales.plot(kind="bar")

        for i, v in enumerate(top_seccionales.values):
            ax.text(i, v, str(int(v)), ha="center", va="bottom")

        plt.title("Top 10 seccionales con mas casos")
        plt.xlabel("Seccional")
        plt.ylabel("Total procesos")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig("data/processed/top_seccionales.png")
        plt.close()

    # GRAFICA 6: ETAPA DEL CASO
    if "ETAPA_CASO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        etapas = (
            df.groupby("ETAPA_CASO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
        )

        plt.figure(figsize=(10, 6))
        ax = etapas.plot(kind="bar")

        for i, v in enumerate(etapas.values):
            ax.text(i, v, str(int(v)), ha="center", va="bottom")

        plt.title("Total de procesos por etapa del caso")
        plt.xlabel("Etapa del caso")
        plt.ylabel("Total procesos")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig("data/processed/etapa_caso.png")
        plt.close()

    # GRAFICA 7: ESTADO DEL PROCESO
    if "ESTADO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        estados = (
            df.groupby("ESTADO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
        )

        plt.figure(figsize=(10, 6))
        ax = estados.plot(kind="bar")

        for i, v in enumerate(estados.values):
            ax.text(i, v, str(int(v)), ha="center", va="bottom")

        plt.title("Total de procesos por estado")
        plt.xlabel("Estado")
        plt.ylabel("Total procesos")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig("data/processed/estado_proceso.png")
        plt.close()

    # GRAFICA 8: CRECIMIENTO ANUAL
    if "AÑO_HECHOS" in df.columns and "TOTAL_PROCESOS" in df.columns:
        crecimiento_anual_grafica = (
            df.groupby("AÑO_HECHOS")["TOTAL_PROCESOS"]
            .sum()
            .sort_index()
        )

        plt.figure(figsize=(10, 6))
        ax = crecimiento_anual_grafica.plot(kind="bar")

        for i, v in enumerate(crecimiento_anual_grafica.values):
            ax.text(i, v, str(int(v)), ha="center", va="bottom")

        plt.title("Total de procesos por año")
        plt.xlabel("Año")
        plt.ylabel("Total procesos")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("data/processed/total_por_anio.png")
        plt.close()

    # GRAFICA 9: PORCENTAJE TOP 10 DELITOS
    if "DELITO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        total_general = df["TOTAL_PROCESOS"].sum()
        top_delitos_pct = (
            df.groupby("DELITO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        top_delitos_pct = (top_delitos_pct / total_general * 100).round(2)

        plt.figure(figsize=(14, 8))
        ax = top_delitos_pct.plot(kind="barh")

        for i, v in enumerate(top_delitos_pct.values):
            ax.text(v, i, f"{v:.2f}%", va="center")

        plt.title("Porcentaje de participacion - Top 10 delitos")
        plt.xlabel("Porcentaje")
        plt.ylabel("Delito")
        plt.tight_layout()
        plt.savefig("data/processed/porcentaje_top_delitos.png")
        plt.close()

    # GRAFICA 10: CONCENTRACION TOP 10 DEPARTAMENTOS
    if "DEPARTAMENTO_HECHO" in df.columns and "TOTAL_PROCESOS" in df.columns:
        total_general = df["TOTAL_PROCESOS"].sum()
        top_deptos_pct = (
            df.groupby("DEPARTAMENTO_HECHO")["TOTAL_PROCESOS"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        top_deptos_pct = (top_deptos_pct / total_general * 100).round(2)

        plt.figure(figsize=(12, 6))
        ax = top_deptos_pct.plot(kind="bar")

        for i, v in enumerate(top_deptos_pct.values):
            ax.text(i, v, f"{v:.2f}%", ha="center", va="bottom")

        plt.title("Concentracion geografica - Top 10 departamentos")
        plt.xlabel("Departamento")
        plt.ylabel("Porcentaje")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig("data/processed/concentracion_top_departamentos.png")
        plt.close()

    print("\nEDA FINALIZADO - Archivos guardados en data/processed/")


if __name__ == "__main__":
    ruta_archivo = "data/raw/Delitos_Informaticos_V1_20260328.csv"
    df = pd.read_csv(ruta_archivo)
    analisis_exploratorio(df)