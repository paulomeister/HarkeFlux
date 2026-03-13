# PROYECTO DELITOS INFORMATICOS

Perfecto, convertí tu documento a **formato Markdown (.md) listo para GitHub** manteniendo una estructura limpia con encabezados, listas y tabla.
El contenido proviene de tu archivo: 

Puedes copiarlo directamente como `README.md` o como archivo `.md` en tu repositorio.

---

# Caracterización de la Fuente de Datos

## 1. Origen de los datos

La fuente de datos utilizada proviene del portal oficial de datos abiertos del gobierno colombiano:

**Datos Abiertos Colombia**

El conjunto de datos corresponde al dataset **“Delitos Informáticos V1”**, disponible en:

[https://www.datos.gov.co/Justicia-y-Derecho/Delitos-Informaticos-V1/wxd8-ucns/about_data](https://www.datos.gov.co/Justicia-y-Derecho/Delitos-Informaticos-V1/wxd8-ucns/about_data)

Este portal recopila información pública generada por entidades gubernamentales con el objetivo de promover:

* la transparencia
* el acceso a la información
* el uso de datos para investigación
* el análisis estadístico
* el desarrollo de políticas públicas

Los datos hacen referencia a registros de **delitos informáticos reportados dentro del sistema judicial colombiano**, lo que permite analizar tendencias de **cibercriminalidad en el país**.

---

# 2. Tipo de fuente de datos

La fuente corresponde a un **dataset estructurado publicado en formato abierto**.

### Características principales

* **Formato del archivo:** CSV (Comma Separated Values)
* **Tipo de datos:** estructurados
* **Modelo de datos:** tabular
* **Acceso:** público y abierto
* **Origen institucional:** sector justicia del gobierno colombiano

En este tipo de archivos:

* Cada **fila** representa un **registro**
* Cada **columna** representa una **variable**

---

# 3. Tamaño del dataset

El archivo cargado contiene aproximadamente:

* **63,705 registros (filas)**
* **17 variables (columnas)**

---

# 4. Estructura de las variables

El dataset contiene **17 variables** que describen características relacionadas con los procesos o registros de delitos informáticos.

| Variable           | Tipo       | Descripción aproximada                       |
| ------------------ | ---------- | -------------------------------------------- |
| CRIMINALIDAD       | Categórica | Indica si el caso corresponde a criminalidad |
| ES_ARCHIVO         | Categórica | Indica si el caso fue archivado              |
| ES_PRECLUSIÓN      | Categórica | Indica si el proceso fue precluido           |
| ESTADO             | Categórica | Estado del proceso                           |
| ETAPA_CASO         | Categórica | Etapa procesal del caso                      |
| LEY                | Categórica | Ley asociada al delito                       |
| PAÍS_HECHO         | Categórica | País donde ocurrió el hecho                  |
| DEPARTAMENTO_HECHO | Categórica | Departamento del evento                      |
| MUNICIPIO_HECHO    | Categórica | Municipio del evento                         |
| SECCIONAL          | Categórica | Seccional de la Fiscalía                     |
| AÑO_HECHOS         | Numérica   | Año en que ocurrió el hecho                  |
| AÑO_ENTRADA        | Numérica   | Año de ingreso del proceso                   |
| AÑO_DENUNCIA       | Categórica | Año de denuncia                              |
| DELITO             | Categórica | Tipo específico de delito informático        |
| GRUPO_DELITO       | Categórica | Categoría general del delito                 |
| CONSUMADO          | Categórica | Indica si el delito fue consumado            |
| TOTAL_PROCESOS     | Numérica   | Número total de procesos registrados         |

---

# 5. Tipos de datos

En el dataset se identifican principalmente **dos tipos de datos**.

## Variables categóricas (tipo texto)

Ejemplos:

* DELITO
* MUNICIPIO_HECHO
* DEPARTAMENTO_HECHO
* ESTADO
* GRUPO_DELITO

Estas variables se utilizan para **clasificación o agrupación de los registros**.

## Variables numéricas

* AÑO_HECHOS
* AÑO_ENTRADA
* TOTAL_PROCESOS

Estas permiten realizar **análisis cuantitativos y temporales**.

---

# 6. Dominio del dataset

El dataset pertenece al dominio de **seguridad digital y justicia penal**, y está relacionado con el análisis de:

* cibercrimen
* delitos informáticos
* fraude digital
* acceso ilegal a sistemas
* uso indebido de tecnologías

---

# 7. Posibles problemas de calidad de datos

Algunos posibles problemas de calidad que pueden presentarse en el dataset incluyen:

* valores faltantes
* inconsistencias en nombres de municipios
* registros categóricos con múltiples variantes
* datos almacenados como texto cuando deberían ser numéricos (por ejemplo **AÑO_DENUNCIA**)

---

# 8. Uso potencial del dataset

Este conjunto de datos puede ser utilizado para diferentes análisis, como:

* análisis estadístico de delitos informáticos
* análisis geográfico de cibercrimen
* tendencias temporales de delitos
* análisis de políticas de seguridad digital
* desarrollo de modelos predictivos de criminalidad digital



