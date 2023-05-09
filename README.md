# NDBI046 Introduction to Data Engeneering

## Github Pages
- Site is live at https://vaclavstibor.github.io/NDBI046/

## System requirements
- Operační systém: Windows, macOS, Linux nebo jiný operační systém s podporou pro Python
- Verze Pythonu: Python 3.8 a novější (testováno na 3.8.6 64-bit)
- Docker (alespoň 4 GB volného paměti)

## Installation instructions
- Nakloujte tento repozitář
- V adresáři `./airflow` spusťte `docker compose up --build`
- Po inicializaci kontejneru webserver naleznete na http://localhost:8080, kde přihlašoací údaje jsou defaultně nastavené na 
    - login: `airflow`
    - password: `airflow`
- Pro ukončení a odstranění kontejneru spusťte `docker compose down --volumes --rmi all`

## Description of script files
### care-providers.py
- Skript po spuštění automaticky načte potřebná data umístěná v adresáři `./airflow/data`
#### Description
- Datová kostka
    - Dimension: county (okres)
    - Dimension: region (kraj)
    - Dimension: field of care (obor péče)
    - Measure: number of care providers per county (počet poskytovatelů péče)
#### Output
- Výstupem bude soubor formátu turtle popisující datovou kostku
- `./airflow/ata/data-cubes/population-2021.ttl`

### population-2021.py
- Skript po spuštění automaticky načte potřebná data umístěná v adresáři `./airflow/data`
#### Description
- Datová kostka
    - Dimension: county (okres)
    - Dimension: region (kraj)
    - Measure: mean population per county (střední stav obyvatel)
#### Output
- Výstupem bude soubor formátu turtle popisující datovou kostku
- `./airflow/data/data-cubes/population-2021.ttl`

### datacube.py
- Obsahuje všechny potřebné implementace funkcí pro konstrukci datové kostky, jenž využíváme v `care-providers.py` a `population-2021.py`