# NDBI046 Introduction to Data Engeneering
## Apache Airflow
- Airflow is written in python, so python needs to be installed in the environment, and python must be greater than 2.7, 3.x is recommended.

## System requirements
- Operační systém: Windows, macOS, Linux nebo jiný operační systém s podporou pro Python
- Verze Pythonu: Python 3.8 a novější (testováno na 3.8.6 64-bit)
- Procesor: Intel nebo AMD procesor s 64bitovou architekturou
- Paměť RAM: minimálně 4 GB
- Pevný disk: minimálně 5 GB volného místa (především pro instalaci Pythonu a jeho knihoven)
- Grafická karta: nevyžaduje se speciální grafická karta (pro běh Pythonu)
- Připojení k internetu: vyžaduje se pro stahování a aktualizaci knihoven a balíčků pro Python, stejně jako pro data (nicméně jsou obshame i tohoto repozitáře)

## Installation instructions
- Stáhněte a nainstailujte nejnovější verzi Pythonu (idálně 3.8.6) https://www.python.org/downloads/
- Nakolujte tento GitHub repozitář
- Tento repozitář dále obsahuje i data, se kterými chceme pracovat, nicméně také obsahuje skript pro případné stažení z internetu
- Pro instalaci knihoven spusťte následující `pip` (instalace nástroje `pip python -m ensurepip --default-pip`)
)příkaz v příkazové řádce tohto repozitáře
    - `pip install -r requirements.txt`
- Pro vytvoření datové kostky **Care Providers** spusťte `python care-providers.py`
- Pro vytvoření datové kostky **Population 2021** spusťte `python population-2021.py`

## Description of script files
### care-providers.py
#### Input
- Skript po spuštění automaticky načte potřebná data umístěná v adresáři `./data`
#### Description
- Datová kostka
    - Dimension: county (okres)
    - Dimension: region (kraj)
    - Dimension: field of care (obor péče)
    - Measure: number of care providers per county (počet poskytovatelů péče)
#### Output
- Výstupem bude soubor formátu turtle popisující datovou kostku
- `./data/data-cubes/population-2021.ttl`

### population-2021.py
#### Input
- Skript po spuštění automaticky načte potřebná data umístěná v adresáři `./data`
#### Description
- Datová kostka
    - Dimension: county (okres)
    - Dimension: region (kraj)
    - Measure: mean population per county (střední stav obyvatel)
#### Output
- Výstupem bude soubor formátu turtle popisující datovou kostku
- `./data/data-cubes/population-2021.ttl`

### datacube.py
- Obsahuje všechny potřebné implementace funkcí pro konstrukci datové kostky, jenž využíváme v `care-providers.py` a `population-2021.py`