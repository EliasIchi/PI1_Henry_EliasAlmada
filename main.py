from fastapi import FastAPI, HTTPException, Path
import pandas as pd
from starlette.responses import RedirectResponse  # Corregir aquÃ­
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pandas as pd
import zipfile
from io import BytesIO
import requests
from pydantic import BaseModel
from fastapi import Depends
from zipfile import ZipFile


app = FastAPI()




#######################################################################################

ruta_de_archivo_zip1 = "https://github.com/EliasIchi/VNGLOBAL/raw/main/def_developer.zip"

def cargar_datos_desarrollador():
    # Descargar y descomprimir el archivo ZIP desde GitHub
    response = requests.get(ruta_de_archivo_zip1)
    zipf = zipfile.ZipFile(BytesIO(response.content))

    # Leer el archivo CSV desde el archivo ZIP con el separador ";"
    with zipf.open('developer_data.csv') as f:
        # Cargar el archivo CSV en un DataFrame
        df = pd.read_csv(f, sep=';')

    # Convertir la columna 'release_date' a datetime
    df['release_date'] = pd.to_datetime(df['release_date'])

    return df

@app.get('/developer')
async def developer(desarrollador: str):
    try:
        # Cargar los datos del desarrollador
        df = cargar_datos_desarrollador()

        # Filtrar el DataFrame por la empresa desarrolladora
        df_filtered = df[df['developer'] == desarrollador]

        # Agrupar por año y contar la cantidad de items
        grouped = df_filtered.groupby(df_filtered['release_date'].dt.year)

        # Inicializar una lista para almacenar los resultados
        result = []

        for year, data in grouped:
            # Calcular la cantidad total de items en el año
            total_items_year = data['items_count'].sum()

            # Calcular la cantidad de items gratuitos en el año
            free_items_year = data[data['price'].str.lower().str.contains('free')]['items_count'].sum()

            # Calcular el porcentaje de contenido gratuito en relación con el total de items en el año
            if total_items_year != 0:
                percentage_free = (free_items_year / total_items_year) * 100
            else:
                percentage_free = 0

            # Crear un diccionario con los resultados del año
            year_result = {
                'release_date': int(year),
                'Cantidad de Items': len(data),
                'Total Items': total_items_year,
                'Porcentaje de Contenido Free': percentage_free
            }

            # Agregar los resultados del año a la lista de resultados
            result.append(year_result)

        # Imprimir los resultados intermedios para depuración
        print("Resultados intermedios:")
        print(result)

        # Retornar la lista de resultados como respuesta JSON
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


