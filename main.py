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
import pyarrow

app = FastAPI()


@app.get("/")
def root():
    return RedirectResponse(url="/docs/")


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


#############################################################################################################

ruta_de_archivo_zip2 = "https://github.com/EliasIchi/VNGLOBAL/raw/main/def_userdata.zip"


class UserData(BaseModel):
    usuario: str
    dinero_gastado: float
    porcentaje_recomendacion: float
    cantidad_items: int


def cargar_datos_usuario() -> pd.DataFrame:
    response = requests.get(ruta_de_archivo_zip2)
    with ZipFile(BytesIO(response.content)) as zipf:
        with zipf.open('def_userdata.csv') as f:
            df = pd.read_csv(f, sep=',', encoding='latin-1')
    return df

def obtener_datos_usuario(User_id: str, df: pd.DataFrame = Depends(cargar_datos_usuario)) -> UserData:
    user_data = df[df['user_id'] == User_id]
    total_money_spent = user_data['price'].sum()
    total_recommendations = user_data['recommend'].count()
    positive_recommendations = user_data['recommend'].sum()
    recommendation_percentage = (positive_recommendations / total_recommendations) * 100 if total_recommendations > 0 else 0
    total_items_purchased = user_data['items_count'].sum()
    return UserData(
        usuario=User_id,
        dinero_gastado=total_money_spent,
        porcentaje_recomendacion=recommendation_percentage,
        cantidad_items=total_items_purchased
    )

@app.get('/userdata/{User_id}', response_model=UserData)
async def get_user_data(User_data: UserData = Depends(obtener_datos_usuario)):
    return User_data


#############################################################################################################




def UserForGenre(genero: str, ruta_de_archivo_zip: str):
    try:
        # Descargar y descomprimir el archivo ZIP desde la URL proporcionada
        response = requests.get(ruta_de_archivo_zip)
        with zipfile.ZipFile(BytesIO(response.content)) as zipf:
            with zipf.open('def_UserForGenre.csv') as f:
                # Leer el archivo CSV en un DataFrame
                df = pd.read_csv(f, sep=';')

        # Transformar la columna 'release_date' a tipo datetime
        df['release_date'] = pd.to_datetime(df['release_date'])

        # Filtrar el DataFrame por el género especificado
        df_genre = df[df['genres'].str.contains(genero)]

        # Agrupar por año de lanzamiento y sumar las horas jugadas
        df_year_playtime = df_genre.groupby(df_genre['release_date'].dt.year)['playtime_forever'].sum()

        # Convertir el tiempo jugado de minutos a horas y redondear a 2 decimales
        df_year_playtime = df_year_playtime.apply(lambda x: round(x / 60, 2))

        # Crear la lista de la acumulación de horas jugadas por Anio de lanzamiento
        hours_by_year = [{'Anio': year, 'Horas': hours} for year, hours in df_year_playtime.items()]

        # Ordenar la lista por la cantidad de horas jugadas en cada año de manera descendente
        hours_by_year.sort(key=lambda x: x['Horas'], reverse=True)

        # Obtener el usuario con mas horas jugadas para el genero dado
        max_user = df_genre[df_genre['playtime_forever'] == df_genre['playtime_forever'].max()]['user_id'].iloc[0]

        result = {
            'Usuario con mas horas jugadas para Genero {}'.format(genero): max_user,
            'Horas jugadas': hours_by_year
        }
        return result

    except Exception as e:
        return {'Error': str(e)}

# Define la ruta para obtener los datos del usuario
@app.get('/UserForGenre/{genero}')
async def get_user_data(genero: str):
    ruta_de_archivo_zip = "https://github.com/EliasIchi/VNGLOBAL/raw/main/UserForGenre.zip"
    resultado = UserForGenre(genero, ruta_de_archivo_zip)
    return resultado

########################################################################################################

def best_developer_year(anio: int):
    try:
        # Cargar el archivo Parquet y obtener el DataFrame del archivo CSV dentro del Parquet
        parquet_url = "https://github.com/EliasIchi/VNGLOBAL/raw/main/best_developer_year.parquet"
        df = pd.read_parquet(parquet_url, engine='pyarrow', columns=['release_date', 'developer', 'recommend'])
        
        # Filtrar los juegos para el aniodado
        df_filtered = df[df['release_date'].dt.year == anio]
        
        # Filtrar los juegos recomendados y con comentarios positivos
        df_positive_reviews = df_filtered[df_filtered['recommend'] == True]
        
        # Contar la cantidad de juegos recomendados por desarrollador
        developer_count = df_positive_reviews['developer'].str.split(',', expand=True)[0].value_counts()
        
        # Ordenar los desarrolladores según la cantidad de juegos recomendados
        sorted_developers = developer_count.sort_values(ascending=False)
        
        # Tomar los primeros 3 desarrolladores
        top_developers = sorted_developers.head(3)
        
        # Crear la lista de resultados
        result = [{"Puesto " + str(i+1): developer, "Juegos Recomendados": count} for i, (developer, count) in enumerate(top_developers.items())]
        
        return result
    
    except Exception as e:
        return {'Error': str(e)}


@app.get("/best_developer/{year}")
async def get_best_developer(year: int):
    # Llamar a la función best_developer_year con el anio proporcionado
    result = best_developer_year(year)
    return result

#####################################################################################################

@app.get('/developer_reviews_analysis')
async def developer_reviews_analysis(desarrolladora: str):
    try:
        # Cargar una muestra aleatoria del 10% del archivo Parquet
        parquet_url = "https://github.com/EliasIchi/VNGLOBAL/raw/main/sentimientos_final.parquet"
        df = pd.read_parquet(parquet_url)
        df_sample = df.sample(frac=0.1, random_state=42)  # Tomar el 10% de los datos
        
        # Filtrar las reseñas para la desarrolladora especificada
        df_desarrolladora = df_sample[df_sample['developer'] == desarrolladora]
        
        # Filtrar reseñas positivas y negativas
        df_positive = df_desarrolladora[df_desarrolladora['sentiment_analysis'] == 2]
        df_negative = df_desarrolladora[df_desarrolladora['sentiment_analysis'] == 0]
        
        # Contar la cantidad de reseñas positivas y negativas
        positive_reviews = len(df_positive)
        negative_reviews = len(df_negative)
        
        # Construir el diccionario de resultados
        result = {desarrolladora: {'Positive': positive_reviews, 'Negative': negative_reviews}}
        
        return result
    
    except Exception as e:
        return {'Error': str(e)}

################################################################################################


# URL del archivo Parquet en GitHub

url_parquet = "https://github.com/EliasIchi/PI1_Henry_EliasAlmada/raw/main/datasets/sistema_de_recomendacion.parquet"

# Cargar el DataFrame con los datos de los juegos y sus características
def cargar_datos_juegos():
    # Cargar el DataFrame desde el archivo Parquet en GitHub
    df = pd.read_parquet(url_parquet)
    return df

# Función de recomendación de juegos similares a uno dado
def recomendacion_juego(id_producto, df, num_recomendaciones=5):
    # Verificar si el ID del producto dado existe en el DataFrame
    if id_producto not in df['id'].values:
        raise HTTPException(status_code=404, detail="El ID del producto especificado no existe en el DataFrame")
    
    # Obtener el género del juego dado
    genero_juego = df[df['id'] == id_producto]['genres'].iloc[0]

    # Filtrar juegos con el mismo género y obtener juegos similares
    juegos_similares = df[df['genres'].str.contains(genero_juego) & (df['id'] != id_producto)]

    # Si hay menos de num_recomendaciones juegos similares, completar con los siguientes juegos en el DataFrame
    num_similares = len(juegos_similares)
    if num_similares < num_recomendaciones:
        # Obtener los siguientes juegos en el DataFrame sin repetir
        siguientes_juegos = df[~df['id'].isin(juegos_similares['id'])].head(num_recomendaciones - num_similares)
        # Concatenar juegos similares con siguientes juegos
        juegos_recomendados = pd.concat([juegos_similares, siguientes_juegos])
    else:
        juegos_recomendados = juegos_similares

    # Eliminar juegos con nombres duplicados
    juegos_recomendados = juegos_recomendados.drop_duplicates(subset=['app_name'])

    # Si hay más de num_recomendaciones juegos recomendados, seleccionar 5 al azar
    if len(juegos_recomendados) > num_recomendaciones:
        juegos_recomendados = juegos_recomendados.sample(n=num_recomendaciones)

    return juegos_recomendados[['id', 'app_name']].to_dict(orient='records')


# Definir un endpoint GET para obtener recomendaciones de juegos similares dado un ID de juego
@app.get("/recomendacion/{id_juego}")
async def obtener_recomendaciones(id_juego: int):
    df = cargar_datos_juegos()
    recomendaciones = recomendacion_juego(id_juego, df)
    return recomendaciones
