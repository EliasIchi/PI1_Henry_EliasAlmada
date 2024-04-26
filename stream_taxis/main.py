import streamlit as st
import snowflake.connector
import pandas as pd

snowflake_credentials = {
    'user': 'ELIASALMADA1234',
    'password': 'Ichi2017',
    'account': 'pzbgdyt-aib83585',
    'warehouse': 'COMPUTE_WH',
    'database': 'SCHEMA_TAXIS_NYC_ECODRIVE',
    'schema': 'PUBLIC'
}

# Establecer la conexión a Snowflake al inicio de la aplicación
conexion = snowflake.connector.connect(**snowflake_credentials)

def ejecutar_consulta(sql_query):
    try:
        cursor = conexion.cursor()
        cursor.execute(sql_query)
        registros = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(registros, columns=columnas)
        return df
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Error al ejecutar consulta: {e}")
        return None
    finally:
        cursor.close()
def subir_datos_a_base_de_datos(conn, fecha, millas_electrico_turno_1, millas_electrico_turno_2,
                                millas_electrico_turno_3, millas_convencionales_turno_1,
                                millas_convencionales_turno_2, millas_convencionales_turno_3):
    try:
        cursor = conn.cursor()
        
        cursor.execute(f"""INSERT INTO SCHEMA_TAXIS_NYC_ECODRIVE.PUBLIC.USO_VEHICULOS_ELECTRICOS (Fecha, Millas_Elec_T1, Millas_Elec_T2, Millas_Elec_T3, 
                            Millas_Conv_T1, Millas_Conv_T2, Millas_Conv_T3) 
                            VALUES ('{fecha}', {millas_electrico_turno_1}, {millas_electrico_turno_2}, 
                            {millas_electrico_turno_3}, {millas_convencionales_turno_1}, 
                            {millas_convencionales_turno_2}, {millas_convencionales_turno_3})""")
        
        conn.commit()
        st.success("Datos insertados correctamente en base de datos.")

    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Error al insertar datos en base de datos: {e}")

    finally:
        # Cerrar el cursor
        cursor.close()

def mostrar_interfaz():
    fecha = st.date_input("Fecha", format="YYYY-MM-DD")  # Forzar el formato de fecha a aaaa-mm-dd
    millas_electrico_turno_1 = st.number_input("Millas Eléc. Turno 1")
    millas_electrico_turno_2 = st.number_input("Millas Eléc. Turno 2")
    millas_electrico_turno_3 = st.number_input("Millas Eléc. Turno 3")
    millas_convencionales_turno_1 = st.number_input("Millas Conv. Turno 1")
    millas_convencionales_turno_2 = st.number_input("Millas Conv. Turno 2")
    millas_convencionales_turno_3 = st.number_input("Millas Conv. Turno 3")

    if st.button("Registrar Uso"):
        subir_datos_a_base_de_datos(conexion, fecha, millas_electrico_turno_1, millas_electrico_turno_2,
                                    millas_electrico_turno_3, millas_convencionales_turno_1,
                                    millas_convencionales_turno_2, millas_convencionales_turno_3)

    if st.button("Mostrar resultados"):
        sql_query = """
        SELECT
        FECHA,
        MILLAS_ELEC_T1,
        MILLAS_ELEC_T2,
        MILLAS_ELEC_T3,
        MILLAS_CONV_T1,
        MILLAS_CONV_T2,
        MILLAS_CONV_T3,
        MILLAS_TOTAL / MILLAS_ANTERIOR AS PORCENTAJE_PARTICIPACION_FLOTA_ELECTRICA
        FROM
        (
        SELECT *,
        (MILLAS_ELEC_T1 +
        MILLAS_ELEC_T2 +
        MILLAS_ELEC_T3 +
        MILLAS_CONV_T1 +
        MILLAS_CONV_T2 +
        MILLAS_CONV_T3) AS MILLAS_TOTAL,
        LAG(MILLAS_TOTAL, 1) OVER (ORDER BY id ASC) AS MILLAS_ANTERIOR
        FROM SCHEMA_TAXIS_NYC_ECODRIVE.PUBLIC.USO_VEHICULOS_ELECTRICOS
        ORDER BY id ASC) AS A
        GROUP BY FECHA;
        """
        resultados = ejecutar_consulta(sql_query)
        if resultados is not None:
            st.write(resultados)
            st.success("Consulta ejecutada correctamente.")

# Crear la interfaz gráfica con Streamlit
st.title("Análisis de Uso de Vehículos Eléctricos")
mostrar_interfaz()

