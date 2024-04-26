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

def obtener_ultimos_registros(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""SELECT * FROM SCHEMA_TAXIS_NYC_ECODRIVE.PUBLIC.USO_VEHICULOS_ELECTRICOS 
                           ORDER BY Fecha DESC LIMIT 3""")
        registros = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(registros, columns=columnas)
        return df

    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Error al obtener los últimos registros: {e}")
        return None

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

    # Mostrar los últimos 3 registros después de registrar nuevos datos
    st.subheader("Últimos 3 Registros:")
    ultimos_registros = obtener_ultimos_registros(conexion)
    if ultimos_registros is not None:
        st.write(ultimos_registros)

# Crear la interfaz gráfica con Streamlit
st.title("Registro de Uso de Vehículos Eléctricos")
mostrar_interfaz()
