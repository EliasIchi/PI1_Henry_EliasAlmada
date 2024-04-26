import streamlit as st
import snowflake.connector

snowflake_credentials = {
    'user': 'ELIASALMADA1234',
    'password': 'Ichi2017',
    'account': 'pzbgdyt-aib83585',
    'warehouse': 'COMPUTE_WH',
    'database': 'SCHEMA_TAXIS_NYC_ECODRIVE',
    'schema': 'PUBLIC'
}

def establecer_conexion():
    try:
        # Establecer conexión a Snowflake
        conexion = snowflake.connector.connect(**snowflake_credentials)
        st.success("¡Conexión exitosa a Snowflake!")
        return conexion
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Error al conectar a Snowflake: {e}")
        return None

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
        st.success("Datos insertados correctamente en Snowflake.")

    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Error al insertar datos en Snowflake: {e}")

    finally:
        # Cerrar el cursor
        cursor.close()

def registrar_uso():
    fecha = st.text_input("Fecha")
    millas_electrico_turno_1 = st.number_input("Millas Eléc. Turno 1")
    millas_electrico_turno_2 = st.number_input("Millas Eléc. Turno 2")
    millas_electrico_turno_3 = st.number_input("Millas Eléc. Turno 3")
    millas_convencionales_turno_1 = st.number_input("Millas Conv. Turno 1")
    millas_convencionales_turno_2 = st.number_input("Millas Conv. Turno 2")
    millas_convencionales_turno_3 = st.number_input("Millas Conv. Turno 3")

    conexion = establecer_conexion()
    if conexion is None:
        return

    subir_datos_a_base_de_datos(conexion, fecha, millas_electrico_turno_1, millas_electrico_turno_2,
                                millas_electrico_turno_3, millas_convencionales_turno_1,
                                millas_convencionales_turno_2, millas_convencionales_turno_3)
    conexion.close()

    st.success("Datos registrados correctamente en Snowflake.")

# Crear la interfaz gráfica con Streamlit
st.title("Registro de Uso de Vehículos Eléctricos")

registrar_uso()
