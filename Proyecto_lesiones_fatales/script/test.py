#Librerias
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px


#Extracion de archivo y se unen las fuentes.
ruta_carpeta = "Archivos\\Proyecto_lesiones_fatales\\fuentes"
ruta_archivo = "" 
datos_p = pd.DataFrame() #df provisional.
datos_u = pd.DataFrame() #df final con los datos unidos.

#Buscar los archivos en el repositorio.
for carpeta_actual, subcarpetas, archivos in os.walk(ruta_carpeta):
    for archivo in archivos:
        ruta_archivo = ruta_carpeta + "\\" + archivo
        datos_p = pd.read_csv(ruta_archivo, sep=None, engine="python", encoding="UTF-8")
        #Union de las dos fuentes
        datos_u = pd.concat([datos_u, datos_p], axis=0)

#Renombrar columnas
datos_u = datos_u.rename(columns={
    'ID':'id',
    'Año del hecho':'ano',
    'Grupo de edad de la víctima':'g_edad_victima',
    'Grupo Mayor Menor de Edad':'mayor_edad',
    'Edad judicial':'edad_judicial',
    'Ciclo Vital':'ciclo_vital',
    'Sexo de la víctima':'sexo',
    'Estado Civil':'estado_civil',
    'País de Nacimiento de la Víctima':'pais_nacimiento',
    'Escolaridad':'escolaridad',
    'Pertenencia Grupal':'pertenencia_grupal',
    'Mes del hecho':'mes',
    'Dia del hecho':'dia',
    'Rango de Hora del Hecho X 3 Horas':'rango_hora',
    'Código Dane Municipio':'cod_dane_municipio',
    'Municipio del hecho DANE':'municipio',
    'Departamento del hecho DANE':'departamento',
    'Código Dane Departamento':'cod_dane_departamento',
    'Escenario del Hecho':'escenario',
    'Zona del Hecho':'zona',
    'Actividad Durante el Hecho':'actividad',
    'Circunstancia del Hecho':'circustancia',
    'Manera de Muerte':'manera_muerte',
    'Mecanismo Causal':'mecanismo_causal',
    'Diagnóstico Topográfico de la Lesión':'lesion',
    'Presunto Agresor':'presunto_agresor',
    'Razón del Suicidio':'razon_suicidio',
    'Condición de la Víctima':'condicion_victima',
    'Medio de Desplazamiento o Transporte':'medio_transporte',
    'Servicio del Vehículo':'servicio_transporte',
    'Clase o Tipo de Accidente':'tipo_accidente',
    'Objeto de Colisión':'objeto_colision',
    'Servicio del Objeto de Colisión':'servicio_objeto_colision',
    'Localidad del Hecho':'localidad',
    'Ancestro Racial':'ancestro_racial',
    'Pueblo Indígena':'pueblo_indigena',
    'Orientación Sexual':'orientacion_sexual',
    'Identidad de Género':'identidad_genero',
    'Transgénero':'transgenero'
})
             
#datos dia, mes, rango_hora upper
datos_u['mes'] = datos_u['mes'].str.upper()
datos_u['dia'] = datos_u['dia'].str.upper()
datos_u['rango_hora'] = datos_u['rango_hora'].str.upper()

#datos mayor_edad 
datos_u['mayor_edad'] = datos_u['mayor_edad'].str.replace(r"[a-zA-Z]\)\s*", "", regex=True)
datos_u['mayor_edad'] = datos_u['mayor_edad'].str.replace(r"\(\s*", "", regex=True)
# transformacion 1 y 0
datos_u['mayor_edad'] = datos_u['mayor_edad'].replace({
    "Menores de Edad <18 año" : 0,
    "Mayores de Edad >18 año" : 1,
    "Sin información" : -1,
    "Por determinar"  : -1
})

# Parámetros de conexión
host = "localhost"  # Ejemplo: "localhost" o una IP
port = "5432"  # Puerto por defecto de PostgreSQL
dbname = "db_lesiones_fatales"
schema_stg = "stg"
schema_dwh = "dwh"
user = "arq"
password = "password"

# Crear la cadena de conexión
connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

# Crear el engine
engine = create_engine(connection_string,
    connect_args={'options': '-csearch_path={}'.format(schema_stg)})

datos_u.to_sql('stg_lesiones_fatales', engine, if_exists='replace', index=False)

print("DataFrame insertado correctamente en PostgreSQL.")

app = Dash()

# Requires Dash 2.17.0 or later
app.layout = [
    html.H1(children='Title of Dash App', style={'textAlign':'center'}),
    dcc.Dropdown(datos_u['pais_nacimiento'].unique(), 'Canada', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
]

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = datos_u[datos_u['pais_nacimiento']==value]
    return px.line(dff, x='ano', y='mayor_edad')

if __name__ == '__main__':
    app.run(debug=True)