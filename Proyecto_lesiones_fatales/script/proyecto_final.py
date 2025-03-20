
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px

import pandas as pd

#Extracion de archivo y se unen las fuentes.
ruta_carpeta = "Archivos\\Proyecto_lesiones_fatales\\fuentes"
ruta_archivo = "" 
datos_p = pd.DataFrame() #df provisional.
Datos = pd.DataFrame() #df final con los datos unidos.

#Buscar los archivos en el repositorio.
for carpeta_actual, subcarpetas, archivos in os.walk(ruta_carpeta):
    for archivo in archivos:
        ruta_archivo = ruta_carpeta + "\\" + archivo
        datos_p = pd.read_csv(ruta_archivo, sep=None, engine="python", encoding="UTF-8")
        #Union de las dos fuentes
        Datos = pd.concat([Datos, datos_p], axis=0)

# Renombrar columnas
Datos = Datos.rename(columns={
    'A~$el Hecho': 'Año del hecho',
    'Grupo Mayor Menor de Edad': '¿Es mayor de edad?'
})

#identificar datos duplicados
duplicados = Datos[Datos.duplicated()]
print(duplicados)

# Información general del DataFrame
Datos.info()

# Estadísticas de todas las columnas (numéricas y categóricas)
Datos.describe(include="all")

# Ver los primeros registros
Datos.head()

# Contar valores nulos por columna
print(Datos.isnull().sum())

# Ver nombres de columnas
print(Datos.columns)

# Fusionar columnas llenando valores faltantes
Datos['Sexo de la victima'] = Datos['Sexo de la victima'].fillna(Datos['Sexo de la víctima'])
Datos['Grupo de edad de la victima'] = Datos['Grupo de edad de la victima'].fillna(Datos['Grupo de edad de la víctima'])
Datos['Diagnostico Topográfico de la Lesión'] = Datos['Diagnostico Topográfico de la Lesión'].fillna(Datos['Diagnóstico Topográfico de la Lesión'])

# Eliminar las columnas redundantes
Datos.drop(columns=['Sexo de la víctima', 'Grupo de edad de la víctima', 'Diagnóstico Topográfico de la Lesión', 'Edad judicial', 'Ciclo Vital'], inplace=True)

# Contar valores nulos por columna
print(Datos.isnull().sum())
#Contar 'No aplica' por columna
print(Datos.apply(lambda x: x.astype(str).str.contains('No aplica', case=False).sum()))

# Calcular que columnas tienen más de 40% datos nulos
columnas_con_muchos_nulos = Datos.columns[Datos.isnull().mean() > 0.4]
print(columnas_con_muchos_nulos)

#Eliminar las columnas que tienen mas de 40% de datos nulos
Datos = Datos.drop(columns=columnas_con_muchos_nulos)

#calcular que columnas tienen mas de 40% datos de 'No aplica'
columnas_con_muchos_no_aplica = Datos.apply(lambda x: x.astype(str).str.contains('No aplica', case=False).mean() > 0.4)

#Eliminar columnas que tengan más de 40% de datos 'No aplica'
Datos = Datos.drop(columns=columnas_con_muchos_no_aplica[columnas_con_muchos_no_aplica].index)

# Contar valores nulos por columna
print(Datos.isnull().sum())
#Contar valores 'No aplica' por columna
print(Datos.apply(lambda x: x.astype(str).str.contains('No aplica', case=False).sum()))

#Rellenar espacios sin datos con 'Sin información'
Datos.fillna('Sin Información', inplace=True)

Datos['¿Es mayor de edad?'] = Datos['¿Es mayor de edad?'].str.replace(r"[a-zA-Z]\)\s*", "", regex=True)
# Reemplazar los valores limpios por 1 y 0
Datos["¿Es mayor de edad?"] = Datos["¿Es mayor de edad?"].replace({
    "Menores de Edad (<18 año": 0,
    "Mayores de Edad (>18 año": 1
})

# Eliminar filas donde la columna "Departamento del hecho DANE" contiene números
Datos = Datos[~Datos['Departamento del hecho DANE'].str.contains(r'\d', na=False)]

# Reemplazar nombres incorrectos
Datos['Departamento del hecho DANE'] = Datos['Departamento del hecho DANE'].replace({
    'Boyac': 'Boyacá',
    'Quindo': 'Quindío',
    'Atlntico': 'Atlántico',
    'Crdoba': 'Córdoba',
    'Nario': 'Nariño',
    'Amazonas': 'Amazonas',  # Para asegurarnos de que es consistente
    'Antioquia': 'Antioquia',
    'Quindío': 'Quindío',
    'Santander': 'Santander',
    'Boyaca': 'Boyacá',
    'Atlántico': 'Atlántico',
    'Valle del Cauca': 'Valle del Cauca',
    'Bolvar': 'Bolívar',
    'Córdoba': 'Córdoba',
    'Tolima': 'Tolima',
    'Meta': 'Meta',
    'Norte de Santander': 'Norte de Santander',
    'Magdalena': 'Magdalena',
    'Cundinamarca': 'Cundinamarca',
    'Nariño': 'Nariño',
    'Caldas': 'Caldas',
    'Amazonas': 'Amazonas',
    'Risaralda': 'Risaralda',
    'Cordoba': 'Córdoba',
    'Huila': 'Huila',
    'Cauca': 'Cauca',
    'Choc': 'Chocó',
    'Guaviare': 'Guaviare',
    'Bogot DC': 'Bogotá D.C.',
    'Sucre': 'Sucre',
    'Casanare': 'Casanare',
    'Arauca': 'Arauca',
    'Cesar': 'César',
    'Sin informacion': 'Sin información',
    'Putumayo': 'Putumayo',
    'La Guajira': 'La Guajira',
    'Vaups': 'Vaupés',
    'Caquet': 'Caquetá',
    'Guaina': 'Guaviare',
    'Archipilago de San Andres Providencia y Santa Catalina': 'Archipiélago de San Andrés, Providencia y Santa Catalina',
    'Vichada': 'Vichada',
    'Otros': 'Otros',
    'Vivienda': 'Vivienda',
    'Espacios acuticos al aire libre mar ro arroyo humedal lago etc': 'Espacios acuáticos al aire libre (mar, río, arroyo, humedal, lago, etc.)',
    'Va pblica': 'Vía pública',
    'Archipilago de San Andrs Providencia y Santa Catalina': 'Archipiélago de San Andrés, Providencia y Santa Catalina'
})

# Eliminar filas con valores que no son válidos
Datos = Datos[Datos['Departamento del hecho DANE'].notna()]

# Guardar el archivo como Excel (XLSX)
Datos.to_excel("Archivos\\Proyecto_lesiones_fatales\\dest\\datos_limpios.xlsx", index=False)

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
    connect_args={'options': '-csearch_path={}'.format(schema_dwh)})

Datos.to_sql('lesiones_fatales', engine, if_exists='replace', index=False)

print("DataFrame insertado correctamente en PostgreSQL.")

datos_reporte = pd.read_sql("select \"País de Nacimiento de la Víctima\" As pais,\"Año del hecho\" as año, count(*)as conteo from dwh.lesiones_fatales group by \"Año del hecho\", \"País de Nacimiento de la Víctima\" order by 2",engine)

app = Dash()

app.layout = [
    html.H1(children='Conteo de registro Año, pais', style={'textAlign':'center'}),
    dcc.Dropdown(datos_reporte['pais'].unique(), 'Canada', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
]

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = datos_reporte[datos_reporte['pais']==value]
    return px.bar(dff, x='año', y='conteo')

if __name__ == '__main__':
    app.run(debug=True)