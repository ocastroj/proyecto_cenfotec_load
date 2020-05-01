import petl as etl
import pycountry_convert as pycountry
import pandas as pd
import pymysql
import sys
import datetime

# Función para determinar el continente de un país por nombre
def get_continent_code(country):
    try:
        return pycountry.country_alpha2_to_continent_code(pycountry.country_name_to_country_alpha2(country))
    except :
        # Manejamos las excepciones de países o lugares que no son países oficiales
        if (country == 'Diamond Princess') or (country == 'Timor-Leste'):
            return 'AS'
        elif (country == 'Western Sahara'):
            return 'AF'
        elif (country == 'MS Zaandam'):
            return 'NA'
        elif (country == 'Kosovo') or (country == 'Holy See'):
            return 'EU'
        else:
            # Nos permite revisar si hay algún país con error
            print('País no encontrado: %s', country)
            return 'N/A'
        
# Función para procesar los archivos de casos confirmados, fallecidos y recuperados
# Esperamos el path del archivo y un nombre que será usado como nombre de la tabla en la base de datos
def procesar_fuente(path, nombre):
    try: 
        # Procesamos primero casos confirmados
        tabla = etl.fromcsv(path)

        # Cambiamos el nombre a los encabezados
        tabla = etl.rename(tabla, {'Country/Region': 'Country'})

        # Ajustamos los tipos de datos
        # A partir de la columna 5, el tipo de dato es integer, que es el número de personas/casos
        # Adicionalmente aprovechamos para cambiar el formato de la fecha de 1/23/20 a 2020-01-23 en el header
        headers = etl.fieldnames(tabla)
        i=0
        for header in headers:
            if i>=4:
                tabla = etl.convert(tabla, header, int)        # corregimos el tipo de dato
                fecha =  datetime.datetime.strptime(header, '%m/%d/%y')    # calculamos la fecha en formato correcto
                tabla = etl.rename(tabla, header, fecha.strftime('%Y-%m-%d'))   
            i = i + 1

        # Eliminamos las columnas de Province/State, Lat y Lon que no vamos a utilizar
        tabla = etl.cutout(tabla, 0, 2, 3)

        # Ajustamos algunos nombres de países para luego asignarles una región/continente
        tabla = etl.convert(tabla, 'Country', 'replace', 'Congo (Brazzaville)', 'Congo')
        tabla = etl.convert(tabla, 'Country', 'replace', 'Congo (Kinshasa)', 'Democratic Republic of the Congo')
        tabla = etl.convert(tabla, 'Country', 'replace', 'Cote d\'Ivoire', 'Ivory Coast')
        tabla = etl.convert(tabla, 'Country', 'replace', 'Korea, South', 'South Korea')
        tabla = etl.convert(tabla, 'Country', 'replace', 'West Bank and Gaza', 'Palestine')
        tabla = etl.convert(tabla, 'Country', 'replace', 'Burma', 'Myanmar')
        tabla = etl.convert(tabla, 'Country', 'replace', 'US', 'USA')
        tabla = etl.convert(tabla, 'Country', 'replace', 'Taiwan*', 'Taiwan')

        # Luego procedemos a agrupar y acumular los resultados por el país
        df_confirmed = etl.todataframe(tabla)
        df = df_confirmed.groupby(['Country']).sum()
        tabla = etl.fromdataframe(df, include_index=True)

        # Renombramos el campo de Country nuevamente
        tabla = etl.rename(tabla, {'index': 'Country'})

        # Luego agregamos las columnas de fecha como datos y renombramos las nuevas columnas
        tabla = etl.melt(tabla, 'Country')
        tabla = etl.rename(tabla, {'variable': 'Date'})
        tabla = etl.rename(tabla, {'value': 'Cases'})

        # Luego agregamos el continente para agrupar
        tabla = etl.addfield(tabla, 'Continent', lambda rec: get_continent_code(rec['Country']))

        # Y nuevamente nos aseguramos que sean del tipo de dato que deben ser.
        tabla = etl.convert(tabla, 'Cases', int)
        tabla = etl.convert(tabla, 'Date', lambda v: datetime.datetime.strptime(v, '%Y-%m-%d') )

        #Finalmente, subimos el archivo al repositorio de datos
        conn = pymysql.connect(password='cenfotec', database='covid', user='covid')
        conn.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        etl.todb(tabla, conn, nombre, create=True, drop=True)
        conn.close()
    except:
        print('Se ha presentado un error! ', sys.exc_info()[0])
        raise


# Fuente de los datos que vamos a leer
uri_confirmed = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
uri_death = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
uri_recovered = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'

# Usamos la función de procesar_fuente para cargar los diferentes archivos a la base de datos
procesar_fuente(uri_confirmed, 'confirmados')
procesar_fuente(uri_death, 'fallecidos')
procesar_fuente(uri_recovered, 'recuperados')

# Ejemplos de visualización para debugging
#print(etl.header(tabla))
#print(etl.records(tabla))
#print(tabla.lookall())
#etl.tocsv(tabla, 'confirmados.csv')
#df.to_csv(r'confirmados.csv', index=True, header=True)

