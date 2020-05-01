import petl as etl
import pycountry_convert as pycountry
import pandas as pd
import pymysql
import datetime

# Función para determinar el continente de un país por nombre
def get_continent_code(country):
    try:
        return pycountry.country_alpha2_to_continent_code(pycountry.country_name_to_country_alpha2(country))
    except :
        # Manejamos las excepciones de países o lugares que no son países oficiales
        if (country == 'Diamond Princess') or (country == 'Timor-Leste'):
            return 'AS'
        elif (country == 'MS Zaandam'):
            return 'NA'
        elif (country == 'Kosovo') or (country == 'Holy See'):
            return 'EU'
        else:
            return 'N/A'

# Fuente de los datos que vamos a leer
uri_confirmed = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
uri_death = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
uri_recovered = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'

# Procesamos primero casos confirmados
t_confirmed = etl.fromcsv(uri_confirmed)

# Cambiamos el nombre a los encabezados
t_confirmed = etl.rename(t_confirmed, {'Country/Region': 'Country'})

# Ajustamos los tipos de datos
# A partir de la columna 5, el tipo de dato es integer, que es el número de personas/casos
# Adicionalmente aprovechamos para cambiar el formato de la fecha de 1/23/20 a 2020-01-23 en el header
headers = etl.fieldnames(t_confirmed)
i=0
for header in headers:
    if i>=4:
        t_confirmed = etl.convert(t_confirmed, header, int)        # corregimos el tipo de dato
        fecha =  datetime.datetime.strptime(header, '%m/%d/%y')    # calculamos la fecha en formato correcto
        t_confirmed = etl.rename(t_confirmed, header, fecha.strftime('%Y-%m-%d'))   
    i = i + 1

# Eliminamos las columnas de Province/State, Lat y Lon que no vamos a utilizar
t_confirmed = etl.cutout(t_confirmed, 0, 2, 3)

# Ajustamos algunos nombres de países para luego asignarles una región/continente
t_confirmed = etl.convert(t_confirmed, 'Country', 'replace', 'Congo (Brazzaville)', 'Congo')
t_confirmed = etl.convert(t_confirmed, 'Country', 'replace', 'Congo (Kinshasa)', 'Democratic Republic of the Congo')
t_confirmed = etl.convert(t_confirmed, 'Country', 'replace', 'Cote d\'Ivoire', 'Ivory Coast')
t_confirmed = etl.convert(t_confirmed, 'Country', 'replace', 'Korea, South', 'South Korea')
t_confirmed = etl.convert(t_confirmed, 'Country', 'replace', 'West Bank and Gaza', 'Palestine')
t_confirmed = etl.convert(t_confirmed, 'Country', 'replace', 'Burma', 'Myanmar')
t_confirmed = etl.convert(t_confirmed, 'Country', 'replace', 'US', 'USA')
t_confirmed = etl.convert(t_confirmed, 'Country', 'replace', 'Taiwan*', 'Taiwan')

# Luego procedemos a agrupar y acumular los resultados por el país
df_confirmed = etl.todataframe(t_confirmed)
df = df_confirmed.groupby(['Country']).sum()
t_confirmed = etl.fromdataframe(df, include_index=True)

# Renombramos el campo de Country nuevamente
t_confirmed = etl.rename(t_confirmed, {'index': 'Country'})

# Luego agregamos las columnas de fecha como datos y renombramos las nuevas columnas
t_confirmed = etl.melt(t_confirmed, 'Country')
t_confirmed = etl.rename(t_confirmed, {'variable': 'Date'})
t_confirmed = etl.rename(t_confirmed, {'value': 'Cases'})

# Luego agregamos el continente para agrupar
t_confirmed = etl.addfield(t_confirmed, 'Continent', lambda rec: get_continent_code(rec['Country']))

# Y nuevamente nos aseguramos que sean del tipo de dato que deben ser.
t_confirmed = etl.convert(t_confirmed, 'Cases', int)
t_confirmed = etl.convert(t_confirmed, 'Date', lambda v: datetime.datetime.strptime(v, '%Y-%m-%d') )

#Finalmente, subimos el archivo al repositorio de datos
conn = pymysql.connect(password='cenfotec', database='covid', user='covid')
conn.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
etl.todb(t_confirmed, conn, 'confirmed', create=True, drop=True)
conn.close()



# Ejemplos de visualización para debugging
#print(etl.header(t_confirmed))
#print(etl.records(t_confirmed))
#print(t_confirmed.lookall())
#etl.tocsv(t_confirmed, 'confirmados.csv')
#df.to_csv(r'confirmados.csv', index=True, header=True)

#cols = etl.columns(t_confirmed)

