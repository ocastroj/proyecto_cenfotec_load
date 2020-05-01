import pycountry_convert as pycountry

# countries = [
#     'Kosovo',
#     'Holy See',
#     'Timor-Leste'
# ]

# for country in countries:

country = 'Democratic Republic of the Congo'
#print(pycountry.country_alpha2_to_continent_code(pycountry.country_name_to_country_alpha2(country)))
print(pycountry.country_name_to_country_alpha2(country))