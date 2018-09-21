import re
from statistics import pstdev
from typing import List, Union

import pywikibot
import requests
from pymongo import MongoClient
from pymongo.database import Database

# This is SPARQL query that gets us all cities with Wikipedia articles that have populatatio over 10000
query = '''
SELECT DISTINCT ?city ?cityLabel ?population ?country ?countryLabel ?article (SAMPLE(?gps) AS ?gps)
WHERE
{
  ?city wdt:P31/wdt:P279* wd:Q515 .
  ?city wdt:P1082 ?population .
  ?city wdt:P625 ?gps .
  ?city wdt:P17 ?country .
  FILTER (?population >= 10000) .
  ?article schema:about ?city .
  ?article schema:inLanguage 'en' .
  FILTER REGEX(STR(?article), ".wikipedia.org/wiki/") .
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en" .
  }
}
GROUP BY ?city ?cityLabel ?population ?country ?countryLabel ?article
ORDER BY DESC(?population)
'''

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

en_wiki = pywikibot.Site("en", "wikipedia")


def month_avg(iterable: List[float])->float:
    return sum(iterable)/12


def f2c(f: float)->float:
    """
    Fahrenheit to Celsius conversion
    :param f: Fahrenheit value
    :return: Celsius value
    """
    return (f - 32) / 1.8


def i2mm(i: float)->float:
    """
    Inch to millimeter conversion
    :param i: Inch value
    :return: Millimeterr value
    """
    return i * 25.4


def get_cities() -> List[dict]:
    """
    Get cities from SPARQL wikidata query
    :return: List of obtained cities
    """
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
    return data['results']['bindings']


def get_weather_box(index: int, total: int, city: dict) -> Union[dict, None]:
    """
    Takes city and crawls wikipedia to get weather box. That weather box can be in multiple locations
    :param index: Index of iteration, needed only for logging
    :param total: Total amount of cities, needed only for logging
    :param city: City to get weather box from
    :return: Weather box dictionary
    """
    # Try original page of the city
    page = pywikibot.Page(en_wiki, re.search(r'wiki/(.*)', city['article']['value']).group(1))
    if page.pageid == 0:
        raise Exception('page do not exist')
    templates = page.raw_extracted_templates
    weather_box = next((t[1] for t in templates if t[0] in ['Weather box']), None)
    if weather_box is None:
        # If there is no weather box directly there, try looking for weather template (big cities usually have those)
        weather_template = next((t[0] for t in templates if 'weatherbox' in t[0]), None)
        if weather_template is None:
            print('({}/{}) Skipping {}, missing weather box'.format(index + 1, total, city['cityLabel']['value']))
            return None
        # OK, we found template, dig in there
        page = pywikibot.Page(en_wiki, 'Template: {}'.format(weather_template))
        if page.pageid == 0:
            raise Exception('page do not exist')
        templates = page.raw_extracted_templates
        weather_box = next((t[1] for t in templates if t[0] in ['Weather box']), None)
        if weather_box is None:
            print(' ({}/{})Skipping {}, missing weather box'.format(index + 1, total, city['cityLabel']['value']))
            return None
    return weather_box


def process_city(db: Database, index: int, total: int, city: dict):
    """
    Processes data for one city and insert it in DB. Processing data means making sure data is in the right type, adding
    missing averages/standard deviations, and normalizing from imperial to metric units
    :param db: Temperature Mongo database
    :param total: Total amount of cities, needed only for logging
    :param index: Index of iteration, needed only for logging
    :param city: City to be processed
    """
    # Create basic weather box (to be inserted if real is not found)
    gps = city['gps']['value']
    basic_box = {'name': city['cityLabel']['value'],
                 'population': int(city['population']['value']),
                 'country': city['countryLabel']['value'],
                 'city_wd': city['city']['value'],
                 'gps_lat': float(re.search(r'Point\((.*)\s', gps).group(1)),
                 'gps_lon': float(re.search(r'\s(.*)\)', gps).group(1))}

    weather_box = get_weather_box(index, total, city)
    # If we cannot find proper one, just insert basic one and we are done
    if weather_box is None:
        db.cities.insert_one(basic_box)
        return

    weather_box.update(basic_box)
    # Convert all data dealing with months to floats
    for month in MONTHS:
        for key in weather_box.keys():
            if month + ' ' in key and weather_box[key].strip() != '':
                try:
                    weather_box[key] = float(weather_box[key].
                                             replace('−', '-').
                                             replace('&minus;', '-').
                                             replace('trace', '0').
                                             replace('—', '-'))
                except ValueError:
                    print('Unable to convert value {} to float'.format(weather_box[key]))

    # Check we either have all 12 months or no data
    for param in ['high C', 'high F', 'mean C', 'mean F', 'low C', 'low F',
                  'humidity', 'sun', 'precipitation days', 'precipitation mm', 'precipitation inch',
                  'record high C', 'record high F', 'record low C', 'record low F']:
        if len(['{} {}'.format(m, param) in weather_box for m in MONTHS]) not in (0, 12):
            raise Exception('Parameter {} do not have all the months'.format(param))

    # Remove all data that are not floats
    for param in ['high C', 'high F', 'mean C', 'mean F', 'low C', 'low F',
                  'humidity', 'sun', 'precipitation days', 'precipitation mm', 'precipitation inch',
                  'record high C', 'record high F', 'record low C', 'record low F']:
        if '{} {}'.format(MONTHS[0], param) not in weather_box:
            continue
        all_are_floats = all(type(weather_box['{} {}'.format(m, param)]) == float for m in MONTHS)
        if not all_are_floats:
            for m in MONTHS:
                del weather_box['{} {}'.format(m, param)]

    # Convert imperial stuff
    for param, convert_function in {'high F': f2c, 'mean F': f2c, 'low F': f2c, 'record high F': f2c,
                                    'record low F': f2c, 'precipitation inch': i2mm}.items():
        if '{} {}'.format(MONTHS[0], param) not in weather_box:
            continue
        for m in MONTHS:
            converted = convert_function(weather_box['{} {}'.format(m, param)])
            weather_box['{} {}'.format(m, param)] = round(converted, 1)

    # Do additional aggregation per year
    for param, agg_function in {'high C': month_avg, 'mean C': month_avg, 'low C': month_avg,
                                'humidity': month_avg, 'sun': sum, 'precipitation days': sum, 'precipitation mm': sum,
                                'record high C': max, 'record low C': min}.items():
        if '{} {}'.format(MONTHS[0], param) not in weather_box:
            continue
        agg_value = agg_function([weather_box['{} {}'.format(m, param)] for m in MONTHS])
        weather_box['year {}'.format(param)] = round(agg_value, 1)

    # Do stdev
    for param in ['mean C', 'humidity', 'sun', 'precipitation days', 'precipitation mm']:
        if '{} {}'.format(MONTHS[0], param) not in weather_box:
            continue
        agg_value = pstdev([weather_box['{} {}'.format(m, param)] for m in MONTHS])
        weather_box['year {} stdev'.format(param)] = round(agg_value, 1)
    db.cities.insert_one(weather_box)


def process_cities(db: Database, cities: List[dict]):
    """
    Iterates for all cities and insert them in DB if they are not there
    :param db: Temperature Mongo database
    :param cities: List of cities
    """
    for index, city in enumerate(cities):
        if db.cities.find_one({'city_wd': city['city']['value']}):
            print('({}/{}) Skipping {}, already in DB'.format(index + 1, len(cities), city['cityLabel']['value']))
            continue
        print('({}/{}) Inserting {}'.format(index + 1, len(cities), city['cityLabel']['value']))
        process_city(db, index, len(cities), city)


if __name__ == '__main__':
    client = MongoClient()
    temp_db = client.temp
    all_cities = get_cities()
    process_cities(temp_db, all_cities)
