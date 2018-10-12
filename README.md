# Wiki climate

Tool with which climate data can be extracted from Wikipedia for all cities.

Accompanying code to analysis [what is best city to live in](https://www.kaggle.com/brankokokanovic/best-city-to-live-in). Latest (October 2018) data can be seen at [Kaggle](https://www.kaggle.com/brankokokanovic/wiki-climate/home).

Works by obtaining cities using SPARQL/Wikidata and then query Wikipedia (using pywikibot) to parse those weatherbox boxes.
Data is stored in Mongo and can be exported to JSON with:

```
mongo.exe --quiet temp --eval "printjson(db.cities.find({}, {'_id':0}).toArray())" > wiki-climate.json
```
