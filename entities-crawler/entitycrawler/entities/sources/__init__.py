from freebase import FreebaseEntityImporter
from imdb import IMDBEntityImporter
from musicbrainz import MusicbrainzngsEntityImporter
from wikidata import WikidataEntityImporter

SOURCES_LIST = [
    (FreebaseEntityImporter.source, FreebaseEntityImporter.source),
    (IMDBEntityImporter.source, IMDBEntityImporter.source),
    (MusicbrainzngsEntityImporter.source, MusicbrainzngsEntityImporter.source),
    (WikidataEntityImporter.source, WikidataEntityImporter.source),
]
