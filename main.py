#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
from   os       import makedirs, unlink
from   os.path  import abspath, dirname, exists, splitext
import re
from   shlex             import quote
from   typing_extensions import Annotated

import duckdb
from   geojson          import (dumps,
                                Feature,
                                Polygon)
import h3
from   rich.progress    import track
from   shapely          import wkt
from   shapely.geometry import MultiPolygon, shape
from   shapely.ops      import polygonize
from   shpyx            import run as execute
import typer


app = typer.Typer(rich_markup_mode='rich')


remove_ext = lambda filename: splitext(filename)[0]


def ot_to_json(other_tag, remove_sub=True):
    out = {}

    for x in other_tag.split('","'):
        if '"=>"' not in x:
            continue

        x = x.strip('"')
        parts = x.split('"=>"')

        if len(parts) != 2:
            continue

        key = parts[0].strip().lower()

        if remove_sub:
            key = key.split(':')[0]

        out[key] = parts[1].strip()

    return out


def num_rows_by_type(osm_file: str, layer: str):
    con = duckdb.connect(database=":memory:")
    con.execute('INSTALL spatial')
    con.execute('LOAD spatial')
    sql = """SELECT COUNT(*)
             FROM ST_READ(?,
                          open_options=['INTERLEAVED_READING=YES'],
                          layer=?,
                          sequential_layer_scan=true)"""

    return con.sql(sql, params=(osm_file, layer)).fetchone()[0]


def get_geom_by_type(osm_file: str, layer: str):
    con = duckdb.connect(database=":memory:")
    con.execute('INSTALL spatial')
    con.execute('LOAD spatial')
    sql = """SELECT other_tags,
                    st_astext(geom)
             FROM ST_READ(?,
                          open_options=['INTERLEAVED_READING=YES'],
                          layer=?,
                          sequential_layer_scan=true)"""

    res = con.sql(sql, params=(osm_file, layer))

    while True:
        rec = res.fetchone()

        if rec is None:
            return

        yield rec


def lines(other_tags:dict, other_tags_no_subs:dict):
    category = 'other'

    if 'highway' in other_tags.keys():
        category = 'highway/%s' % other_tags['highway']
    elif 'place' in other_tags.keys():
        category = 'place/%s' % other_tags['place']
    elif 'indoor' in other_tags.keys():
        category = 'indoor/%s' % other_tags['indoor']
    elif 'power' in other_tags.keys() or \
         'cables' in other_tags.keys() or \
         'frequency' in other_tags.keys() or \
         'voltage' in other_tags.keys() or \
         'wires' in other_tags.keys() or \
         ('route' in other_tags.keys() and
          'power' in other_tags['route']):
        category = 'electric_cables'
    elif 'public_transport' in other_tags_no_subs.keys() or \
         'railway' in other_tags_no_subs.keys() or \
         ('route' in other_tags.keys() and
          other_tags['route'] in ('bus',
                                  'train',
                                  'tram',
                                  'trolleybus',
                                  'ferry',
                                  'railway')) or \
         'disused:route' in other_tags.keys() or \
         'was:route' in other_tags.keys():
        category = 'public_transport'
    elif 'seamark' in other_tags_no_subs.keys():
        category = 'seamark'
    elif 'waterway' in other_tags_no_subs.keys():
        category = 'water/way'
    elif 'diplomatic' in other_tags_no_subs.keys() or \
         'embassy' in other_tags_no_subs.keys():
        category = 'diplomatic'
    elif 'building' in other_tags.keys():
        if 'aeroway' in other_tags.keys():
            category = 'aeroway'
        elif 'historic' in other_tags.keys():
            category = 'building/historic'
        elif other_tags['building'] != 'yes' and \
             len(other_tags['building']) > 4:
            category = 'building/%s' % other_tags['building']
        else:
            category = 'building'
    elif 'barrier' in other_tags.keys():
        category = 'barrier'
    elif 'airspace' in other_tags.keys():
        category = 'airspace'
    elif 'museum' in other_tags.keys():
        category = 'museum'
    elif 'landcover' in other_tags.keys():
        category = 'landcover'
    elif 'aeroway' in other_tags.keys():
        category = 'aeroway'
    elif 'boundary' in other_tags.keys():
        category = 'boundary'
    elif 'traffic_calming' in other_tags.keys():
        category = 'traffic_calming'
    elif 'footway' in other_tags.keys():
        category = 'footway'
    elif 'amenity' in other_tags.keys():
        category = other_tags['amenity']
    elif 'landuse' in other_tags.keys():
        category = other_tags['landuse']
    elif 'water' in other_tags.keys():
        category = 'water/%s' % other_tags['water']
    elif 'natural' in other_tags.keys():
        if other_tags['natural'] == 'water':
            category = 'water/water'
        else:
            category = other_tags['natural']
    elif 'man_made' in other_tags.keys():
        category = other_tags['man_made']
    elif 'leisure' in other_tags.keys():
        category = other_tags['leisure']

    return category


def multilinestrings(other_tags:dict, other_tags_no_subs:dict):
    category = 'other'

    if 'cables' in other_tags.keys() or \
       'frequency' in other_tags.keys() or \
       'voltage' in other_tags.keys() or \
       'wires' in other_tags.keys() or \
       ('route' in other_tags.keys() and
        'power' in other_tags['route']):
        category = 'electric_cables'
    elif 'public_transport' in other_tags_no_subs.keys() or \
         ('route' in other_tags.keys() and
          other_tags['route'] in ('bus',
                                  'train',
                                  'tram',
                                  'trolleybus',
                                  'ferry',
                                  'railway')):
        category = 'public_transport'
    elif 'lanes' in other_tags.keys() or \
         'e-road' in other_tags.keys() or \
         ('route' in other_tags.keys() and
          'road' in other_tags['route']):
        category = 'roads'
    elif 'route' in other_tags.keys():
        category = other_tags['route']

    return category


def multipolygons(other_tags:dict, other_tags_no_subs:dict):
    category = 'other'

    if 'highway' in other_tags.keys():
        category = 'highway/%s' % other_tags['highway']
    elif 'area:highway' in other_tags.keys() or \
         'surface' in other_tags.keys() or \
         'traffic_calming' in other_tags.keys():
        category = 'highway/other'
    elif 'water' in other_tags.keys():
        category = 'water/%s' % other_tags['water']
    elif 'landuse' in other_tags.keys():
        category = other_tags['landuse']
    elif 'natural' in other_tags.keys():
        category = other_tags['natural']
    elif 'place' in other_tags.keys():
        category = other_tags['place']
    elif 'amenity' in other_tags.keys():
        category = other_tags['amenity']
    elif 'boundary' in other_tags.keys():
        category = 'boundary/%s' % other_tags['boundary']
    elif 'building' in other_tags_no_subs.keys():
        category = 'building'
    elif 'type' in other_tags.keys():
        category = other_tags['type']

    return category


def points(other_tags:dict, other_tags_no_subs:dict):
    category = 'other'

    if 'power' in other_tags.keys() or \
       'cables' in other_tags.keys() or \
       'frequency' in other_tags.keys() or \
       'voltage' in other_tags.keys() or \
       'wires' in other_tags.keys() or \
       ('route' in other_tags.keys() and
        'power' in other_tags['route']):
        category = 'electric_cables'
    elif 'natural' in other_tags.keys():
        category = 'natural/%s' % other_tags['natural']
    elif 'highway' in other_tags.keys():
        category = 'highway/%s' % other_tags['highway']
    elif 'traffic_calming' in other_tags.keys():
        category = 'highway/traffic_calming'
    elif 'amenity' in other_tags.keys():
        category = other_tags['amenity']
    elif 'disused:amenity' in other_tags.keys():
        category = other_tags['disused:amenity']
    elif 'shop' in other_tags.keys():
        category = 'shop/%s' % other_tags['shop']
    elif 'craft' in other_tags.keys():
        category = 'shop/%s' % other_tags['craft']
    elif 'disused:shop' in other_tags.keys():
        category = 'shop/%s' % other_tags['disused:shop']
    elif 'leisure' in other_tags.keys():
        category = 'leisure/%s' % other_tags['leisure']
    elif 'barrier' in other_tags.keys():
        category = 'barrier'
    elif 'crossing' in other_tags.keys():
        category = 'crossing'
    elif 'kerb' in other_tags.keys():
        category = 'kerb'
    elif 'playground' in other_tags.keys():
        category = 'playground'
    elif 'noexit' in other_tags.keys():
        category = 'noexit'
    elif 'emergency' in other_tags.keys():
        category = 'emergency'
    elif 'office' in other_tags.keys():
        category = 'office/%s' % other_tags['office']
    elif 'cuisine' in other_tags.keys():
        category = 'restaurant'
    elif 'railway' in other_tags.keys():
        category = 'railway'
    elif 'tourism' in other_tags.keys():
        category = 'tourism'
    elif 'entrance' in other_tags.keys():
        category = 'entrance'
    elif 'surveillance' in other_tags.keys():
        category = 'surveillance'
    elif 'historic' in other_tags.keys():
        category = 'historic'
    elif 'advertising' in other_tags.keys():
        category = 'advertising'
    elif 'man_made' in other_tags.keys():
        if other_tags['man_made'] == 'surveillance':
            category = 'surveillance'
        else:
            category = other_tags['man_made']
    elif 'public_transport' in other_tags_no_subs.keys():
        category = 'public_transport'
    elif 'place' in other_tags.keys():
        category = 'place/%s' % other_tags['place']

    return category


def other_relations(other_tags:dict, other_tags_no_subs:dict):
    if 'type' in other_tags_no_subs.keys():
        return other_tags_no_subs['type']

    return 'other'


@app.command()
def main(osm_file:  Annotated[str,
                              typer.Argument(
                                 help="GeoFabrik .osm.pbf filename")],
         geom_type: Annotated[str,
                              typer.Option(
                                help="line, multilinestring, "
                                     "multipolygon, points or "
                                     "other_relations")] = None,
         only_h3:   Annotated[str,
                              typer.Option(
                                  help="Comma-delimited H3 "
                                       "indices in 15-character "
                                       "hexadecimal form. Find IDs with "
                                       "https://what-the-h3index.vercel.app/")]
                                          = None,
         polygon_buildings: Annotated[bool,
                              typer.Option(
                                help="Buildings in LINESTRING or POINTS "
                                     "form will be converted "
                                     "into POLYGONS")] = False):
    h3_polys = []

    if only_h3:
        for h3_id in only_h3.split(','):
            poly_ = Polygon(list(h3.h3_to_geo_boundary(h3_id.strip())))

            # Form a closed polygon
            polygon = ['%s %s' % (y, x)
                       for x, y in poly_['coordinates'] +
                                   [poly_['coordinates'][0]]]
            h3_polys.append(
                shape(
                    wkt.loads('POLYGON((%s))' % ', '.join(polygon))))

    # Make sure there is an osmconf.ini file in the working folder.
    _osm_conf = 'osmconf.ini'

    if not exists(_osm_conf):
        open(_osm_conf, 'w').write('')

    categorisers = {
        'lines':            lines,
        'multilinestrings': multilinestrings,
        'multipolygons':    multipolygons,
        'points':           points,
        'other_relations':  other_relations}

    # WIP: Could these be run over multiple threads at the same time?
    # What would RAM consumption look like?
    for geom_type_ in categorisers.keys():
        if geom_type and (geom_type != geom_type_):
            continue

        file_handles = {}

        num_rows = num_rows_by_type(osm_file, geom_type_)

        for rec in track(get_geom_by_type(osm_file, geom_type_),
                         description='Categorising (%s)..' % geom_type_,
                         total=num_rows):
            geom = shape(wkt.loads(rec[1]))

            # WIP: Would it be more performant to filter in DuckDB instead?
            if only_h3 and not any(x.contains(geom.centroid)
                                   for x in h3_polys):
                continue

            other_tags         = ot_to_json(rec[0], remove_sub=False)
            other_tags_no_subs = ot_to_json(rec[0], remove_sub=True)
            category = categorisers[geom_type_](other_tags, other_tags_no_subs)

            geom_category = re.sub(r'[^a-zA-Z0-9\-\_\.\/]',
                                   '',
                                   category.replace('-', '_'))\
                                .lower()\
                                .strip('_')\
                                .strip('-')\
                                .strip()

            if len(geom_category) < 1:
                continue

            override_geom_type_ = None

            if polygon_buildings and \
               'building' in geom_category and \
               geom_type_ != 'multipolygons':
                geom = MultiPolygon(polygonize(geom))
                override_geom_type_ = 'multipolygons'

            basename = '%s/%s' % (geom_type_
                                  if not override_geom_type_
                                  else override_geom_type_,
                                  geom_category)

            folder = '/'.join(basename.split('/')[:-1])

            if not exists(folder):
                makedirs(folder)

            _geojson = '%s.geojson' % basename

            if _geojson not in file_handles.keys():
                if exists(_geojson):
                    unlink(_geojson)

                file_handles[_geojson] = open('%s.geojson' % basename, 'a')

            file_handles[_geojson].write(dumps(
                    Feature(geometry=geom,
                            properties={k: v
                                        for k, v in other_tags.items()
                                        if v and len(str(v))})) + '\n')

        for geojson_filename in track(file_handles.keys(),
                                      description=
                                        'GeoJSON to GPKG (%s)..' % geom_type_):
            file_handles[geojson_filename].close()
            basename = geojson_filename.replace('.geojson', '')

            if exists('%s.gpkg' % basename):
                unlink('%s.gpkg' % basename)

            cmd = 'ogr2ogr -f GeoJSON ' \
                  '%(to)s %(from)s' % {
                    'from': quote(geojson_filename),
                    'to':   quote(basename + '.gpkg')}

            try:
                execute(cmd)
            except Exception as exc:
                # WIP: free(): invalid pointer
                # I'm running a dodgy build from GDAL's main branch
                if 'invalid pointer' not in str(exc):
                    raise exc

            unlink('%s.geojson' % basename)


if __name__ == "__main__":
    app()
