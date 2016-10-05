# Find the closest station to downtown

from sqlalchemy import create_engine
from shapely import wkb
import requests
import rasterio
from rasterio.mask import mask
import copy
import os

from shapely.geometry import MultiPolygon, shape
from rasterio.features import shapes
from shapely.geometry import mapping, shape
from shapely.ops import cascaded_union
import numpy as np
import pandas as pd
import json
import os
from pyspatial.raster import read_catalog
from pyspatial.vector import from_series
# import matplotlib.pyplot as plt
from rasterio import plot
from rasterio.transform import from_bounds

import pickle
from multiprocessing import Pool, TimeoutError



def processitem(placeid, sorig):

    POSTGRESURI = 'postgresql://urbis:urbis@ontoserv:5434/urbisdata01'
    engine = create_engine(POSTGRESURI)

    earthenvtable = 'urbanclusters.earthenv_urbannamed'


    print "doing the deepcopy"
    s = copy.deepcopy(sorig)



    GETGEOM = """
        SELECT ST_AsEWKB(geom), ST_AsEWKB(ST_Transform(geom, 4326)) as wgs84geom,
        ST_AsEWKB(ST_Transform(ST_Difference(
        ST_Buffer(geom, sqrt(St_Area(geom)/pi())*2)
        , geom), 4326)) AS ruralgeomwgs84,
        ST_AsEWKB(ST_Difference(
        ST_Buffer(geom, sqrt(St_Area(geom)/pi())*2)
        , geom)) AS ruralgeom
        FROM {0}
        WHERE placeid={1}
        """.format(earthenvtable, placeid)
    print "running earthenv"
    r = engine.execute(GETGEOM)
    firstitem = r.first()
    if firstitem:
        
        s["earthenv"] = {
            'geom': wkb.loads(str(firstitem[0])),
            'wgs84': wkb.loads(str(firstitem[1])),
            'ruralgeom': wkb.loads(str(firstitem[3])),
            'ruralgeomwgs84': wkb.loads(str(firstitem[2])),
        }




    GETGEOM = """
        SELECT 
        ST_AsEWKB(censusurb.geom) as geom, ST_AsEWKB(ST_Transform(censusurb.geom, 4326)) as geomwgs84 ,
        ST_AsEWKB(ST_Transform(ST_Difference(
        ST_Buffer(censusurb.geom, 50000)
        , fullgeom.geom), 4326)) AS ruralgeomwgs84,
        ST_AsEWKB(ST_Difference(
        ST_Buffer(censusurb.geom, 50000)
        , fullgeom.geom)) AS ruralgeom
        FROM urbanclusters.usgscities as usgscities, urbanclusters.censusdabbagemsa as censusurb, urbanclusters.censusurbangroup as fullgeom
        WHERE ST_Contains(censusurb.geom, usgscities.geom) AND usgscities."gnis_id" = {0}
        """.format(s['gnisid'])
    print "running the census msa geo tools with", s['gnisid']
    r = engine.execute(GETGEOM)
    firstitem = r.first()
    if firstitem:
        try:
            s["censusurb"] = {
                'geom': wkb.loads(str(firstitem[0])),
                'wgs84': wkb.loads(str(firstitem[1])),
                'ruralgeom': wkb.loads(str(firstitem[3])),
                'ruralgeomwgs84': wkb.loads(str(firstitem[2])),
            }
        except Exception,e:
            print e
            print placeid
            print s['usgsplacename']
            print s['gnisid']

    print "loading the dem.tif file"
    demfile = "/Volumes/UrbisBackup/rasterstorage/dem/dem.tif"
    demsrc = rasterio.open(demfile)
    profile = demsrc.profile

    print "loading the dem.json file"
    dempyspatialpath = "/Volumes/UrbisBackup/rasterstorage/dem/dem.json"
    dempyspatial = read_catalog(dempyspatialpath)




    print "doing", s[u'usgsplacename']
    if s['censusurb'].get('processbuffer', False):
        return (placeid, s, )
    try:

        buffergeom = mapping(s['censusurb']['ruralgeom'])

        try:
            os.mkdir('scratch')
        except:
            pass

        #get average elevation of the urbangeom
        urbanvl = from_series(pd.Series([s['censusurb']['geom']]))
        urbanelevationresult = dempyspatial.query(urbanvl) 
        r = urbanelevationresult.next()
        indexc = np.argwhere(r.values > -9000)
        newv =  np.take(r.values, indexc)
        neww =  np.take(r.weights, indexc)
        urbmeanelevation = float((newv * neww).sum() / neww.sum())

        # now calculate the buffer section
        print "doing the mask operation"
        result = mask(demsrc, [buffergeom], nodata=None, crop=True, all_touched=False, invert=False)
        z = np.where((result[0] > urbmeanelevation - 50) & (result[0] < urbmeanelevation + 50), 1, 0)
        p = z[0].astype('uint8')


        w,south, e, n = list(s['censusurb']['ruralgeom'].bounds)
        bufferaffine = from_bounds(w,south,e,n, z[0].shape[0], z[0].shape[1])



        with rasterio.open('scratch/rgb_no_ndv_{0}.tif'.format(placeid), 'w', driver=profile['driver'], 
                           width=z[0].shape[0], height=z[0].shape[1], count=1, dtype='uint8', nodata=0,
                          affine=bufferaffine, crs=profile['crs']) as dst:
            dst.write(p, 1)



        with rasterio.open('scratch/rgb_no_ndv_{0}.tif'.format(placeid), 'r') as newsrc:
            elevationoutput = newsrc.read(1)
            tempgeoms = []
            for shaper in shapes(elevationoutput, mask= elevationoutput==1, transform=bufferaffine):
                tempgeoms.append(shaper)

        multipolygons = MultiPolygon([shape(g[0]) for g in tempgeoms])

        s['censusurb']['processbuffer'] = cascaded_union(multipolygons)
    except Exception,e:
        print e
        print "---------"

    return (placeid, s,)

def main():

    POSTGRESURI = 'postgresql://urbis:urbis@ontoserv:5434/urbisdata01'
    engine = create_engine(POSTGRESURI)

    SELECTPLACES = """
    SELECT 
    (array_agg(earthenv.placeid ORDER BY usgscities."pop_2010" DESC))[1] AS placeid,
    (array_agg(usgscities.name ORDER BY usgscities."pop_2010" DESC))[1] AS usgsplacename,
    (array_agg(ST_AsEWKB(ST_Transform(usgscities.geom, 4326)) ORDER BY usgscities."pop_2010" DESC))[1] AS usgsplacegeomwgs84str,
    (array_agg(ST_AsEWKB(usgscities.geom) ORDER BY usgscities."pop_2010" DESC))[1] AS usgsplacegeomstr,
    (array_agg(usgscities."pop_2010"  ORDER BY usgscities."pop_2010" DESC))[1] AS usgspopulation,
    (array_agg(usgscities.countyfips  ORDER BY usgscities."pop_2010" DESC))[1] AS countryfips,
    (array_agg(usgscities."state_fips"  ORDER BY usgscities."pop_2010" DESC))[1] AS statefips,
    (array_agg(usgscities."gnis_id"  ORDER BY usgscities."pop_2010" DESC))[1] AS gnisid
    FROM urbanclusters.usgscities as usgscities, 
    urbanclusters.earthenv_urbannamed as earthenv
    WHERE ST_Intersects(usgscities.geom, earthenv.geom) AND usgscities."gnis_id" > 0
    GROUP BY earthenv.placeid
    ORDER BY usgspopulation DESC
    LIMIT 100 """

    placeresult = engine.execute(SELECTPLACES)

    sampleplaces = {}

    for row in placeresult:
        rowdict = dict(row)
        rowdict['usgsplacegeom'] = wkb.loads(str(rowdict["usgsplacegeomstr"]))
        rowdict['usgsplacegeomwgs84'] = wkb.loads(str(rowdict["usgsplacegeomwgs84str"]))
        sampleplaces[rowdict['placeid']] = rowdict



    results = {}
    pool = Pool(processes=8)
    jobs = []
    
    for placeid, s in sampleplaces.iteritems():
        if os.path.exists('Dabbage/pickles/{0}.pickle'.format(placeid)):
            continue
        s[u'usgsplacegeomstr'] = str(s[u'usgsplacegeomstr'])
        s[u'usgsplacegeomwgs84str'] = str(s[u'usgsplacegeomwgs84str'])

        # try:
        #     print "starting on ", s['usgsplacename']
        #     newplaceid, news = processitem(placeid, s)
        # except Exception,e:
        #     print e
        # else:
        #     with open('Dabbage/pickles/{0}.pickle'.format(newplaceid), 'wb') as fout:
        #         pickle.dump(news, fout)


        jobs.append(pool.apply_async(processitem, [placeid, s]))

    try:
        os.mkdir("Dabbage/pickles")
    except:
        pass
    for job in jobs:
        newplaceid, news = job.get()
        with open('Dabbage/pickles/{0}.pickle'.format(newplaceid), 'wb') as fout:
            pickle.dump(news, fout)


    # s = None
    # placeid = None
    # s = None
    # for k, i in sampleplaces.iteritems():
    #     if i[u'usgsplacename'] == "Chicago":
    #         s = i
    #         placeid = k
    #         break



    # job = pool.apply_async(processitem, [placeid, s])
    # newplaceid, news = job.get()
    # print newplaceid




    pool.close()

            





if __name__ == '__main__':
    main()




