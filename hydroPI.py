from pandas import DataFrame
import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon
from fiona.drvsupport import supported_drivers

from shapely.geometry import polygon
from xml.dom.minidom import *

#create a session
s = requests.session()

#set some configuration options
gpd.options.display_precision = 9

#gets the bis number
def get_bis():
    url = "http://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
    resp = s.get(url=url)
    BIS_VERSION = resp.json()
    return BIS_VERSION


def get_markers(BIS_VERSION):
    url = f"http://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{BIS_VERSION}.json"
    markers = s.get(url=url).json()
    return markers

#gets all the outages polygones from the kml file 
def get_polys_kml(BIS_VERSION, save_path = "./"):
    url = f"http://pannes.hydroquebec.com/pannes/donnees/v3_0/bispoly{BIS_VERSION}.kmz"
    kmz = s.get(url=url, stream=True)
    kmz = ZipFile(BytesIO(kmz.content))

    kml = kmz.open(kmz.namelist()[0])


    #kmz.extractall()

    supported_drivers['KML'] = 'rw'
    supported_drivers['LIBKML'] = 'rw'

    dom = parse(kml)
    placemarks = dom.getElementsByTagName("Placemark")

    coords = placemarks[0].getElementsByTagName("coordinates")[0].firstChild.data
    coords_int = [ [ float(i) for i in x.split(",")] for x in coords.split(" ")] 
    
    centroid_tag = [i  for i in placemarks[0].getElementsByTagName("Data") if i.hasAttribute('name') and i.getAttribute('name') == 'centroid'][0]
    centroid_raw = centroid_tag.getElementsByTagName("value")[0].firstChild.data
    centroid_coord = [ float(n) for n in centroid_raw.strip('][').split(',')]

    entries = []
    for i in placemarks:
        coords = i.getElementsByTagName("coordinates")[0].firstChild.data
        coords_int = [ [ float(n) for n in x.split(",")] for x in coords.split(" ")] 

        centroid_tag = [n  for n in i.getElementsByTagName("Data") if n.hasAttribute('name') and n.getAttribute('name') == 'centroid'][0]
        centroid_raw = centroid_tag.getElementsByTagName("value")[0].firstChild.data
        centroid_coord = [ float(n) for n in centroid_raw.strip('][').split(',')]
        #centroid_coord = centroid_tag.getElementsByTagName("value")[0].firstChild.data
        centroid = Point(centroid_coord)

        poly = Polygon(coords_int)
        entries.append((centroid, centroid_coord, poly ,coords_int)) # List of tuples

    df = pd.DataFrame(entries, columns=('centroid','centroid_coord','polygon', 'poly_coords'))
    #geod = gpd.GeoDataFrame(df, geometry=df.polygon, crs="EPSG:4326")
    geod = gpd.GeoDataFrame(df, geometry=df.polygon, crs="EPSG:4326")
    geod = geod.set_geometry("polygon").drop('geometry', axis=1)

    return geod
def resolve_outage_cause(code):

    #if code is not empty, set cause = code as default, and cast code to int. If code is empty str, set cause to Unkown
    if code is not None and code.strip():
        cause = str(code)
        code = int(code)
    else:
        cause = "Unknown"

    if code in [11, 12, 13, 14, 58, 70, 72, 73, 74, 79]: cause = "Equipment failure"
    if code in [21, 22, 24, 25, 26]: cause = "Weather conditions"
    if code in [31, 32, 33, 34, 41, 42, 43, 44, 54, 55, 56, 57]: cause = "Accident or incident"
    if code in [52, 53]: cause = "Damage caused by an animal"
    if code == 51: cause = "Damage due to vegetation"
    return cause

def resolve_status_code(code):

    #convert to upper case str to be safe
    code = str(code).upper()

    #by deefault, if status could not be resolved it is set to the original code
    status = code

    if code == 'A' : status = "Work assigned"
    if code == 'L' : status = "Crew at work"
    if code == 'R' : status = "Crew on the way"
    #if code == 'N' : status = "unkwn"

    return status 

#creates a df of all the outages and the causes
def create_markers_df(markers):
    markers_df = pd.DataFrame(markers['pannes'], columns=['num_affected', 'start','end','policy','p_centroid_coord','status_code','unkwn','cause_code','municipality_code','outage_msg'])

    #centroid coordinates come in as a string. Converting to list of x,y coordinates
    markers_df['p_centroid_coord'] = markers_df['p_centroid_coord'].map(lambda x: [ float(n) for n in x.strip('][').split(',')])

    #create a centroid point using the x,y coordinates
    markers_df['centroid'] = markers_df['p_centroid_coord'].apply(lambda x: Point(x))

    #resolve the status codes
    markers_df['status'] = markers_df['status_code'].apply(lambda x: resolve_status_code(x))

    #resolve the cause codes
    markers_df['cause'] = markers_df['cause_code'].apply(lambda x: resolve_outage_cause(x))
    return markers_df
'''
def get_affected_points(polys, markers, points):
    mask = polys["polygon"].apply(lambda x: any(x.contains(i) for i in points))
    indexes = polys["polygon"][mask].index.values.tolist()
    marked = [ markers['pannes'][index] for index in indexes ]
    return marked
'''
#returns a df of all the affected points
def get_affected_points(outage_df, points_df):

    #decresing the number of rows by removing the ones that don't have any relevance 
    mask = outage_df["polygon"].apply(lambda x: any(x.contains(i) for i in points_df['point']))
    marked = outage_df[mask]

    merged = marked.merge(points_df, how='cross')


    merged = merged.loc[lambda x:x['polygon'].contains( x['point'] ) ]
   
    return merged

#takes a json str and returns a df of points
def points_df_from_json(json_input):
    point_df = pd.read_json(json_input)

    #find any missing coordinates, and use the adress feild to determine the long/lat
    point_df = process_addresses(point_df)

    point_df = gpd.GeoDataFrame(point_df)
    #point_df['coord']  = point_df[['Longitude','Latitude']].values.tolist()
    point_df['coord'] = point_df.apply( lambda x : [x['Longitude'], x['Latitude']], axis=1) 
    point_df['point'] = point_df.apply( lambda x : Point(x['coord']), axis=1) 

    #points_df['point'] = gpd.GeoSeries(points_df['point'])
    point_df.set_geometry("point")
    point_df._geometry_column_name = "point"

    return point_df

def geocode_rate_limited(address_str):
    locator = Nominatim(user_agent="myGeocoder")
    geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
    result = geocode(address_str)

    if(result is None):
        raise Exception('No results found for ' + address_str + '. Make sure the format is correct.')
    point = result.point

    point = list(point)
    point_2d = point[0:2]
    point_2d.reverse()
    return point_2d

def process_addresses(points_df1):
    points_df1[['Longitude', 'Latitude']] = points_df1.apply(lambda x: pd.Series([x['Longitude'] , x['Latitude']])  if (x['Longitude'] and x['Latitude']  ) else pd.Series(geocode_rate_limited(x['address'])) , axis=1)

    return points_df1

#returns a df with all the outages polygones (as well as the cause for aoutages)
def get_polys_df(bis=None):
    #get info from hydro

    #if bis was not provided get one
    if not bis:
        bis = get_bis()

    #get the df of all the outages
    polys = get_polys_kml(bis)

    #get the list of markers (unlike the kml file, markers give reasons for outages)
    markers = get_markers(bis)


    #convert markers to df
    markers_df = create_markers_df(markers=markers)
    print(markers_df)
    #merge to one main
    main_df = polys.merge(markers_df, on='centroid', how='left')
    return main_df

def process_view(affected_pts_df):
    df = affected_pts_df[['alais','address','start','end','num_affected','status','cause','municipality_code']]
    print(affected_pts_df)
    print(df.to_json(orient = "records",force_ascii=False))
    
def get_ouatges(points_str):
    #get the points dataframe
    points_df = points_df_from_json(points_str)

    #merge to one main
    outage_df =  get_polys_df() 

    #get a df of affected points
    affected_points_df = get_affected_points(outage_df, points_df)

    #print(affected_points_df)
    affected_points_json = process_view(affected_points_df)
    return affected_points_json


p_str = '''
[
{
    "alais":"point 1",
    "address":"6845 27e Avenue, Montréal, QC ",
    "Longitude": -74.46115135970805,
    "Latitude": 45.9035602301897
},
{
    "alais":"point 2",
    "address":"2579 Rue Claudel, Montréal, QC ",
    "Longitude": -73.6182939,
    "Latitude": 45.7489449
},
{
    "alais":"point 3",
    "address":"2579 Rue Claudel, Montréal, QC ",
    "Longitude": "",
    "Latitude": 45.7489449
},

{
    "alais":"point 5",
    "address":"",
    "Longitude": -73.9168822,
    "Latitude":45.4977203
},
{
    "alais":"point 6",
    "address":"",
    "Longitude": -73.9174319,
    "Latitude":45.4982874
}
]
'''



print(get_ouatges(p_str))





































