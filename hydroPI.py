import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import geopandas as gpd
from shapely.geometry import Point, Polygon
from fiona.drvsupport import supported_drivers
from xml.dom.minidom import *
import json

#create a session
s = requests.session()

#set some configuration options
gpd.options.display_precision = 9

def get_aip():
    url = "http://pannes.hydroquebec.com/pannes/donnees/v3_0/aipversion.json"
    resp = s.get(url=url)
    AIP_VERSION = resp.json()
    return AIP_VERSION

def get_aip_markers(AIP_VERSION):
    url = f"http://pannes.hydroquebec.com/pannes/donnees/v3_0/aipmarkers{AIP_VERSION}.json"
    markers = s.get(url=url).json()
    return markers

#gets all the outages polygones from the kml file 
def get_aip_polys_geo_df(AIP_VERSION):
    url = f"http://pannes.hydroquebec.com/pannes/donnees/v3_0/aippoly{AIP_VERSION}.kmz"
    kmz = s.get(url=url, stream=True)
    geo_df = process_kmz_to_geo_df(kmz)
    return geo_df

#gets the bis number
def get_bis():
    url = "http://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
    resp = s.get(url=url)
    BIS_VERSION = resp.json()
    return BIS_VERSION

def get_bis_markers(BIS_VERSION):
    url = f"http://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{BIS_VERSION}.json"
    markers = s.get(url=url).json()
    return markers

#gets all the outages polygones from the kml file 
def get_bis_polys_geo_df(BIS_VERSION):
    url = f"http://pannes.hydroquebec.com/pannes/donnees/v3_0/bispoly{BIS_VERSION}.kmz"
    kmz = s.get(url=url, stream=True)

    geo_df = process_kmz_to_geo_df(kmz)

    return geo_df

def process_kmz_to_geo_df(kmz):

    #unzip the kmz file into a kml file
    kmz = ZipFile(BytesIO(kmz.content))
    kml = kmz.open(kmz.namelist()[0])

    #add kml to the supported drivers
    supported_drivers['KML'] = 'rw'
    supported_drivers['LIBKML'] = 'rw'

    #parse the kml file using an xml parser
    dom = parse(kml)

    #get all the polygons that form the outage map 
    placemarks = dom.getElementsByTagName("Placemark")

    entries = []
    for i in placemarks:

        #get the polygon coordinates
        coords = i.getElementsByTagName("coordinates")[0].firstChild.data

        #convert the string of polygon coordinates into a list of floats
        coords_int = [ [ float(n) for n in x.split(",")] for x in coords.split(" ")] 

        #locate the centroid tag in the xml document
        centroid_tag = [n  for n in i.getElementsByTagName("Data") if n.hasAttribute('name') and n.getAttribute('name') == 'centroid'][0]
        
        #get the centroid coordinates (returns string)
        centroid_raw = centroid_tag.getElementsByTagName("value")[0].firstChild.data

        #convert to list of floats 
        centroid_coord = [ float(n) for n in centroid_raw.strip('][').split(',')]

        #create a centroid point
        centroid = Point(centroid_coord)

        #create polygon 
        poly = Polygon(coords_int)

        #append a tupple of all the collected information into a list
        entries.append((centroid, centroid_coord, poly ,coords_int)) # List of tuples

    #create a datafram using the collected info 
    df = pd.DataFrame(entries, columns=('centroid','centroid_coord','polygon', 'poly_coords'))
 
    #convert the datafrom into a geodataframe so we can run geometric/mathematical opperations on the points and polygons 
    geod = gpd.GeoDataFrame(df, geometry=df.polygon, crs="EPSG:4326")

    #specify the "geomitry" column in the geodataframe 
    geod = geod.set_geometry("polygon").drop('geometry', axis=1)

    return geod

#recieves a numeric code and returns the cause in string form 
def resolve_outage_cause(code):

    #if code is not empty, set cause = code as default, and cast code to int. If code is empty str, set cause to Unkown
    if code is not None and code.strip():
        cause = str(code)
        code = int(code)
    else:
        cause = "Unknown"

    #avoid using a switch to keep compatibility with older python versions 
    if code in [11, 12, 13, 14, 58, 70, 72, 73, 74, 79]: cause = "Equipment failure"
    if code in [21, 22, 24, 25, 26]: cause = "Weather conditions"
    if code in [31, 32, 33, 34, 41, 42, 43, 44, 54, 55, 56, 57]: cause = "Accident or incident"
    if code in [52, 53]: cause = "Damage caused by an animal"
    if code == 51: cause = "Damage due to vegetation"
    if code == 9: cause = "System repair or improvement"


    return cause

#recieves a numeric code and returns the current status of the outage in string form 
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
def create_bis_markers_df(markers):
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

#creates a df of all the planned outages and the causes
def create_aip_markers_df(markers):
    markers_df = pd.DataFrame(markers, columns=['num_affected', 'notice_number', 'scheduled_start','scheduled_end','actual_start','actual_end','postponed_start','postponed_end','delayed_start','delayed_end','unkwn2','unkwn','cause_code','municipality_code', 'status_code','p_centroid_coord'])

    #centroid coordinates come in as a string. Converting to list of x,y coordinates
    markers_df['p_centroid_coord'] = markers_df['p_centroid_coord'].map(lambda x: [ float(n) for n in x.strip('][').split(',')])

    #create a centroid point using the x,y coordinates
    markers_df['centroid'] = markers_df['p_centroid_coord'].apply(lambda x: Point(x))

    #resolve the status codes
    markers_df['status'] = markers_df['status_code'].apply(lambda x: resolve_status_code(x))

    #resolve the cause codes
    markers_df['cause'] = markers_df['cause_code'].apply(lambda x: resolve_outage_cause(x))
    return markers_df

#returns a df of all the affected points
def get_affected_points(outage_df, points_df):

    #decresing the number of rows by removing the ones that don't have any relevance 
    mask = outage_df["polygon"].apply(lambda x: any(x.contains(i) for i in points_df['point']))
    marked = outage_df[mask]

    #merge all the points rows and outages rows using cartesian product
    merged = marked.merge(points_df, how='cross')

    #clean up the df and only leave rows where the given point is within a polygon. Otherwise the row is irrelevant 
    merged = merged.loc[lambda x:x['polygon'].contains( x['point'] ) ]
   
    return merged

#takes a json str and returns a df of points
def points_df_from_json(json_input):

    #load the json string 
    json_object = json.loads(json_input)

    #if the input is not a list 
    if not isinstance(json_object,list):

        #assuming this is a single point, which would be of type dict, wrapper in a list, otherwise throw an error 
        if isinstance(json_object, dict):
            json_object = [json_object]
        else: 
            raise Exception('The points provided are incorectly formatted. Please make sure the input is a list of objects.')

    #load the list into a dataframe 
    point_df = pd.DataFrame(json_object)

    #find any missing coordinates, and use the adress feild to determine the long/lat
    point_df = process_addresses(point_df)

    #convert the dataframe of points into a geodataframe 
    point_df = gpd.GeoDataFrame(point_df)

    #create a coordinates column 
    point_df['coord'] = point_df.apply( lambda x : [x['Longitude'], x['Latitude']], axis=1) 

    #create a calculated Point column 
    point_df['point'] = point_df.apply( lambda x : Point(x['coord']), axis=1) 

    #set the 'geometry' column in the df 
    point_df.set_geometry("point")
    point_df._geometry_column_name = "point"

    return point_df

def geocode_rate_limited(address_str):

    #initiate the geocoder 
    locator = Nominatim(user_agent="https")
    geocode = RateLimiter(locator.geocode, min_delay_seconds=1)

    #resolve the address 
    result = geocode(address_str)

    #if address could not be resolved throw an exception  
    if(result is None):
        raise Exception('No results found for ' + address_str + '. Make sure the format is correct.')
    
    #get the x,y,z coordinates as a list 
    point = list(result.point)

    #filter out the z axis 
    point_2d = point[0:2]

    #reverse the order of the longitude and latitude 
    point_2d.reverse()
    return point_2d

#if a point does not have the coordinates specified, the function tries to resolves the address to coordinates using geocoding 
def process_addresses(points_df1):
    points_df1[['Longitude', 'Latitude']] = points_df1.apply(lambda x: pd.Series([x['Longitude'] , x['Latitude']])  if (x['Longitude'] and x['Latitude']  ) else pd.Series(geocode_rate_limited(x['address'])) , axis=1)

    return points_df1

#returns a df with all the outages polygones (as well as the cause for aoutages)
def get_bis_polys_df(bis=None):
    #get info from hydro

    #if bis was not provided get one
    if not bis:
        bis = get_bis()

    #get the df of all the outages
    polys = get_bis_polys_geo_df(bis)

    #get the list of markers (unlike the kml file, markers give reasons for outages)
    markers = get_bis_markers(bis)

    #convert markers to df
    markers_df = create_bis_markers_df(markers=markers)

    #merge to one main
    main_df = polys.merge(markers_df, on='centroid', how='left')
    return main_df

#returns a df with all the planned outages polygones (as well as the cause for aoutages)
def get_aip_polys_df(aip=None):
    #get info from hydro

    #if aip was not provided get one
    if not aip:
        aip = get_aip()

    #get the df of all the outages
    polys = get_aip_polys_geo_df(aip)
    
    #get the list of markers (unlike the kml file, markers give reasons for outages)
    markers = get_aip_markers(aip)

    #convert markers to df
    markers_df = create_aip_markers_df(markers=markers)

    #merge to one main
    main_df = polys.merge(markers_df, on='centroid', how='left')
    return main_df

#a function for formatting the returned affected points 
def process_view(affected_pts_df):

    if 'postponed_start' in affected_pts_df.columns:
        return process_aip_view(affected_pts_df)
    else:
        return process_bis_view(affected_pts_df)



def process_bis_view(affected_pts_df):
    #filter out columns and leave only the ones we care about.
    df = affected_pts_df[['alais','address','start','end','num_affected','status','cause','municipality_code']]

    #convert to json 
    affected_points_json = df.to_json(orient = "records",force_ascii=False)

    return affected_points_json

def process_aip_view(affected_pts_df):
    #filter out columns and leave only the ones we care about.
    df = affected_pts_df[['alais','address','scheduled_start','scheduled_end','actual_start','actual_end','postponed_start','postponed_end','num_affected','status','cause','municipality_code']]
    
    #convert to json 
    affected_points_json = df.to_json(orient = "records",force_ascii=False)

    return affected_points_json
    
def get_ouatges(points_str):
    #get the points dataframe
    points_df = points_df_from_json(points_str)

    #merge to one main
    outage_df =  get_bis_polys_df() 

    #get a df of affected points
    affected_points_df = get_affected_points(outage_df, points_df)

    #format the df to a readable format
    affected_points_json = process_view(affected_points_df)
    return affected_points_json

    
def get_planned_interuptions(points_str):
    #get the points dataframe
    points_df = points_df_from_json(points_str)

    #get the planned interuptions df
    planned_outage_df =  get_aip_polys_df() 
    
    #get a df of affected points
    affected_points_df = get_affected_points(planned_outage_df, points_df)
    
    #format the df to a readable format
    affected_points_json = process_view(affected_points_df)
    return affected_points_json\


#run the code
#print(get_planned_interuptions(""))





















