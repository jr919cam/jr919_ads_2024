import pandas as pd
from .config import *
import osmnx as ox
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from . import access

"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""

def get_df_from_sql(table_name, username, password, url):
  engine = create_engine(f"mysql+pymysql://{username}:{password}@{url}:{3306}/ads_2024")
  query = 'SELECT * FROM ' + table_name 
  return pd.read_sql_query(query, engine)

def get_box(latitude, longitude, length):
  box_height = 0.018*length
  box_width = 0.029*length
  north = latitude + box_height/2
  south = latitude - box_height/2
  west = longitude - box_width/2
  east = longitude + box_width/2
  return north, south, west, east

def get_buildings_with_area(pois, has_full_address):
    if has_full_address:
      buildings_full_addr = pois[
        pois['building'].notna()
        & pois['addr:housenumber'].notna()
        & pois['addr:street'].notna()
        & pois['addr:postcode'].notna()
        & pois['addr:city'].notna()
        ]
      buildings_full_addr['area'] = buildings_full_addr.geometry.area
      return buildings_full_addr
    else:
      buildings_no_addr = pois[
          pois['building'].notna()
          & (
              pois['addr:housenumber'].isna()
              | pois['addr:street'].isna()
              | pois['addr:postcode'].isna()
              | pois['addr:city'].isna()
            )
            ]
      buildings_no_addr['area'] = buildings_no_addr.geometry.area
      return buildings_no_addr

def plotBuildings(latitude, longitude, place_name):
  tags = {
      "building": True,
      "addr:housenumber": True,
      "addr:street": True,
      "addr:postcode": True,
      "addr:city": True,
  }

  north, south, west, east = get_box(latitude, longitude)
  pois = ox.geometries_from_bbox(north, south, east, west, tags)
  graph = ox.graph_from_bbox(north, south, east, west)
  nodes, edges = ox.graph_to_gdfs(graph)
  area = ox.geocode_to_gdf(place_name)

  fig, ax = plt.subplots()

  area.plot(ax=ax, facecolor="white")

  # edges.plot(ax=ax, linewidth=1, edgecolor="dimgray")

  ax.set_xlim([west, east])
  ax.set_ylim([south, north])
  ax.set_xlabel("longitude")
  ax.set_ylabel("latitude")

  buildings_full_addr = get_buildings_with_area(pois, True)
  buildings_no_addr = get_buildings_with_area(pois, False)

  buildings_full_addr.plot(ax=ax, color="blue", alpha=0.7, markersize=10)
  buildings_no_addr.plot(ax=ax, color="grey", alpha=0.7, markersize=10)
  plt.tight_layout()

def get_pcd_joined_df(lat, long, postcode_start, conn):
  lat_km_in_degs = 0.009
  long_km_in_degs = 0.014

  query1 = "SELECT pp.price, pp.date_of_transfer, po.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.primary_addressable_object_name ,pp.town_city, pp.district, pp.county, po.country, po.latitude, po.longitude FROM"
  query2 = f" (SELECT price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, primary_addressable_object_name, town_city, district, county FROM pp_data WHERE date_of_transfer BETWEEN '2020-01-01' AND '2024-12-31' and postcode LIKE '{postcode_start}%') AS pp INNER JOIN"
  query3 = f" (SELECT * FROM postcode_data where latitude between {lat} - {lat_km_in_degs} and {lat} + {lat_km_in_degs} and longitude between {long} - {long_km_in_degs} and {long} + {long_km_in_degs} and postcode LIKE '{postcode_start}%') AS po"
  query4 = " ON pp.postcode = po.postcode "

  query = query1+query2+query3+query4
  return pd.read_sql_query(query, conn)

def get_merged_df(pp_buildings_df, pois):
  pp_buildings_df
  osm_buildings_df = pois[pois['building'].notna()]
  osm_buildings_df['area'] = osm_buildings_df.geometry.area
  merged_df = pd.merge(
      pp_buildings_df,
      osm_buildings_df,
      left_on=['primary_addressable_object_name', 'postcode'],
      right_on=['addr:housenumber', 'addr:postcode'],
      how='outer',
      indicator=True
  )
  unmatched_pp = merged_df[(merged_df['_merge'] == 'left_only')]
  unmatched_osm = merged_df[(merged_df['_merge'] == 'right_only') & (merged_df['addr:postcode'].notna())]
  both = merged_df[merged_df['_merge'] == 'both']
  print("\nnum matches:", len(both), "| num unmatched pp:", len(unmatched_pp), "| num unmatched osm:", len(unmatched_osm),'\n')
  return merged_df

def count_pois_near_coordinates(latitude: float, longitude: float, tags: dict, distance_km: float = 1.0) -> dict:
    """
    Count Points of Interest (POIs) near a given pair of coordinates within a specified distance.
    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        tags (dict): A dictionary of OSM tags to filter the POIs (e.g., {'amenity': True, 'tourism': True}).
        distance_km (float): The distance around the location in kilometers. Default is 1 km.
    Returns:
        dict: A dictionary where keys are the OSM tags and values are the counts of POIs for each tag.
    """
    pois = ox.geometries_from_point((latitude, longitude), tags=tags, dist=distance_km*1000)

    pois_count = {}

    for tag in tags.keys():
      if tag in pois.columns:
        pois_count[tag] = pois[tag].notnull().sum()
      else:
        pois_count[tag] = 0

    return pois_count