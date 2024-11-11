from matplotlib import pyplot as plt
import pandas as pd
import pymysql
from config import *
import requests
import yaml
import csv
import osmnx as ox

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError

def hello_world():
    print("Hello from the data science library! (and Joel!)")

def download_price_paid_data(start_year, end_year):
    base_url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    # File name with placeholders
    file_name = "/pp-<year>-part<part>.csv"
    for year in range(start_year, end_year):
        print("Downloading data for year: " + str(year))
        for part in range(1,3):
            url = base_url + file_name.replace("<year>", str(year)).replace("<part>", str(part))
            response = requests.get(url)
            if response.status_code == 200:
                with open("." + file_name.replace("<year>", str(year)).replace("<part>", str(part)), "wb") as file:
                    file.write(response.content)

def create_connection(user, password, host, database, port=3306):
    """ Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database name
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database
                               )
        print(f"Connection established!")
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn

def housing_upload_join_data(conn, year):
    start_date = str(year) + "-01-01"
    end_date = str(year) + "-12-31"

    cur = conn.cursor()
    print('Selecting data for year: ' + str(year))
    cur.execute(f'SELECT pp.price, pp.date_of_transfer, po.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, po.country, po.latitude, po.longitude FROM (SELECT price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county FROM pp_data WHERE date_of_transfer BETWEEN "' + start_date + '" AND "' + end_date + '") AS pp INNER JOIN postcode_data AS po ON pp.postcode = po.postcode')
    rows = cur.fetchall()

    csv_file_path = 'output_file.csv'

    # Write the rows to the CSV file
    with open(csv_file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write the data rows
        csv_writer.writerows(rows)
    print('Storing data for year: ' + str(year))
    cur.execute(f"LOAD DATA LOCAL INFILE '" + csv_file_path + "' INTO TABLE `prices_coordinates_data` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '\"' LINES STARTING BY '' TERMINATED BY '\n';")
    conn.commit()
    print('Data stored for year: ' + str(year))

def get_box(latitude, longitude):
  box_height = 0.018
  box_width = 0.029
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
