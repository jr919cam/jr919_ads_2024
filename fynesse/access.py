import json
from matplotlib import pyplot as plt
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from config import *
import requests
import osmium
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

class TestError(Exception):
    def __init__(self):
        super().__init__()

class OSMHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.nodes = []


    def node(self, n):
        self.nodes.append([n.id, n.location.lat, n.location.lon, dict(n.tags)])


def full_england_osm_to_df():
    handler = OSMHandler()
    handler.apply_file("england-latest.osm.pbf")
    nodes_df = pd.DataFrame(handler.nodes, columns=["id", "latitude", "longitude", "tags"])

    # filter out points without tags
    nodes_df = nodes_df[nodes_df['tags'] != {}].reset_index()

    nodes_df['tags'] = nodes_df['tags'].apply(lambda x: json.dumps(x))
    return nodes_df

def upload_full_england_osm(username, password, url):
    node_df = full_england_osm_to_df()
    engine = create_engine(f"mysql+pymysql://{username}:{password}@{url}/{'ads_2024'}")
    node_df.to_sql(name='osm_england_nodes', con=engine, if_exists='replace')
