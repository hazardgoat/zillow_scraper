import copy
from datetime import datetime, timedelta
import geopandas as gpd
import httpx
import json
import os
import pandas as pd
from parsel import Selector
from pytz import timezone
import requests
from requests.exceptions import HTTPError
from shapely.geometry import Point


pd.set_option("display.max_columns", None)


### CONSTANTS ###
GOOGLE_API_KEY = <SOME API STRING>

MAX_MONTHLY_PRICE = 6000
MIN_BEDS = 4
MIN_BATHS = 2
LAUNDRY = True
AC = True
MIN_SQ_FT = 2000

# zillow search url
LISTING_URL = f"https://www.zillow.com/new-york-ny/rentals/?" \
              f"searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22mapBounds%22%3A%7B%22" \
              f"north%22%3A41.06799488951523%2C%22" \
              f"south%22%3A40.32564014061633%2C%22" \
              f"east%22%3A-73.26419638085937%2C%22" \
              f"west%22%3A-74.69516561914062%7D%2C%22" \
              f"regionSelection%22%3A%5B%7B%22" \
              f"regionId%22%3A6181%2C%22" \
              f"regionType%22%3A6%7D%5D%2C%22" \
              f"isMapVisible%22%3Atrue%2C%22" \
              f"filterState%22%3A%7B%22" \
              f"mapZoom%22%3A11%2C%22" \
              f"usersSearchTerm%22%3A%22Portland%20OR%22%2C%22" \
              f"fore%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"lau%22%3A%7B%22value%22%3A{str(LAUNDRY).lower()}%7D%2C%22" \
              f"ah%22%3A%7B%22value%22%3Atrue%7D%2C%22" \
              f"auc%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"nc%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"fr%22%3A%7B%22value%22%3Atrue%7D%2C%22" \
              f"fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"ac%22%3A%7B%22value%22%3A{str(AC).lower()}%7D%2C%22" \
              f"mp%22%3A%7B%22max%22%3A{str(MAX_MONTHLY_PRICE)}%7D%2C%22" \
              f"price%22%3A%7B%22max%22%3A778217%7D%2C%22" \
              f"beds%22%3A%7B%22min%22%3A{str(MIN_BEDS)}%7D%2C%22" \
              f"baths%22%3A%7B%22min%22%3A{str(MIN_BATHS)}%7D%2C%22" \
              f"apco%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"apa%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"con%22%3A%7B%22value%22%3Afalse%7D%2C%22" \
              f"sqft%22%3A%7B%22min%22%3A{str(MIN_SQ_FT)}%7D%7D%2C%22" \
              f"isListVisible%22%3Atrue%7D"

# output directory
OUTPUT_DIR = <SOME DIRECTORY PATH STRING>

# User
ADDRESS = "City Hall Park, New York, NY 10007"
TRANSIT_MODE = "transit"
MAX_TRANSIT_TIME = 60
DEPARTURE_HOUR = 17
USER_CONSTANTS = {'address': ADDRESS, 'transit_mode': TRANSIT_MODE, 'max_transit_time':MAX_TRANSIT_TIME,
                 'departure_hour': DEPARTURE_HOUR}


def parse_listings(url: str) -> dict:
    """
    Parses the Zillow listing page HTML and extracts out a DataFrame of listings

    Args:
        url: Zillow listings url
    
    Returns:
        listings_df: DataFrame of Zillow listings data
    """
    BASE_HEADERS = {"accept-language": "en-US,en;q=0.9",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                    "accept-encoding": "gzip, deflate, br",}

    with httpx.Client(http2=True, headers=BASE_HEADERS, follow_redirects=True) as client:
        resp = client.get(url)
    sel = Selector(text=resp.text)
    data = json.loads(sel.css("script#__NEXT_DATA__::text").get())

    columns = ["Latitude", "Longitude", "Address", "URL", "Date Available", "Price", "Beds", "Baths", "Laundry", "AC"]
    listings_df = pd.DataFrame(columns=columns)
    for key, value in data.items():
        if isinstance(value, dict):
            try:
                listings = value['pageProps']['searchPageState']['cat1']['searchResults']['listResults']
                for i in listings:
                    try:
                        house_data = {"Latitude": [i['latLong']['latitude']],
                                      "Longitude": [i['latLong']['longitude']],
                                      "Address": [i['address']],
                                      "URL": [i['detailUrl']],
                                      "Date Available": [i['availabilityDate']],
                                      "Price": [i['unformattedPrice']],
                                      "Beds": [i['beds']],
                                      "Baths": [i['baths']],
                                      "Laundry": [LAUNDRY],
                                      "AC": [AC]}
                    except Exception as e:
                        house_data = {"Latitude": [None],
                                      "Longitude": [None],
                                      "Address": [None],
                                      "URL": [None],
                                      "Date Available": [None],
                                      "Price": [None],
                                      "Beds": [None],
                                      "Baths": [None],
                                      "Laundry": [None],
                                      "AC": [None]}
                    house_df = pd.DataFrame(house_data)
                    listings_df = pd.concat([listings_df, house_df], ignore_index=True)
            except Exception as e:
                pass
    
    listings_df = listings_df.dropna(how='all').reset_index(drop=True)

    return listings_df


def get_transit_time(api_key: str, departure_address: str, arrival_address: str, transit_mode: str,
                     departure_time: int) -> int:
    """
    Finds the transit time for each user from work to home at the specified time

    Args:
        api_key: Google maps API key
        departure_address: address acting as starting point
        arrival_address: address acting as ending point
        transit_mode: mode of transit to use for calculation (e.g. driving, transit)
        departure_time: departure time in the PST timezone
    
        Returns:
            duration_in_minutes: travel time in minutes
    """
    base_url = "https://maps.googleapis.com/maps/api/directions/json?"

    # Defining the parameters of the search
    params = {
        "origin": departure_address,
        "destination": arrival_address,
        "mode": transit_mode,
        "key": api_key,
        "departure_time": str(departure_time)  # API expects a string
    }

    # Sending the request to the API and storing the response
    response = requests.get(base_url, params=params)
    
    # Check the status of the request
    if response.status_code == 200:
        # Parse the response JSON
        data = response.json()
        # Extract the travel time from the response
        if data["routes"]:
            route = data["routes"][0]  # Assuming we want the first (best) route
            leg = route["legs"][0]  # A route can have one or more legs depending on whether waypoints were provided
            duration = leg["duration"]  # Duration of the trip

            # Convert travel time from seconds to minutes
            duration_in_minutes = int(duration['value'] / 60)

            # Return duration in minutes
            return duration_in_minutes

        else:
            print(f"No route found between {departure_address} and {arrival_address}.")
            return None
    else:
        print("Error with the transit time request.")
        return None


def export_as_shapefile(df: pd.DataFrame, output_dir: str):
    """
    Exports DataFrame as ESRI shapefile for viewing is geospacial applications

    Args:
        df: DataFrame of filtered Zillow listings
    
    Outputs:
        ESRI shapefile
    """
    # Convert DataFrame to GeoDataFrame
    geometry = [Point(xy) for xy in zip(df['Longitude'], df['Latitude'])]
    filtered_listings_gdf = gpd.GeoDataFrame(df, geometry=geometry)

    # Specify CRS
    filtered_listings_gdf.set_crs(epsg=4326, inplace=True)

    output_file_path = os.path.join(output_dir, "filtered_zillow_listings.shp")
    filtered_listings_gdf.to_file(output_file_path)


def filter_zillow_listings(google_api_key: str, url: str, user_constants: dict, output_dir: str):
    """
    Constructs a filtered DataFrame of Zillow listings that match the requirments and outputs them as a shapefile

    Args:
        google_api_key: Google API key
        url: url of Zillow search
        user_constants: dictionary of constants for each user (e.g. Jen, Christine, Erin)
        output_dir: directory to write out shapefile of listings to

    Outputs:
        ESRI Shapefile of filtered Zillow listings
    """
    # get listings
    listings_df = parse_listings(url=url)

    filtered_listings_df = copy.deepcopy(listings_df)
    for idx, row in listings_df.iterrows():
        # home address
        home_address = str(row.Address)

        listing_url = "https://www.zillow.com" + str(row.URL)
        filtered_listings_df.at[idx, 'detailUrl'] = listing_url

        # Get transit time
        departure_address = user_constants['address']
        arrival_address = home_address
        transit_mode = user_constants['transit_mode']
        max_transit_time = user_constants['max_transit_time']
        departure_hour = user_constants['departure_hour']

        # Note: this code assumes that it's run before the minimum departure hour PST on the day of travel
        pst = timezone('America/Los_Angeles')

        departure_time = datetime.now(pst).replace(hour=departure_hour, minute=0, second=0, microsecond=0) \
                         + timedelta(days=1)

        # Convert the departure time to a timestamp (seconds since the Unix epoch)
        departure_time = int(departure_time.timestamp())

        transit_time = get_transit_time(api_key=google_api_key, departure_address=departure_address,
                                        arrival_address=arrival_address, transit_mode=transit_mode,
                                        departure_time=departure_time)

        if isinstance(transit_time, int):
            if transit_time <= max_transit_time:
                filtered_listings_df.at[idx, f'Minutes Transit'] = transit_time
            else:
                try:
                    filtered_listings_df.drop(idx, inplace=True)
                    print(f"Dropping {row.Address} at index {idx} because transit time = {transit_time}")
                except Exception as e:
                    pass

    export_as_shapefile(df=filtered_listings_df, output_dir=output_dir)


if __name__ == "__main__":
    filter_zillow_listings(google_api_key=GOOGLE_API_KEY, url=LISTING_URL, user_constants=USER_CONSTANTS,
                           output_dir=OUTPUT_DIR)
