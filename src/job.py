import sys
import os
import requests
import pygeohash as pgh
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, row_number, lit, concat_ws
from pyspark.sql.types import StringType, DoubleType, StructType, StructField
from pyspark.sql.window import Window

# Load environment variables
load_dotenv()

# --- Geohash Utils ---

def generate_geohash(lat, lng, precision=4):
    """
    Generate a geohash from latitude and longitude.
    Returns None if lat or lng are None.
    """
    if lat is None or lng is None:
        return None
    try:
        # Ensure inputs are floats
        return pgh.encode(float(lat), float(lng), precision=precision)
    except (ValueError, TypeError):
        return None

# --- Geocoding Utils ---

class GeocodingService:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.opencagedata.com/geocode/v1/json"
        self._cache = {}

    def get_coordinates(self, address):
        """
        Fetch coordinates for a given address string.
        Returns (latitude, longitude) or (None, None) if not found/error.
        Uses internal cache to avoid duplicate calls.
        """
        if not address or not isinstance(address, str):
            return None, None
            
        if address in self._cache:
            return self._cache[address]

        if not self.api_key:
            return None, None

        params = {
            'q': address,
            'key': self.api_key,
            'limit': 1,
            'no_annotations': 1
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            if data['results']:
                geometry = data['results'][0]['geometry']
                lat = geometry['lat']
                lng = geometry['lng']
                self._cache[address] = (lat, lng)
                return lat, lng
            else:
                self._cache[address] = (None, None)
                return None, None

        except requests.RequestException as e:
            print(f"API Request failed: {e}")
            return None, None

_geocoding_service = None

def get_lat_lng(street, city, country, api_key):
    global _geocoding_service
    if _geocoding_service is None:
        _geocoding_service = GeocodingService(api_key)
    
    # Construct address safely
    # Note: street might be None if data doesn't have it
    parts = [p for p in [street, city, country] if p]
    address = ", ".join(parts)
    
    return _geocoding_service.get_coordinates(address)

# --- Spark Job ---

def create_spark_session(app_name="RestaurantWeatherETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

def read_data(spark, path, fmt="csv", header=True, inferSchema=True):
    # For parquet, header/inferSchema options are ignored/irrelevant usually
    return spark.read.format(fmt) \
        .option("header", header) \
        .option("inferSchema", inferSchema) \
        .load(path)

def enrich_data(df, api_key):
    """
    Separates valid/invalid coordinates, enriches invalid ones, and recombines.
    """
    # Cast lat/lng to double first
    df = df.withColumn("lat", col("lat").cast(DoubleType())) \
           .withColumn("lng", col("lng").cast(DoubleType()))

    # 1. Split Data
    valid_df = df.filter(col("lat").isNotNull() & col("lng").isNotNull())
    invalid_df = df.filter(col("lat").isNull() | col("lng").isNull())
    
    if invalid_df.count() == 0:
        return valid_df

    # 2. Define UDF
    schema = StructType([
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True)
    ])

    def enrich_row(street, city, country):
        lat, lng = get_lat_lng(street, city, country, api_key)
        return (lat, lng)

    enrich_udf = udf(enrich_row, schema)

    # Handle missing 'street' column if it doesn't exist in dataframe
    street_col = col("street") if "street" in df.columns else lit(None)

    # 3. Apply Enrichment
    enriched_invalid_df = invalid_df.withColumn("coords", enrich_udf(street_col, col("city"), col("country"))) \
        .withColumn("lat", col("coords.lat")) \
        .withColumn("lng", col("coords.lng")) \
        .drop("coords")

    # 4. Union
    final_df = valid_df.unionByName(enriched_invalid_df)
    return final_df

def add_geohash(df):
    """
    Adds geohash_4 column.
    """
    # Ensure lat/lng are doubles (redundant if enrich_data ran, but safe)
    df = df.withColumn("lat", col("lat").cast(DoubleType())) \
           .withColumn("lng", col("lng").cast(DoubleType()))

    geohash_udf = udf(lambda lat, lng: generate_geohash(lat, lng, precision=4), StringType())
    
    return df.withColumn("geohash_4", geohash_udf(col("lat"), col("lng")))

def prepare_weather_data(weather_df):
    """
    1. Generate geohash for weather data using lat/lng.
    2. Deduplicates weather data by geohash, keeping the latest timestamp.
    """
    # Weather data has lat/lng. Needs geohash.
    weather_df = add_geohash(weather_df)
    
    # Deduplicate. Use 'wthr_date' as timestamp if 'timestamp' missing.
    # The actual schema has 'wthr_date'.
    ts_col = "timestamp" if "timestamp" in weather_df.columns else "wthr_date"
    
    windowSpec = Window.partitionBy("geohash_4").orderBy(col(ts_col).desc())
    
    deduped_df = weather_df.withColumn("rn", row_number().over(windowSpec)) \
        .filter(col("rn") == 1) \
        .drop("rn")
        
    return deduped_df

def main():
    spark = create_spark_session()
    
    input_restaurants = os.getenv("INPUT_RESTAURANTS_PATH", "data/restaurants")
    input_weather = os.getenv("INPUT_WEATHER_PATH", "data/weather")
    output_path = os.getenv("OUTPUT_PATH", "output/enriched_data")
    api_key = os.getenv("OPENCAGE_API_KEY")

    print("Reading data...")
    try:
        # Restaurants are CSV
        restaurants_df = read_data(spark, input_restaurants, fmt="csv")
        # Weather is Parquet (partitioned)
        weather_df = read_data(spark, input_weather, fmt="parquet")
    except Exception as e:
        print(f"Error reading data: {e}")
        sys.exit(1)

    # Validate & Enrich Restaurants
    print("Enriching restaurant data...")
    enriched_restaurants = enrich_data(restaurants_df, api_key)
    
    # Add Geohash to Restaurants
    print("Generating geohashes for restaurants...")
    restaurants_with_geohash = add_geohash(enriched_restaurants)
    
    # Prepare Weather
    print("Preparing weather data...")
    weather_unique = prepare_weather_data(weather_df)
    
    # Rename weather columns to avoid collision if necessary, or just selecting useful ones
    # We want to keep temperature. In new schema it is 'avg_tmpr_c' or 'avg_tmpr_f'.
    # Let's clean up weather columns before join
    weather_final = weather_unique.select(
        col("geohash_4"),
        col("avg_tmpr_c").alias("temperature_c"),
        col("avg_tmpr_f").alias("temperature_f"),
        col("wthr_date")
    )
    
    # Join
    print("Joining datasets...")
    joined_df = restaurants_with_geohash.join(
        weather_final, 
        on="geohash_4", 
        how="left"
    )
    
    # Write Output
    print(f"Writing output to {output_path}...")
    joined_df.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .parquet(output_path)
        
    print("Job completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()