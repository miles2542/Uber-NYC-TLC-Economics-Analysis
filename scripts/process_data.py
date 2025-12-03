import polars as pl
import numpy as np
import os
import glob
import time
import gc


# --- Configuration ---
RAW_DATA_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025"
OUTPUT_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\TLC_NYC_Processed"
WEATHER_FILE = r"./nyc_weather_hourly_2019_2025.csv"
ZONE_FILE = r"./taxi_zones_detailed.csv"
UBER_LICENSE = "HV0003"

# Performance Tuning
os.environ["POLARS_MAX_THREADS"] = "15"
pl.Config.set_streaming_chunk_size(300000)


# --- 1. Static Asset Loader ---
def load_static_assets():
    print("📦 Loading Static Assets (Zones & Weather)...")

    # Zones
    zones = pl.read_csv(ZONE_FILE).with_columns([
        pl.col("LocationID").cast(pl.Int32),
        pl.col("centroid_lat").cast(pl.Float32),
        pl.col("centroid_lon").cast(pl.Float32),
        pl.col("Borough").cast(pl.Categorical),
        pl.col("service_zone").cast(pl.Categorical),
    ])

    # Weather
    weather = pl.read_csv(WEATHER_FILE).with_columns([
        pl.col("datetime").str.to_datetime(),
        pl.col("temp").cast(pl.Float32),
        pl.col("feelslike").cast(pl.Float32),
        pl.col("precip").cast(pl.Float32),
        pl.col("snow").cast(pl.Float32),
        pl.col("snowdepth").cast(pl.Float32),
        pl.col("windspeed").cast(pl.Float32),
        pl.col("visibility").cast(pl.Float32),
        pl.col("conditions").cast(pl.Categorical),
    ])

    # Truncate weather to hour for joining
    weather = weather.with_columns(pl.col("datetime").dt.truncate("1h").alias("weather_match_time"))

    return zones, weather


# --- 2. The Feature Engineering Engine ---
def build_feature_pipeline(lf, zones, weather):
    # A. Pre-Processing & Casting
    schema_cols = lf.collect_schema().names()

    lf = lf.filter(pl.col("hvfhs_license_num") == UBER_LICENSE)

    # Define Flags to convert to 1/0 (UInt8)
    flag_cols = ["wav_request_flag", "wav_match_flag", "shared_request_flag", "shared_match_flag", "access_a_ride_flag"]
    flag_exprs = []
    for f in flag_cols:
        if f in schema_cols:
            # 'Y' -> 1, else 0. Cast to UInt8.
            flag_exprs.append(pl.when(pl.col(f).cast(pl.String) == "Y").then(1).otherwise(0).cast(pl.UInt8).alias(f))
        else:
            flag_exprs.append(pl.lit(0).cast(pl.UInt8).alias(f))

    lf = lf.with_columns(
        [
            # Cast IDs
            pl.col("PULocationID").cast(pl.Int32),
            pl.col("DOLocationID").cast(pl.Int32),
            # Cast Datetimes (Required for Service Metrics)
            pl.col("pickup_datetime").cast(pl.Datetime),
            pl.col("dropoff_datetime").cast(pl.Datetime),
            pl.col("request_datetime").cast(pl.Datetime),
            pl.col("on_scene_datetime").cast(pl.Datetime),
            # Cast Floats
            pl.col("trip_miles").cast(pl.Float32),
            pl.col("base_passenger_fare").cast(pl.Float32),
            pl.col("driver_pay").cast(pl.Float32),
            pl.col("tips").cast(pl.Float32),
            pl.col("tolls").cast(pl.Float32),
            pl.col("congestion_surcharge").fill_null(0).cast(pl.Float32),
            # Optional Financial Cols
            pl.col("airport_fee").fill_null(0).cast(pl.Float32)
            if "airport_fee" in schema_cols
            else pl.lit(0.0).cast(pl.Float32).alias("airport_fee"),
            pl.col("sales_tax").fill_null(0).cast(pl.Float32)
            if "sales_tax" in schema_cols
            else pl.lit(0.0).cast(pl.Float32).alias("sales_tax"),
            pl.col("bcf").fill_null(0).cast(pl.Float32)
            if "bcf" in schema_cols
            else pl.lit(0.0).cast(pl.Float32).alias("bcf"),
            pl.col("cbd_congestion_fee").fill_null(0).cast(pl.Float32)
            if "cbd_congestion_fee" in schema_cols
            else pl.lit(0.0).cast(pl.Float32).alias("cbd_congestion_fee"),
        ]
        + flag_exprs
    )

    # B. Geospatial Joins
    lf = lf.join(zones.lazy(), left_on="PULocationID", right_on="LocationID", how="left").rename({
        "Borough": "pickup_borough",
        "Zone": "pickup_zone",
        "centroid_lat": "pu_lat",
        "centroid_lon": "pu_lon",
    })
    lf = lf.join(zones.lazy(), left_on="DOLocationID", right_on="LocationID", how="left").rename({
        "Borough": "dropoff_borough",
        "Zone": "dropoff_zone",
        "centroid_lat": "do_lat",
        "centroid_lon": "do_lon",
    })

    # C. Core Physics & Time
    # 1. Calculate raw metrics
    lf = lf.with_columns([
        (pl.col("trip_miles") * 1.60934).alias("trip_km"),
        ((pl.col("dropoff_datetime") - pl.col("pickup_datetime")).dt.total_seconds()).alias("duration_seconds"),
        # Temporal Features
        pl.col("pickup_datetime").dt.hour().alias("pickup_hour"),
        pl.col("pickup_datetime").dt.day().alias("pickup_day"),
        pl.col("pickup_datetime").dt.month().alias("pickup_month"),
        pl.col("pickup_datetime").dt.year().alias("pickup_year"),
        pl.col("pickup_datetime").dt.weekday().alias("pickup_dow"),
        pl.col("pickup_datetime").dt.date().alias("pickup_date"),
        # Weather Match Key
        pl.col("pickup_datetime").dt.truncate("1h").alias("weather_match_time"),
    ])

    # 2. Service Metrics (Wait Times) with Paradox Cleaning
    # If the time is negative (Time Travel), we set it to NULL so it doesn't skew averages.
    wait_calc = (pl.col("pickup_datetime") - pl.col("request_datetime")).dt.total_seconds() / 60
    response_calc = (pl.col("on_scene_datetime") - pl.col("request_datetime")).dt.total_seconds() / 60
    boarding_calc = (pl.col("pickup_datetime") - pl.col("on_scene_datetime")).dt.total_seconds() / 60

    lf = lf.with_columns([
        pl.when(wait_calc < 0).then(None).otherwise(wait_calc).alias("total_wait_time_min"),
        pl.when(response_calc < 0).then(None).otherwise(response_calc).alias("driver_response_time_min"),
        pl.when(boarding_calc < 0).then(None).otherwise(boarding_calc).alias("boarding_time_min"),
    ])

    # D. Derived Physics (Speed, Dist, Bearing)
    # Helper Expressions for Radians
    pu_lat_rad = pl.col("pu_lat").radians()
    pu_lon_rad = pl.col("pu_lon").radians()
    do_lat_rad = pl.col("do_lat").radians()
    do_lon_rad = pl.col("do_lon").radians()
    dlon_rad = do_lon_rad - pu_lon_rad
    dlat_rad = do_lat_rad - pu_lat_rad

    lf = lf.with_columns([
        (pl.col("duration_seconds") / 60).alias("duration_min"),
        # Native Haversine Formula
        (
            6371
            * 2
            * ((dlat_rad / 2).sin().pow(2) + (pu_lat_rad.cos() * do_lat_rad.cos() * (dlon_rad / 2).sin().pow(2)))
            .sqrt()
            .arcsin()
        ).alias("straight_line_dist_km"),
        # Native Bearing Formula
        (
            pl.arctan2(
                y=(dlon_rad.sin() * do_lat_rad.cos()),
                x=(pu_lat_rad.cos() * do_lat_rad.sin()) - (pu_lat_rad.sin() * do_lat_rad.cos() * dlon_rad.cos()),
            ).degrees()
            % 360
        ).alias("bearing_degrees"),
    ])

    # E. Advanced Physics Derivatives
    lf = lf.with_columns([
        (pl.col("trip_km") / (pl.col("duration_seconds") / 3600)).alias("speed_kmh"),
        (pl.col("straight_line_dist_km") / (pl.col("duration_min") / 60)).alias("displacement_speed_kmh"),
        (pl.col("trip_km") / (pl.col("straight_line_dist_km") + 0.01)).alias("tortuosity_index"),
    ])

    # F. Economic Engine (Full Suite)
    # 1. Calculate Total Cost first (Base dependency)
    lf = lf.with_columns([
        (
            pl.col("base_passenger_fare")
            + pl.col("tolls")
            + pl.col("tips")
            + pl.col("congestion_surcharge")
            + pl.col("airport_fee")
            + pl.col("sales_tax")
            + pl.col("bcf")
            + pl.col("cbd_congestion_fee")
        ).alias("total_rider_cost"),
    ])

    # 2. Calculate Ratios & Derivatives
    lf = lf.with_columns([
        (pl.col("total_rider_cost") / (pl.col("trip_km") + 0.01)).alias("cost_per_km"),
        (pl.col("driver_pay") / (pl.col("base_passenger_fare") + 0.01)).alias("driver_revenue_share"),
        (1 - (pl.col("driver_pay") / (pl.col("base_passenger_fare") + 0.01))).alias("uber_take_rate_proxy"),
        (pl.col("driver_pay") / ((pl.col("duration_min") / 60) + 0.01)).alias("pay_per_hour"),
        (pl.col("tips") / (pl.col("base_passenger_fare") + 0.01)).alias("tipping_pct"),
        ((pl.col("tips") / (pl.col("base_passenger_fare") + 0.01)) > 0.25).cast(pl.UInt8).alias("is_generous_tip"),
    ])

    # 3. Calculate Dependent Flags (MUST be in a new block so driver_revenue_share exists)
    lf = lf.with_columns([(pl.col("driver_revenue_share") > 1.0).cast(pl.UInt8).alias("is_subsidized")])

    # G. Weather Join
    lf = lf.join(weather.lazy(), on="weather_match_time", how="left")
    lf = lf.with_columns([
        pl.col("precip").fill_null(0),
        pl.col("snow").fill_null(0),
        pl.col("snowdepth").fill_null(0),
        pl.col("temp").fill_null(pl.col("temp").mean()),
    ])

    # H. Cyclical Time (For ML)
    lf = lf.with_columns([
        (np.sin(2 * np.pi * pl.col("pickup_hour") / 24)).alias("cyclical_hour_sin"),
        (np.cos(2 * np.pi * pl.col("pickup_hour") / 24)).alias("cyclical_hour_cos"),
        (np.sin(2 * np.pi * pl.col("pickup_month") / 12)).alias("cyclical_month_sin"),
        (np.cos(2 * np.pi * pl.col("pickup_month") / 12)).alias("cyclical_month_cos"),
        (np.sin(2 * np.pi * pl.col("pickup_dow") / 7)).alias("cyclical_day_sin"),
        (np.cos(2 * np.pi * pl.col("pickup_dow") / 7)).alias("cyclical_day_cos"),
    ])

    # I. Categorical Engines

    # 1. Detailed Weather Categorization
    lf = lf.with_columns([
        # Rain Intensity
        pl.when(pl.col("precip") > 5.0)
        .then(pl.lit("heavy"))
        .when(pl.col("precip").is_between(1.0, 5.0))
        .then(pl.lit("moderate"))
        .when((pl.col("precip") > 0) & (pl.col("precip") < 1.0))
        .then(pl.lit("light"))
        .otherwise(pl.lit("none"))
        .alias("rain_intensity"),
        # Snow Intensity
        pl.when(pl.col("snow") > 20.0)
        .then(pl.lit("severe"))
        .when(pl.col("snow").is_between(10.0, 20.0))
        .then(pl.lit("heavy"))
        .when(pl.col("snow").is_between(2.5, 10.0))
        .then(pl.lit("moderate"))
        .when((pl.col("snow") > 0) & (pl.col("snow") < 2.5))
        .then(pl.lit("trace_light"))
        .otherwise(pl.lit("none"))
        .alias("snow_intensity"),
        # Wind Intensity
        pl.when(pl.col("windspeed") >= 62.0)
        .then(pl.lit("gale"))
        .when(pl.col("windspeed").is_between(40.0, 62.0))
        .then(pl.lit("windy"))
        .when(pl.col("windspeed").is_between(15.0, 40.0))
        .then(pl.lit("breezy"))
        .otherwise(pl.lit("calm"))
        .alias("wind_intensity"),
        # Visibility Status
        pl.when(pl.col("visibility") < 1.0)
        .then(pl.lit("poor_fog"))
        .when(pl.col("visibility").is_between(1.0, 10.0))
        .then(pl.lit("reduced"))
        .otherwise(pl.lit("clear"))
        .alias("visibility_status"),
    ])

    # 2. High-Level Weather State (Using derived categories)
    lf = lf.with_columns([
        pl.when(pl.col("snow_intensity") != "none")
        .then(pl.lit("snowing"))
        .when((pl.col("snow") == 0) & (pl.col("snowdepth") > 5))
        .then(pl.lit("snow_on_ground"))
        .when(pl.col("rain_intensity").is_in(["moderate", "heavy"]))
        .then(pl.lit("raining"))
        .otherwise(pl.lit("clear_cloudy"))
        .alias("weather_state"),
        # Boolean Flags (Updated Logic)
        # is_bad_weather: Rain >= Moderate OR Snow >= Trace OR Wind >= Windy OR Vis == Poor
        (
            (pl.col("rain_intensity").is_in(["moderate", "heavy"]))
            | (pl.col("snow_intensity") != "none")
            | (pl.col("wind_intensity").is_in(["windy", "gale"]))
            | (pl.col("visibility_status") == "poor_fog")
        )
        .cast(pl.UInt8)
        .alias("is_bad_weather"),
        # is_extreme_weather: Rain == Heavy OR Snow >= Heavy OR Wind == Gale
        (
            (pl.col("rain_intensity") == "heavy")
            | (pl.col("snow_intensity").is_in(["heavy", "severe"]))
            | (pl.col("wind_intensity") == "gale")
        )
        .cast(pl.UInt8)
        .alias("is_extreme_weather"),
        # Temp Bin
        pl.when(pl.col("temp") < 0)
        .then(pl.lit("freezing"))
        .when(pl.col("temp").is_between(0, 10))
        .then(pl.lit("cold"))
        .when(pl.col("temp").is_between(10, 20))
        .then(pl.lit("mild"))
        .when(pl.col("temp").is_between(20, 28))
        .then(pl.lit("warm"))
        .otherwise(pl.lit("hot"))
        .alias("temp_bin"),
    ])

    # 2. Cultural Day Type
    # Replaces `is_weekend`: if not "workday" then is weekend
    lf = lf.with_columns(
        pl.when((pl.col("pickup_dow") == 5) & (pl.col("pickup_hour") >= 17))
        .then(pl.lit("weekend_night"))
        .when((pl.col("pickup_dow") == 6) & (pl.col("pickup_hour") < 5))
        .then(pl.lit("weekend_night"))
        .when((pl.col("pickup_dow") == 6) & (pl.col("pickup_hour") >= 5))
        .then(pl.lit("weekend_day"))
        .when((pl.col("pickup_dow") == 7) & (pl.col("pickup_hour") < 5))
        .then(pl.lit("weekend_night"))
        .when((pl.col("pickup_dow") == 7) & (pl.col("pickup_hour") >= 5))
        .then(pl.lit("sunday_rest"))
        .when((pl.col("pickup_dow") == 1) & (pl.col("pickup_hour") < 6))
        .then(pl.lit("sunday_rest"))
        .otherwise(pl.lit("workday"))
        .alias("cultural_day_type")
    )

    # 3. Time of Day Bin
    lf = lf.with_columns(
        pl.when(pl.col("pickup_hour").is_between(6, 9))
        .then(pl.lit("morning_rush"))
        .when(pl.col("pickup_hour").is_between(10, 15))
        .then(pl.lit("midday"))
        .when(pl.col("pickup_hour").is_between(16, 19))
        .then(pl.lit("evening_rush"))
        .when(pl.col("pickup_hour").is_between(20, 22))
        .then(pl.lit("evening"))
        .otherwise(pl.lit("late_night"))
        .alias("time_of_day_bin")
    )

    # 4. Pandemic Phase
    lf = lf.with_columns(
        pl.when(pl.col("pickup_datetime") < pl.datetime(2020, 3, 1))
        .then(pl.lit("pre_pandemic"))
        .when(pl.col("pickup_datetime").is_between(pl.datetime(2020, 3, 1), pl.datetime(2020, 6, 1)))
        .then(pl.lit("lockdown"))
        .when(pl.col("pickup_datetime").is_between(pl.datetime(2020, 6, 1), pl.datetime(2021, 9, 1)))
        .then(pl.lit("recovery"))
        .otherwise(pl.lit("new_normal"))
        .alias("pandemic_phase")
    )

    # 5. Borough Flow & Trip Archetype & Zone Flow
    lf = lf.with_columns([
        pl.concat_str([pl.col("pickup_borough"), pl.lit(" -> "), pl.col("dropoff_borough")]).alias("borough_flow"),
        # Borough Transition Type
        pl.when((pl.col("pickup_borough") == "Manhattan") & (pl.col("dropoff_borough") == "Manhattan"))
        .then(pl.lit("manhattan_internal"))
        .when((pl.col("pickup_borough") == "Manhattan") | (pl.col("dropoff_borough") == "Manhattan"))
        .then(pl.lit("manhattan_outer_commute"))
        .when(pl.col("pickup_borough") != pl.col("dropoff_borough"))
        .then(pl.lit("outer_inter"))
        .otherwise(pl.lit("outer_intra"))
        .alias("borough_flow_type"),
        # Trip Zone Type
        pl.when(pl.col("PULocationID") == pl.col("DOLocationID"))
        .then(pl.lit("intra_zone"))
        .when(pl.col("pickup_borough") == pl.col("dropoff_borough"))
        .then(pl.lit("intra_borough"))
        .otherwise(pl.lit("inter_borough"))
        .alias("trip_type_zone"),
        # Archetype
        pl.when(pl.col("PULocationID").is_in([1, 132, 138]) | pl.col("DOLocationID").is_in([1, 132, 138]))
        .then(pl.lit("airport"))
        # Strict Commute: workday AND (morning rush OR evening rush)
        .when(
            (pl.col("cultural_day_type") == "workday")
            & (pl.col("time_of_day_bin").is_in(["morning_rush", "evening_rush"]))
        )
        .then(pl.lit("commute"))
        .when(pl.col("cultural_day_type") == "weekend_night")
        .then(pl.lit("nightlife"))
        .otherwise(pl.lit("leisure"))
        .alias("trip_archetype"),
    ])

    # J. The Great Filter (Smart Tips + Physics)
    lf = lf.filter(
        # 1. Physics & Locations
        (pl.col("trip_km").is_between(0.15, 120))
        & (pl.col("duration_seconds").is_between(60, 15000))
        & (pl.col("speed_kmh").is_between(1, 100))
        & (pl.col("PULocationID").is_between(1, 263))
        & (pl.col("DOLocationID").is_between(1, 263))
        # 2. Core Economics
        & (pl.col("base_passenger_fare").is_between(0.10, 300))
        & (pl.col("driver_pay").is_between(0.01, 200))
        # 3. Tax & Fee Caps (The missing pieces)
        & (pl.col("congestion_surcharge").is_between(0, 2.75))
        & (pl.col("tolls").is_between(0, 50))
        & (pl.col("sales_tax").is_between(0, 40))
        & (pl.col("bcf").is_between(0, 15))
        & (pl.col("airport_fee").is_between(0, 6))
        # 4. Smart Tip Filter: Tip <= 50 OR (Tip > 50 AND Ratio <= 4.0)
        & ((pl.col("tips") <= 50) | ((pl.col("tips") > 50) & (pl.col("tips") <= (pl.col("base_passenger_fare") * 4.0))))
        # Note: We do NOT filter cbd_congestion_fee (logic was "Leave as is")
    )

    # Drop Utility Columns
    cols_to_drop = [
        "weather_match_time",
        "service_zone",
        "service_zone_right",
        "pu_lat",
        "pu_lon",
        "do_lat",
        "do_lon",
        # Raw Weather (Replaced by Categories)
        "precip",
        "snow",
        "snowdepth",
        "windspeed",
        "visibility",
        "feelslike",
        # IDs (Noise, we're only filtering for Uber anyway)
        "hvfhs_license_num",
        "dispatching_base_num",
        "originating_base_num",
        # Redundant Time (We have Duration & Metrics)
        "request_datetime",
        "on_scene_datetime",
        "datetime",
        "trip_time",
        # Redundant Meta
        "icon",
        "trip_miles",
    ]

    # Safety check: Only drop columns that actually exist
    existing_cols = lf.collect_schema().names()
    final_drop_list = [c for c in cols_to_drop if c in existing_cols]

    lf = lf.drop(final_drop_list)

    return lf


# --- 3. Main Execution Loop ---
def main():
    print("🚀 Orion: Initializing Master ETL Pipeline...")
    zones, weather = load_static_assets()
    all_files = sorted(glob.glob(os.path.join(RAW_DATA_DIR, "*.parquet")))

    print(f"📂 Found {len(all_files)} raw files. Starting Processing...")

    for i, f in enumerate(all_files, 1):
        filename = os.path.basename(f)
        try:
            date_part = filename.split("_")[-1].replace(".parquet", "")
            yyyy, mm = date_part.split("-")
        except:
            continue

        target_dir = os.path.join(OUTPUT_DIR, f"year={yyyy}", f"month={mm}")
        target_file = os.path.join(target_dir, "data.parquet")

        if os.path.exists(target_file):
            print(f"[{i}/{len(all_files)}] ⏭️ Skipping {filename}")
            continue

        print(f"[{i}/{len(all_files)}] 🔨 Processing {filename}...", end="", flush=True)
        start_t = time.time()

        try:
            lf = pl.scan_parquet(f)
            lf_processed = build_feature_pipeline(lf, zones, weather)
            os.makedirs(target_dir, exist_ok=True)
            lf_processed.sink_parquet(target_file)

            print(f" Done ({time.time() - start_t:.1f}s).")
        except Exception as e:
            print(f" ❌ FAILED: {e}")

        gc.collect()


if __name__ == "__main__":
    main()
