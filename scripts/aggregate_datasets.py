import polars as pl
import os
import glob
import time
import gc

# --- Configuration ---
# CHANGE THIS PATH to switch between Processed vs Raw input
INPUT_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025"
# INPUT_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025 - Processed"

# Output will be saved here
OUTPUT_BASE = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025 - Aggregates"

# Tuning
os.environ["POLARS_MAX_THREADS"] = "14"
# pl.Config.set_streaming_chunk_size(100000)


def enforce_schema(lf, is_processed):
    """
    Ensures types match across all files before aggregation.
    """
    schema = lf.collect_schema().names()
    casts = []

    # 1. Cast Floats (Fix Float64 vs Float32 mismatch)
    float_cols = [
        "cbd_congestion_fee",
        "airport_fee",
        "base_passenger_fare",
        "driver_pay",
        "tips",
        "tolls",
        "trip_miles",
    ]

    if is_processed:
        float_cols.extend([
            "total_rider_cost",
            "trip_km",
            "speed_kmh",
            "cost_per_km",
            "driver_revenue_share",
            "total_wait_time_min",
            "driver_response_time_min",
            "boarding_time_min",
        ])

    for col in float_cols:
        if col in schema:
            casts.append(pl.col(col).cast(pl.Float32))
        elif col == "cbd_congestion_fee":
            casts.append(pl.lit(0.0).cast(pl.Float32).alias("cbd_congestion_fee"))

    # 2. Cast Categoricals/Strings
    cat_cols = [
        "pickup_borough",
        "dropoff_borough",
        "weather_state",
        "trip_archetype",
        "borough_flow_type",
        "time_of_day_bin",
        "rain_intensity",
        "snow_intensity",
        "wind_intensity",
        "visibility_status",
    ]
    for col in cat_cols:
        if col in schema:
            casts.append(pl.col(col).cast(pl.String))

    # 3. Cast Integers (Fix Int32 vs Int64 mismatch in Raw Data) -- NEW BLOCK
    int_cols = ["PULocationID", "DOLocationID"]
    for col in int_cols:
        if col in schema:
            casts.append(pl.col(col).cast(pl.Int32))

    if casts:
        lf = lf.with_columns(casts)

    return lf


def process_single_file(file_path, is_processed):
    """
    Calculates the 4 Marts for a SINGLE file and returns 4 tiny DataFrames.
    """
    try:
        lf = pl.scan_parquet(file_path)
        lf = enforce_schema(lf, is_processed)
        schema = lf.collect_schema().names()

        # --- Fallback for Raw Data (Time Generation) ---
        # Only generate if missing (Processed files have them, Raw files don't)
        time_exprs = []
        if "pickup_year" not in schema:
            time_exprs.append(pl.col("pickup_datetime").dt.year().alias("pickup_year"))
        if "pickup_month" not in schema:
            time_exprs.append(pl.col("pickup_datetime").dt.month().alias("pickup_month"))
        if "pickup_day" not in schema:
            time_exprs.append(pl.col("pickup_datetime").dt.day().alias("pickup_day"))
        if "pickup_hour" not in schema:
            time_exprs.append(pl.col("pickup_datetime").dt.hour().alias("pickup_hour"))
        if "pickup_date" not in schema:
            time_exprs.append(pl.col("pickup_datetime").dt.date().alias("pickup_date"))

        if time_exprs:
            lf = lf.with_columns(time_exprs)

        # --- MART 1: Timeline (Hourly) ---
        keys_1 = ["pickup_year", "pickup_month", "pickup_day", "pickup_hour"]

        aggs_1 = [
            pl.len().alias("trip_count"),
            pl.col("base_passenger_fare").sum().alias("total_fare_amt"),
            pl.col("driver_pay").sum().alias("total_driver_pay"),
            pl.col("cbd_congestion_fee").sum().alias("total_cbd_fee"),
        ]

        if is_processed:
            keys_1.extend(["borough_flow_type", "trip_archetype", "cultural_day_type"])
            aggs_1.extend([
                pl.col("total_rider_cost").sum().alias("total_revenue_gross"),
                pl.col("tips").sum().alias("total_tips"),
                pl.col("trip_km").mean().alias("avg_trip_km"),
                pl.col("speed_kmh").mean().alias("avg_speed_kmh"),
                pl.col("is_bad_weather").sum().alias("bad_weather_count"),
                pl.col("is_extreme_weather").sum().alias("extreme_weather_count"),
            ])
        else:
            # Fallback for Raw
            aggs_1.append(pl.col("trip_miles").mean().alias("avg_trip_miles"))

        df_1 = lf.group_by(keys_1).agg(aggs_1).collect()

        # --- MART 2: Network (Monthly) ---
        keys_2 = ["pickup_year", "pickup_month", "PULocationID", "DOLocationID"]
        aggs_2 = [pl.len().alias("trip_count")]

        if is_processed:
            if "pickup_borough" in schema:
                keys_2.extend(["pickup_borough", "dropoff_borough"])

            aggs_2.extend([
                pl.col("duration_min").mean().alias("avg_duration_min"),
                pl.col("total_rider_cost").mean().alias("avg_cost"),
                pl.col("displacement_speed_kmh").mean().alias("avg_displacement_speed"),
                # NEW: Service Metrics
                pl.col("total_wait_time_min").mean().alias("avg_wait_time"),
                pl.col("driver_response_time_min").mean().alias("avg_driver_response"),
            ])
        else:
            if "trip_time" in schema:
                aggs_2.append(pl.col("trip_time").mean().alias("avg_duration_sec"))

        df_2 = lf.group_by(keys_2).agg(aggs_2).collect()

        # --- MART 3: Economic (Processed Only) ---
        df_3 = None
        if is_processed:
            keys_3 = ["pickup_date", "time_of_day_bin", "weather_state", "borough_flow_type"]
            aggs_3 = [
                pl.len().alias("trip_count"),
                pl.col("driver_revenue_share").mean().alias("avg_driver_share"),
                pl.col("driver_revenue_share").std().alias("std_driver_share"),
                pl.col("uber_take_rate_proxy").mean().alias("avg_take_rate"),
                pl.col("tipping_pct").mean().alias("avg_tip_pct"),
                pl.col("pay_per_hour").mean().alias("avg_hourly_wage"),
                pl.col("base_passenger_fare").median().alias("median_fare"),
                pl.col("base_passenger_fare").quantile(0.90).alias("p90_fare_surge_proxy"),
                # Weather Intensity Check
                pl.col("rain_intensity").mode().first().alias("dominant_rain"),
            ]
            df_3 = lf.group_by(keys_3).agg(aggs_3).collect()

        # --- MART 4: Executive (Daily) ---
        keys_4 = ["pickup_date"]
        aggs_4 = [
            pl.len().alias("total_trips"),
            pl.col("base_passenger_fare").sum().alias("total_fare_revenue"),
        ]

        if is_processed:
            aggs_4.extend([
                pl.col("total_rider_cost").sum().alias("total_gross_booking_value"),
                pl.col("tips").sum().alias("total_tips"),
                pl.col("trip_km").sum().alias("total_km_traveled"),
                pl.col("is_bad_weather").sum().alias("bad_weather_trip_count"),
                pl.col("is_extreme_weather").sum().alias("extreme_weather_trip_count"),
                pl.col("total_wait_time_min").mean().alias("avg_wait_time"),
            ])
        else:
            aggs_4.append(pl.col("trip_miles").mean().alias("avg_distance_miles"))

        df_4 = lf.group_by(keys_4).agg(aggs_4).collect()

        return df_1, df_2, df_3, df_4

    except Exception as e:
        print(f"❌ Error processing {os.path.basename(file_path)}: {e}")
        import traceback

        traceback.print_exc()
        return None, None, None, None


def main():
    print(f"🚀 Orion: Initializing Atomic Data Mart Generation...")
    print(f"📂 Input: {INPUT_DIR}")

    files = sorted(glob.glob(os.path.join(INPUT_DIR, "**", "*.parquet"), recursive=True))
    if not files:
        print("❌ No files found.")
        return

    # Detect Mode
    lf_sample = pl.scan_parquet(files[0])
    sample_cols = lf_sample.collect_schema().names()
    is_processed = "trip_archetype" in sample_cols
    mode_label = "Processed" if is_processed else "Raw"
    print(f"🧠 Operation Mode: {mode_label.upper()}")

    output_dir = os.path.join(OUTPUT_BASE, f"Aggregates_{mode_label}")
    os.makedirs(output_dir, exist_ok=True)

    # Storage for accumulation
    mart1_list = []
    mart2_list = []
    mart3_list = []
    mart4_list = []

    start_total = time.time()

    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Aggregating {os.path.basename(f)}...", end="", flush=True)
        st = time.time()

        d1, d2, d3, d4 = process_single_file(f, is_processed)

        if d1 is not None:
            mart1_list.append(d1)
            mart2_list.append(d2)
            if d3 is not None:
                mart3_list.append(d3)
            mart4_list.append(d4)
            print(f" Done ({time.time() - st:.1f}s)")
        else:
            print(" Skipped.")

        # CRITICAL: Free RAM immediately
        gc.collect()

    print("\n🔗 Concatenating and Saving Final Marts...")

    # Save Mart 1
    if mart1_list:
        print("   -> Saving Timeline Backbone...")
        pl.concat(mart1_list).write_parquet(os.path.join(output_dir, "agg_timeline_hourly.parquet"))
        del mart1_list
        gc.collect()

    # Save Mart 2
    if mart2_list:
        print("   -> Saving Network Backbone...")
        pl.concat(mart2_list).write_parquet(os.path.join(output_dir, "agg_network_monthly.parquet"))
        del mart2_list
        gc.collect()

    # Save Mart 3
    if mart3_list:
        print("   -> Saving Economic Backbone...")
        pl.concat(mart3_list).write_parquet(os.path.join(output_dir, "agg_pricing_distribution.parquet"))
        del mart3_list
        gc.collect()

    # Save Mart 4
    if mart4_list:
        print("   -> Saving Executive Summary...")
        pl.concat(mart4_list).sort("pickup_date").write_csv(os.path.join(output_dir, "agg_executive_daily.csv"))
        del mart4_list
        gc.collect()

    print(f"\n✅ Success! Total Time: {(time.time() - start_total) / 60:.2f} min")


if __name__ == "__main__":
    main()
