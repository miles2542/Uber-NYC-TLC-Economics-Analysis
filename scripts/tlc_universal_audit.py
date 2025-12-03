import polars as pl
import os
import glob
import time
import gc

# --- Configuration ---
# Change this to point to Raw OR Processed OR Sample folder
INPUT_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025"
OUTPUT_FILE = "TLC_Universal_Audit_Report_Raw.csv"

# Tuning
os.environ["POLARS_MAX_THREADS"] = "15"


def build_audit_expressions(schema):
    """
    Dynamically builds expressions based on what columns exist (Raw vs Processed).
    """
    exprs = [pl.len().alias("total_rows")]
    cols = set(schema)

    # 1. Numerical Deep Dives (Metric Agnostic)
    # We look for ANY of these potential columns
    targets = [
        "trip_miles",
        "trip_km",
        "trip_time",
        "duration_seconds",
        "duration_min",
        "base_passenger_fare",
        "driver_pay",
        "tips",
        "tolls",
        "congestion_surcharge",
        "airport_fee",
        "bcf",
        "cbd_congestion_fee",
        "sales_tax",
        "speed_kmh",
        "displacement_speed_kmh",
        "tortuosity_index",
        "total_rider_cost",
        "cost_per_km",
        "driver_revenue_share",
        "pay_per_hour",
        "total_wait_time_min",
        "driver_response_time_min",
    ]

    for col in targets:
        if col in cols:
            exprs.extend([
                pl.col(col).null_count().alias(f"{col}_nulls"),
                (pl.col(col) == 0).sum().alias(f"{col}_zeros"),
                (pl.col(col) < 0).sum().alias(f"{col}_negatives"),
                pl.col(col).mean().alias(f"{col}_mean"),
                pl.col(col).std().alias(f"{col}_std"),
                pl.col(col).min().alias(f"{col}_min"),
                pl.col(col).quantile(0.01).alias(f"{col}_p01"),
                pl.col(col).quantile(0.50).alias(f"{col}_p50"),
                pl.col(col).quantile(0.99).alias(f"{col}_p99"),
                pl.col(col).quantile(0.999).alias(f"{col}_p99.9"),
                pl.col(col).max().alias(f"{col}_max"),
            ])

    # 2. Categorical & Flag Checks
    cat_targets = [
        "weather_state",
        "trip_archetype",
        "borough_flow_type",
        "is_bad_weather",
        "is_extreme_weather",
        "is_generous_tip",
    ]
    for col in cat_targets:
        if col in cols:
            exprs.append(pl.col(col).null_count().alias(f"{col}_nulls"))
            # For flags, count the '1's
            if "is_" in col:
                exprs.append(pl.col(col).sum().alias(f"{col}_count_true"))

    # 3. Paradox Checks (Context Aware)

    # A. Teleportation (Distance vs Time)
    if "trip_km" in cols and "duration_min" in cols:
        # Processed Logic
        exprs.append(((pl.col("trip_km") > 5) & (pl.col("duration_min") < 1)).sum().alias("paradox_teleport_count"))
    elif "trip_miles" in cols and "trip_time" in cols:
        # Raw Logic
        exprs.append(((pl.col("trip_miles") > 2) & (pl.col("trip_time") < 60)).sum().alias("paradox_teleport_count"))

    # B. Slave Labor (Work without Pay)
    if "driver_pay" in cols:
        if "trip_km" in cols:
            exprs.append(
                ((pl.col("trip_km") > 2) & (pl.col("driver_pay") <= 0)).sum().alias("paradox_slave_labor_count")
            )
        elif "trip_miles" in cols:
            exprs.append(
                ((pl.col("trip_miles") > 1) & (pl.col("driver_pay") <= 0)).sum().alias("paradox_slave_labor_count")
            )

    # C. Time Travel (Dropoff before Pickup)
    if "pickup_datetime" in cols and "dropoff_datetime" in cols:
        exprs.append((pl.col("dropoff_datetime") < pl.col("pickup_datetime")).sum().alias("paradox_time_travel_count"))

    return exprs


def process_file(file_path):
    try:
        lf = pl.scan_parquet(file_path)

        # --- 1. DETECT & STANDARDIZE TO METRIC (For Apples-to-Apples Comparison) ---
        # We peek at the schema to see if we are in "Raw Mode"
        raw_schema = lf.collect_schema().names()

        virtual_cols = []

        # If Raw (has miles, missing km), create virtual KM column
        if "trip_miles" in raw_schema and "trip_km" not in raw_schema:
            virtual_cols.append((pl.col("trip_miles") * 1.60934).alias("trip_km"))

        # If Raw (has miles+time, missing speed), create virtual Speed column
        if "trip_miles" in raw_schema and "trip_time" in raw_schema and "speed_kmh" not in raw_schema:
            # Speed = (Miles * 1.6) / (Seconds / 3600)
            # We stick to simple math here just for auditing distributions
            speed_expr = (pl.col("trip_miles") * 1.60934) / (pl.col("trip_time") / 3600)
            virtual_cols.append(speed_expr.fill_nan(0).fill_null(0).alias("speed_kmh"))

        # Apply the virtual columns
        if virtual_cols:
            lf = lf.with_columns(virtual_cols)

        # --- 2. REFRESH SCHEMA ---
        # Now that we added columns, we get the schema again so the Audit Engine sees them
        schema = lf.collect_schema().names()

        # --- 3. GROUPING LOGIC (Unchanged) ---
        if "pickup_month" not in schema:
            # Raw Data: Create month from datetime
            lf = lf.with_columns([pl.col("pickup_datetime").dt.truncate("1mo").cast(pl.Date).alias("audit_month")])
            group_key = "audit_month"
        elif "pickup_date" in schema:
            # Processed Data: Truncate existing date
            lf = lf.with_columns(pl.col("pickup_date").dt.truncate("1mo").alias("audit_month"))
            group_key = "audit_month"
        else:
            # Fallback
            lf = lf.with_columns(pl.col("pickup_datetime").dt.truncate("1mo").cast(pl.Date).alias("audit_month"))
            group_key = "audit_month"

        # --- 4. BUILD EXPRESSIONS ---
        exprs = build_audit_expressions(schema)

        # --- 5. AGGREGATE ---
        df = lf.group_by(group_key).agg(exprs).collect()

        # --- 6. TYPE SAFETY (Unchanged) ---
        casts = []
        for col in df.columns:
            if col == group_key:
                continue
            if any(x in col for x in ["_nulls", "_zeros", "_negatives", "_count", "total_rows"]):
                casts.append(pl.col(col).cast(pl.Int64))
            elif any(x in col for x in ["_mean", "_std", "_min", "_max", "_p01", "_p50", "_p99"]):
                casts.append(pl.col(col).cast(pl.Float64))

        if casts:
            df = df.with_columns(casts)

        return df

    except Exception as e:
        print(f"❌ Error auditing {os.path.basename(file_path)}: {e}")
        return None


def main():
    print(f"🚀 Orion: Initializing Universal Audit...")
    print(f"📂 Target: {INPUT_DIR}")

    files = sorted(glob.glob(os.path.join(INPUT_DIR, "**", "*.parquet"), recursive=True))
    print(f"🔍 Found {len(files)} files.")

    results = []
    start_t = time.time()

    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Scanning {os.path.basename(f)}...", end="", flush=True)
        res = process_file(f)
        if res is not None:
            results.append(res)
            print(" Done.")

        gc.collect()

    if results:
        print("\n🔗 Compiling Report...")
        # Use diagonal concat to handle slight schema variations if any
        final_df = pl.concat(results, how="diagonal")

        # Sort by date
        if "audit_month" in final_df.columns:
            final_df = final_df.sort("audit_month")

        # Reorder: Date, Rows, Paradoxes... then the rest
        cols = final_df.columns
        priority = ["audit_month", "total_rows"] + [c for c in cols if "paradox" in c]
        rest = [c for c in cols if c not in priority]

        final_df = final_df.select(priority + rest)

        final_df.write_csv(OUTPUT_FILE)
        print(f"✅ Audit Report Saved: {OUTPUT_FILE}")
        print(f"⏱️ Time: {(time.time() - start_t) / 60:.2f} min")

        # Quick Summary Print
        print("\n--- QUICK SUMMARY ---")
        print(f"Total Rows Audited: {final_df['total_rows'].sum():,.0f}")
        for p in [c for c in cols if "paradox" in c]:
            print(f"{p}: {final_df[p].sum():,.0f}")


if __name__ == "__main__":
    main()
