import polars as pl
import os
import glob
import time
import gc

# ==============================================================================
# ⚙️ CONFIGURATION
# ==============================================================================
INPUT_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025 - Processed"
OUTPUT_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025 - Samples"

# Fraction of data to keep (0.01 = 1%, 0.10 = 10%)
SAMPLE_FRACTION = 0.01

# Options: "yearly", "monthly", "single"
SPLIT_MODE = "yearly"

# Seed for reproducibility (Ensures you get the exact same sample every time)
RANDOM_SEED = 105

# Performance
os.environ["POLARS_MAX_THREADS"] = "14"
# ==============================================================================


class SamplingEngine:
    def __init__(self, mode, output_dir):
        self.mode = mode
        self.output_dir = output_dir
        self.buffer = []
        self.current_group_key = None  # Tracks Year or Filename depending on mode

        os.makedirs(output_dir, exist_ok=True)

    def flush(self):
        """Writes the current buffer to disk and clears memory."""
        if not self.buffer:
            return

        print(f"   💾 Flushing buffer to disk ({len(self.buffer)} chunks)...", end="")

        # Concatenate buffer
        df_chunk = pl.concat(self.buffer)

        # Determine Filename
        if self.mode == "single":
            fname = "tlc_sample_full.parquet"
        elif self.mode == "yearly":
            fname = f"tlc_sample_{self.current_group_key}.parquet"
        elif self.mode == "monthly":
            fname = f"tlc_sample_{self.current_group_key}.parquet"

        out_path = os.path.join(self.output_dir, fname)

        # For 'single' mode, we might need to append if memory is tight,
        # but for 1-10% samples, writing once at the end (or checking existing) is safer.
        # Here, if file exists and we are in 'yearly' loop (unlikely with logic below), we overwrite.

        df_chunk.write_parquet(out_path)
        print(f" Saved: {fname} ({len(df_chunk):,} rows)")

        # cleanup
        self.buffer = []
        gc.collect()

    def add_chunk(self, df, key):
        """Adds data to buffer. Flushes if the group key changes (e.g. Year changes)."""

        # Initialize key on first run
        if self.current_group_key is None:
            self.current_group_key = key

        # Logic for switching groups
        if self.mode == "yearly" and key != self.current_group_key:
            print(f"   🔄 Year transition ({self.current_group_key} -> {key}).")
            self.flush()
            self.current_group_key = key

        elif self.mode == "monthly":
            self.current_group_key = key
            self.buffer.append(df)
            self.flush()  # Monthly flushes immediately
            return

        # Add to buffer
        self.buffer.append(df)


def extract_metadata(filename):
    """Extracts YYYY and YYYY-MM from filename 'tlc_uber_2019-01.parquet'"""
    base = os.path.basename(filename).replace(".parquet", "")
    parts = base.split("_")[-1].split("-")  # ['2019', '01']
    year = parts[0]
    month = parts[1]
    return year, f"{year}-{month}"


def main():
    print(f"🚀 Orion: Initializing Stratified Sampler")
    print(f"   Mode: {SPLIT_MODE.upper()}")
    print(f"   Rate: {SAMPLE_FRACTION * 100}%")
    print(f"   Seed: {RANDOM_SEED}")

    files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.parquet")))
    if not files:
        print("❌ No files found.")
        return

    engine = SamplingEngine(SPLIT_MODE, OUTPUT_DIR)

    total_rows_in = 0
    total_rows_out = 0
    start_time = time.time()

    for i, f in enumerate(files, 1):
        filename = os.path.basename(f)
        year, yyyy_mm = extract_metadata(filename)

        print(f"[{i}/{len(files)}] Sampling {filename}...", end="", flush=True)

        try:
            # Load File
            lf = pl.scan_parquet(f)

            # Stratified Random Sample
            # We collect() here because sampling a LazyFrame often requires loading into memory anyway
            # to ensure exact row counts and seed stability per file.
            df = lf.collect()
            rows_in = len(df)

            # Apply Sample
            df_sample = df.sample(fraction=SAMPLE_FRACTION, seed=RANDOM_SEED)
            rows_out = len(df_sample)

            # Update Stats
            total_rows_in += rows_in
            total_rows_out += rows_out

            # Pass to Engine
            # Key depends on mode:
            # Yearly -> Pass '2019'
            # Monthly -> Pass '2019-01'
            # Single -> Pass 'ALL' (Dummy key)

            group_key = year if SPLIT_MODE == "yearly" else yyyy_mm
            if SPLIT_MODE == "single":
                group_key = "ALL"

            engine.add_chunk(df_sample, group_key)

            print(f" Done. ({rows_out:,} / {rows_in:,} rows)")

        except Exception as e:
            print(f" ❌ Failed: {e}")

    # Final Flush (for the last batch in buffer)
    engine.flush()

    print("\n" + "=" * 50)
    print(f"✅ Sampling Complete in {(time.time() - start_time) / 60:.2f} min")
    print(f"📉 Reduction: {total_rows_in:,} -> {total_rows_out:,} rows")
    print(f"💾 Output: {OUTPUT_DIR}")
    print("=" * 50)


if __name__ == "__main__":
    main()
