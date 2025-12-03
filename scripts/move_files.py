import os
import shutil
import glob

# --- Configuration ---
# Where the Hive-style folders are currently:
SOURCE_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\TLC_NYC_Processed"

# Where you want the flat files:
DEST_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025 - Processed"


def flatten_dataset():
    print(f"📦 Starting Dataset Flattening...")
    print(f"   Source: {SOURCE_DIR}")
    print(f"   Dest:   {DEST_DIR}")

    if not os.path.exists(SOURCE_DIR):
        print("❌ Source directory does not exist.")
        return

    os.makedirs(DEST_DIR, exist_ok=True)

    # Find all deep 'data.parquet' files
    # Pattern matches: SOURCE_DIR / year=XXXX / month=XX / data.parquet
    search_pattern = os.path.join(SOURCE_DIR, "**", "data.parquet")
    found_files = glob.glob(search_pattern, recursive=True)

    if not found_files:
        print("❌ No 'data.parquet' files found. Check the path.")
        return

    print(f"🔍 Found {len(found_files)} files. Moving...")

    moved_count = 0
    for file_path in found_files:
        try:
            # Extract Year and Month from the directory path
            # Example path: .../year=2019/month=02/data.parquet
            path_parts = file_path.split(os.sep)

            # Robustly find the parts starting with 'year=' and 'month='
            year_part = next((p for p in path_parts if p.startswith("year=")), None)
            month_part = next((p for p in path_parts if p.startswith("month=")), None)

            if year_part and month_part:
                yyyy = year_part.split("=")[1]
                mm = month_part.split("=")[1]

                # Construct descriptive new name
                new_name = f"tlc_uber_{yyyy}-{mm}.parquet"
                new_path = os.path.join(DEST_DIR, new_name)

                # Move the file
                shutil.move(file_path, new_path)
                print(f"   [Move] {yyyy}/{mm} -> {new_name}")
                moved_count += 1
            else:
                print(f"   ⚠️ Skipping non-standard path: {file_path}")

        except Exception as e:
            print(f"   ❌ Error moving {file_path}: {e}")

    print("-" * 40)
    print(f"✅ Operation Complete.")
    print(f"🎉 Moved {moved_count} files to {DEST_DIR}")
    print("Note: The empty 'year=...' folders in the source directory can now be safely deleted.")


if __name__ == "__main__":
    flatten_dataset()
