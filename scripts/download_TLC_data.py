import requests
import os
import time

# --- Configuration ---
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{}.parquet"
START_DATE = (2019, 2)  # (Year, Month)
END_DATE = (2025, 9)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
}

DEST_DIR = r"X:\Programming\Python\Projects\Data processing\TLC NYC datasets\HVFHV subsets 2019-2025"

# --- Logic ---


def generate_dates(start_y, start_m, end_y, end_m):
    """Generates YYYY-MM strings for the URL range."""
    dates = []
    current_year, current_month = start_y, start_m
    while current_year < end_y or (current_year == end_y and current_month <= end_m):
        dates.append(f"{current_year}-{current_month:02d}")
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    return dates


def download_file(date_str):
    """Handles single file download with User-Agent and retry logic."""
    file_url = BASE_URL.format(date_str)
    file_name = file_url.split("/")[-1]
    local_path = os.path.join(DEST_DIR, file_name)

    if os.path.exists(local_path):
        print(f"✅ Skipping {file_name} - already exists.")
        return True

    print(f"⬇️ Downloading {file_name}...")

    for attempt in range(3):
        try:
            response = requests.get(file_url, headers=HEADERS, stream=True, timeout=30)
            response.raise_for_status()

            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"   Success: {file_name} saved to {DEST_DIR}")
            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                # This means the User-Agent spoofing failed or the server tightened up.
                print(
                    f"   🛑 Critical 403 Forbidden error for {file_name}. Server rejected request even with spoofed header."
                )
                return False
            # Handle other HTTP errors (404, 5xx)
            print(f"   ❌ HTTP Error (Attempt {attempt + 1}/3): {e}")

        except requests.exceptions.RequestException as e:
            # Handle connection/timeout errors
            print(f"   ❌ Connection Error (Attempt {attempt + 1}/3): {e}")

        time.sleep(2**attempt)  # Exponential backoff

    print(f"   🛑 Failed to download {file_name} after 3 attempts.")
    return False


# --- Execution ---
if __name__ == "__main__":
    if not os.path.exists(DEST_DIR):
        os.makedirs(DEST_DIR)
        print(f"Created directory: {DEST_DIR}")

    target_dates = generate_dates(START_DATE[0], START_DATE[1], END_DATE[0], END_DATE[1])

    # To leverage multithreading for speed, you would wrap the download_file function
    # using Python's `concurrent.futures.ThreadPoolExecutor`.
    for date_str in target_dates:
        download_file(date_str)
