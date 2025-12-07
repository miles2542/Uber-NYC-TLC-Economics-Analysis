# **Project Overview**

### **Topic: The Power of Data Preparation through Data Storytelling**
This project demonstrates how rigorous data engineering transforms raw, noisy telemetry into actionable economic insights. By applying a systematic cleaning and feature engineering pipeline to over **1.4 billion** ride-hail records, we reveal trends in pricing, driver pay, and urban mobility that are invisible in the raw data.

### **Objective**
To process the massive New York City TLC dataset (2019–2025), showing that careful data preparation and thoughtful architecture (Polars, Parquet, Partitioning) can uncover insights hidden insights in big data that the raw files alone cannot provide.

### **Dataset at a Glance**
*   **Source:** [NYC Taxi & Limousine Commission (TLC) Official Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
*   **Scope:** High-Volume For-Hire Vehicle (HVFHS) Trip Records.
*   **Provider:** Uber (License `HV0003`) (filtered exclusively).
*   **Timeline:** February 2019 to September 2025.
*   **Volume:** ~1 Billion individual trips (after filtering).
*   **Size:** ~70GB (Parquet), or ~400GB+ (CSV estimate).

---

# **1. Data Acquisition & Raw State**

### **1. Source Selection**
The NYC TLC publishes trip records for multiple sectors. We selected the **High-Volume For-Hire Vehicle (HVFHV)** dataset because it contains data for app-based ride-hailing, i.e. Uber, which is taking more market share over time.

*   **Yellow/Green Taxis:** Still of decent volume, but declining market share, and more limited range.
*   **Standard FHV (Livery/Black Car):** Much less volume, no big name, not as interesting.
*   **HVFHV (Uber/Lyft):** The dominant market force with detailed per-trip economics.

**Why Uber Only?**
We filtered specifically for **Uber** (`hvfhs_license_num == 'HV0003'`) to ensure consistency.
*   **Uber:** Dominant market share (~70-75%).
*   **Lyft/Via/Juno:** Excluded to remove platform-specific confounding variables (e.g., different incentive structures), and these platforms have much smaller market shares, or not in service anymore. Analyzing cross-platform dynamics is another aspect reserved for future work.
*   This filtering reduced the total rows **from 1.416 billion to 1.023 billion**, without any other filtering.

### **2. Ingestion Strategy**
Downloading 80 files of monthly high-density Parquet files requires a robust automated script, not manual clicks on website.

*   **Script:** `download_TLC_data.py`
*   **Method:** Iterative HTTP requests to the CloudFront repository.
*   **Reliability:** Implemented exponential backoff (retries with increasing delays) to handle server timeouts and connection resets.
*   **Storage:** Data is saved raw to `HVFHV subsets 2019-2025/`, preserving the original monthly partitioning (which will take 33GB for 80 months).

### **3. Raw Data Dictionary (Pre-Processing)**
*Total Features: 24 (from 2025 onwards, it's 25).* This dictionary is taken almost directly from TLC site.

| Feature Name           | Description                                                 | Note                                     |
| :--------------------- | :---------------------------------------------------------- | :--------------------------------------- |
| `hvfhs_license_num`    | The license number of the platform (e.g., HV0003 for Uber). | Used for filtering.                      |
| `pickup_datetime`      | The date and time the trip started.                         |                                          |
| `dropoff_datetime`     | The date and time the trip ended.                           |                                          |
| `PULocationID`         | TLC Taxi Zone ID where the trip began.                      | Mapped to Shapefiles.                    |
| `DOLocationID`         | TLC Taxi Zone ID where the trip ended.                      | Mapped to Shapefiles.                    |
| `trip_miles`           | Total distance driven in miles.                             | Often noisy/GPS drift.                   |
| `trip_time`            | Total duration in seconds.                                  | Frequently inconsistent with timestamps. |
| `base_passenger_fare`  | Fare before taxes, tolls, and tips.                         | The core revenue metric.                 |
| `driver_pay`           | Net earnings paid to the driver.                            | Critical for economic analysis.          |
| `tips`                 | Voluntary gratuity.                                         |                                          |
| `tolls`                | Bridge and tunnel fees.                                     |                                          |
| `congestion_surcharge` | Fee for trips starting/ending in Manhattan (< 96th St).     |                                          |
| `airport_fee`          | Fee for pickup/dropoff at JFK/LGA/EWR.                      |                                          |
| `shared_match_flag`    | Did the passenger share the ride? (Y/N).                    | Indicates pooling.                       |
| `wav_match_flag`       | Was the vehicle Wheelchair Accessible? (Y/N).               | Accessibility metric.                    |
| `cbd_congestion_fee`   | **New (2025):** Congestion Relief Zone fee.                 | Added Jan 2025.                          |

### **4. External Data Enrichments**
To contextualize the raw trips, we integrated three external datasets:

*   **Spatial:** `taxi_zones.shp` (Shapefile).
    *   *Usage:* Converted Polygon zones into **Centroids (Lat/Lon)** to calculate straight-line distances and bearing.
*   **Meteorological:** Visual Crossing Weather API.
    *   *Usage:* Fetched **Hourly Weather** (Temp, Precip, Snow, Wind) for New York City (2019–2025) to correlate demand with weather events.
*   **Lookup Tables:** `taxi_zone_lookup.csv`.
    *   *Usage:* Mapped ID numbers to Boroughs (e.g., `132` → `Queens`) and Neighborhood names.


# **2. Technical Architecture**

### **Challenge: Big Data on Small Hardware**
Processing **1.4 Billion rows (~70GB Parquet / ~400GB CSV)** on a standard laptop (16GB RAM) presents immediate bottlenecks:
*   **Memory Overflow:** Loading the full dataset into Pandas crashes immediately.
*   **Slow I/O:** Reading/writing huge CSVs takes hours.
*   **Inefficient Compute:** Single-threaded processing leaves CPU cores idle.

### **Solution: A Streaming Lakehouse Architecture**
We implemented a modern, high-performance ETL pipeline using **`Polars`** *(still in Python!)* and **partitioning**.

**Key Architectural Decisions:**
1.  **Polars over Pandas:**
    *   **Lazy Evaluation:** Polars builds a computational graph and only executes when needed, allowing query optimization before data is touched.
    *   **Streaming Engine:** Processes data in chunks, keeping memory usage constant regardless of file size.
    *   **Rust Backend:** significantly faster execution for heavy group-by and join operations.
2.  **Partitioned Parquet files:**
    *   Much smaller file sizes (5-6x smaller than CSV).
    *   Much faster read/write due to columnar storage format, enabling selective column access, built-in chunking, and metadata indexing.
    *   The data is then split monthly and named descriptively, enabling efficient reads/writes of only necessary data slices ("Predicate Pushdown"), or process the whole dataset month-by-month (instead of all at once), avoiding memory overload.
3.  **Atomic Processing:**
    *   The pipeline processes one file at a time (Month-by-Month).
    *   It cleans, engineers, and aggregates a single month, saves the result, flushes memory, and moves to the next. This guarantees the pipeline doesn't crash, even on lower-end consumer machines.

### **Pipeline Flowchart**
*(Concept for Visual Diagram)*

```mermaid
graph TD
    A[Raw Data Source] -->|Download Script| B(Raw Parquet Files)
    B -->|Stream Scan| C{Atomic Processor}
    C -->|Join| D[External Data (Zones + Weather)]
    C -->|Filter| E[Data Hygiene Engine]
    C -->|Calculate| F[Feature Engineering Engine]
    
    F -->|Write| G(Processed Dataset - 70GB)
    G -->|Stratified Sampling| H(The Flesh - 1% Sample)
    G -->|Aggregation| I(The Backbone - 4 Data Marts)
```

---

# **3. Transformation Pipeline**

### **Overview**
The raw data tells us "A car moved from A to B." Our pipeline transforms this into "Paid routes are ~8 km/h faster than free alternatives when moving from Bronx to Manhattan". We achieve this through 3 stages: **Enrichment** $\to$ **Cleaning** $\to$ **Feature Engineering**.

### **4.1 External Enrichments**
We enriched the telemetry with context from the physical world.

*   **Spatial Geometry (The Map):**
    *   **Source:** NYC Taxi Zone Shapefiles (`.shp`).
    *   **Processing:** Converted complex Polygons into **Centroids (Lat/Lon points)**.
    *   **Why:** Raw data only gives Zone IDs (e.g., `161`). By converting these to coordinates, we can calculate **Straight-Line Distance** and **Bearing**, enabling us to detect detours (`Driven Dist` vs `Straight Dist`).

*   **Meteorological Context (The Environment):**
    *   **Source:** Visual Crossing Weather API.
    *   **Data:** Hourly weather data (Temperature, Precipitation, Snow, Wind) for NYC from 2019 to 2025 (`get_weather_data.ipynb`).
    *   **Granularity:** Hourly resolution for "New York, NY".
    *   **Why:** Demand is weather-dependent. We joined weather data on `pickup_hour` to analyze how Rain, Snow, and Extreme Cold impact pricing and tipping behavior.


### **4.2 Data Hygiene: The "Great Filter"**

Raw telemetry data is inherently noisy. GPS sensors drift, app sessions hang, and human entry errors occur. Before analysis, we established a "Ground Truth" by enforcing physical and economic laws on the dataset.

#### **1. The Diagnostic Phase (Audit)**
We built a custom auditing tool (`universal_audit.py`) to scan the raw parquet files without loading them entirely into memory. This generated a "Health Matrix" revealing three critical issues:

*   **Logical Paradoxes:**
    *   **"Time Travel":** Trips where `dropoff_datetime` < `pickup_datetime` (For some reason, this happens every year, and only in November, funny).
    *   **"Slave Labor":** Trips with valid distance but `driver_pay <= 0` (Peaked significantly in August 2019).
    *   **"Teleportation":** Trips covering massive distances in seconds.
*   **Data Completeness:** High null rates in specific fee columns (e.g., `airport_fee` ~29% nulls), requiring strategic imputation (drop if important column, fill with 0 for some columns, etc.).


#### **2. Statistical Thresholding (The Distribution Atlas)**
To determine valid ranges for filtering, we did not guess. We computed the **Distribution Atlas**: a monthly breakdown of every numerical feature's vital statistics (Min, Mean, Median, P99, P99.9, Max).

**Decision Framework:**
1.  **Lower Bound:** Strictly positive for physical metrics (Distance/Time) and Economic metrics (Fare/Pay).
2.  **Upper Bound:** We targeted the **99.9th Percentile** combined with domain knowledge.
    *   *Observation:* The Max values were often absurd (e.g., a $5,000 fare or 10,000 miles).
    *   *Action:* We set cutoffs slightly above P99.9 but significantly below Max to remove system errors while preserving legitimate "Whale" trips.


#### **3. The Filter Rules**
We applied the following strict bounds during the atomic processing of each file. Rows violating these rules were dropped to ensure the integrity of the aggregate analysis.

*Note:* while the data has `trip_miles` and `trip_time`, these are often noisy. We derived our own `duration_seconds` and `trip_km` from timestamps and GPS coordinates for more reliable filtering. And we use kilometers because we're not 🦅🗽.

| Category         | Feature                  | Condition            | Rationale                                                                                                                             |
| :--------------- | :----------------------- | :------------------- | :------------------------------------------------------------------------------------------------------------------------------------ |
| **Physics**      | **Trip Distance**        | `0.15` to `120` km   | Removes micro-trips (GPS jitter) and extreme regional outliers, or data entry errors.                                                 |
|                  | **Trip Duration**        | `60s` to `15,000s`   | Removes immediate cancellations (<1 min) and "Zombie Sessions" (>4 hours).                                                            |
|                  | **Speed**                | `1` to `100 km/h`    | Cap set above NYC speed limits to account for highway speeding, but removes teleportation.                                            |
|                  | **Location**             | `ID` $\in [1, 263]$  | Removes `264` (Unknown) and `265` (Void) to ensure map integrity.                                                                     |
| **Economics**    | **Base Fare**            | `$0.10` to `$300.00` | Ensures non-zero revenue; caps extreme data entry errors.                                                                             |
|                  | **Driver Pay**           | `$0.01` to `$200.00` | Eliminates the "Slave Labor" paradox (trips with $0). The upper bound is capped relative to base fare.                                |
|                  | **Tips**                 | *Dynamic*            | Logic: `Tip <= $50` OR `Ratio <= 400%`. Caps "Fat Finger" errors while preserving high-end generosity.                                |
| **Taxes & Fees** | **Congestion Surcharge** | $\le \$2.75$         | Aligned with Manhattan Congestion Surcharge policy.                                                                                   |
|                  | **Tolls**                | $\le \$50.00$        | Covers major crossings (Verrazzano, GWB) without allowing error spikes.                                                               |
|                  | **Airport Fee**          | $\le \$5.00$         | Standard fee is $2.50. Cap allows for Pickup+Dropoff combos. Trips that go through 3+ airports are possible, but very, very unlikely. |
|                  | **Sales Tax**            | $\le \$36.00$        | Proportional cap based on ~8.875% of the max allowable fare, per the general laws and rules of TLC.                                   |
|                  | **Black Car Fund**       | $\le \$15.00$        | Proportional cap based on ~2.5-3% of the max fare.                                                                                    |
|                  | **CBD Fee**              | *No Filter*          | Left raw to capture the introduction of the fee in 2025, as it shows no extreme outliers or negative number.                          |



### **4.3 Feature Engineering: The Insight Engine**

Cleaning the data was only the first step. To answer questions about "Inefficiency," "Nightlife," and "Inflation," we engineered **over 45 new features**, transforming raw logs into a rich analytical dataset.

#### **1. Temporal Reconstruction**
Raw timestamps (`2019-02-01 14:32:05`) are useless for behavioral analysis. We deconstructed time into:
*   **Granularity:** Extracted `Year`, `Month`, `Day`, `Hour`, `DOW` (Monday=1, Sunday=7).
*   **Cyclical Features:** Converted Hour/Day/Month into `Sin` and `Cos` components. This allows ML models to understand that Hour 23 (11 PM) is "close" to Hour 0 (Midnight), preventing the "cliff" effect of linear time.
*   **Cultural Time:** We created `cultural_day_type` to map rides to human behavior, not the calendar.
    *   *Example:* A ride at 2 AM Saturday is technically "Saturday Morning," but culturally it is **"Friday Night Nightlife."** This feature captures that nuance.
*   **Pandemic Phase:** We tagged every ride with its COVID era (`Pre-Pandemic`, `Lockdown`, `Recovery`, `New Normal`) to enable Era-based comparisons.

#### **2. Geospatial Intelligence**
We moved beyond simple "Zone IDs" to measure flow and efficiency.
*   **Centroid Physics:** By joining Shapefiles, we calculated the **Straight-Line Distance** (Haversine) between Pickup and Dropoff centroids.
*   **Tortuosity Index:** The ratio of `Driven Distance / Straight Line Distance`.
    *   *Why?* This is our "Gridlock Detector." A high index (>1.5) means the driver had to take a massive detour (highway) or got lost in complex streets. It separates "Distance due to Speed" from "Distance due to Inefficiency."
*   **Borough Flow:** We categorized trips into transit archetypes: `Manhattan Internal`, `Manhattan <-> Outer`, `Outer <-> Outer`. This highlights the "Transit Desert" economy where Uber replaces the G subway.

#### **3. Economic Reconstruction**
We rebuilt the entire financial stack to track money flow.
*   **Total Cost:** We summed all 8 financial components (Fare, Tips, Surcharges, Taxes, Tolls, Fees) to derive the **Gross Booking Value (GBV)**—the actual price paid by the rider.
*   **The "Subsidy" Detector:** We calculated `driver_revenue_share` (Driver Pay / Base Fare).
    *   *Why?* This acts as a proxy for platform subsidies, and also the proxy for Uber take rate (1 - driver_revenue_share) - while not accurate (actual values are classified), it gives us a general sense of how much Uber is taking from each fare, and how much of the base fare for each trip is going to the driver.
*   **Generosity Metrics:** We standardized tipping behavior into `tipping_pct` and `is_generous_tip` (>25%) to study the psychology of gratitude versus obligation.

#### **4. Meteorological Context**
We didn't just dump raw weather data; we categorized it based on **human impact**, binned into intensity thresholds. Derived from standard meteorological thresholds, but slightly adjusted to account for NYC specifically, in the context of traffic impact.
* **Temperature:** `Freezing` ($<0$), `Cold` ($0-10$), `Mild` ($10-20$), `Warm` ($20-28$), `Hot` ($>28$).
* **Rain:** `Light` ($<1mm$), `Moderate` ($1-5mm$), `Heavy` ($>5mm$).
* **Snow:** `Trace` ($<2.5cm$), `Moderate` ($2.5-10cm$), `Heavy` ($10-20cm$), `Severe` ($>20cm$).
* **Wind:** `Breezy` ($15-40km/h$), `Windy` ($40-62km/h$), `Gale` ($>62km/h$).
* **Visibility:** `Reduced` ($1-10km$), `Poor/Fog` ($<1km$).
*   **The "Chaos" Hierarchy:** We created a `weather_state` categorical that prioritizes disruption.
    *   *Logic:* If it is raining *and* snowing, the state is **Snowing** (because snow have larger impact on traffic than simply raining).

---

### **Processed Data Dictionary (Schema)**
*Total Features: 70. Grouped by Analytical Domain.*

#### **Group 1: Temporal Context (17 Features)**


*Features that place the ride in human and machine time.*

| Feature Name         | Type     | Definition                                 | Range / Values                                                    | Rationale & Usage                                                                                      |
| :------------------- | :------- | :----------------------------------------- | :---------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------- |
| `pickup_datetime`    | Datetime | The exact timestamp when the trip started. | 2019–2025                                                         | **The anchor.** Use for sorting and granular time plotting.                                            |
| `dropoff_datetime`   | Datetime | The exact timestamp when the trip ended.   | 2019–2025                                                         | Used to calculate duration.                                                                            |
| `pickup_date`        | Date     | The calendar date of the trip.             | YYYY-MM-DD                                                        | Optimized for grouping by day without re-casting.                                                      |
| `pickup_year`        | Int32    | The year of the trip.                      | 2019–2025                                                         | High-level yearly faceting.                                                                            |
| `pickup_month`       | UInt32   | The month of the trip.                     | 1–12                                                              | Seasonality analysis.                                                                                  |
| `pickup_day`         | UInt32   | The day of the month.                      | 1–31                                                              | Intra-month trend analysis.                                                                            |
| `pickup_hour`        | UInt32   | The hour of the day (24h).                 | 0–23                                                              | Hourly demand analysis.                                                                                |
| `pickup_dow`         | UInt32   | The day of the week (ISO).                 | 1 (Mon) – 7 (Sun)                                                 | Weekly cycle analysis.                                                                                 |
| `time_of_day_bin`    | String   | Functional categorization of the hour.     | `morning_rush`, `midday`, `evening_rush`, `evening`, `late_night` | **Storytelling.** Better than raw hours for describing "Commute" vs "Nightlife".                       |
| `cultural_day_type`  | String   | A sociological definition of the day type. | `workday`, `weekend_night`, `weekend_day`, `sunday_rest`          | **Crucial.** Distinguishes "Friday Night Party" (Weekend Night) from "Monday Morning Grind" (Workday). |
| `pandemic_phase`     | String   | The COVID-19 era of the trip.              | `pre_pandemic`, `lockdown`, `recovery`, `new_normal`              | Essential for contextualizing 2020–2021 volume drops.                                                  |
| `cyclical_hour_sin`  | Float64  | Sine transformation of the hour.           | -1.0 to 1.0                                                       | **ML Only.** Allows models to understand that Hour 23 is adjacent to Hour 0.                           |
| `cyclical_hour_cos`  | Float64  | Cosine transformation of the hour.         | -1.0 to 1.0                                                       | **ML Only.**                                                                                           |
| `cyclical_month_sin` | Float64  | Sine transformation of the month.          | -1.0 to 1.0                                                       | **ML Only.**                                                                                           |
| `cyclical_month_cos` | Float64  | Cosine transformation of the month.        | -1.0 to 1.0                                                       | **ML Only.**                                                                                           |
| `cyclical_day_sin`   | Float64  | Sine transformation of the day of week.    | -1.0 to 1.0                                                       | **ML Only.**                                                                                           |
| `cyclical_day_cos`   | Float64  | Cosine transformation of the day of week.  | -1.0 to 1.0                                                       | **ML Only.**                                                                                           |


#### **Group 2: Geospatial & Trip Context (10 Features)**
*Location identifiers and trajectory classifications.*

| Feature Name        | Type   | Definition                                           | Range / Values                                                                | Rationale & Usage                                                                 |
| :------------------ | :----- | :--------------------------------------------------- | :---------------------------------------------------------------------------- | :-------------------------------------------------------------------------------- |
| `PULocationID`      | Int32  | TLC Taxi Zone ID where the trip began.               | 1–263                                                                         | Join with Shapefile for maps.                                                     |
| `DOLocationID`      | Int32  | TLC Taxi Zone ID where the trip ended.               | 1–263                                                                         | Join with Shapefile for maps.                                                     |
| `pickup_borough`    | String | The NYC Borough of the pickup.                       | Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR                        | High-level geographic grouping.                                                   |
| `dropoff_borough`   | String | The NYC Borough of the dropoff.                      | (Same as above)                                                               | High-level geographic grouping.                                                   |
| `pickup_zone`       | String | The name of the neighborhood (e.g., "East Village"). | (Variable)                                                                    | Human-readable labels for charts.                                                 |
| `dropoff_zone`      | String | The name of the neighborhood.                        | (Variable)                                                                    | Human-readable labels for charts.                                                 |
| `borough_flow`      | String | A string describing the movement path.               | e.g., "Manhattan -> Brooklyn"                                                 | Simplifies flow analysis (Sankey diagrams).                                       |
| `borough_flow_type` | String | Classification of the transit path.                  | `manhattan_internal`, `manhattan_outer_commute`, `outer_inter`, `outer_intra` | **Storytelling.** Highlights the "Transit Desert" economy (Outer-to-Outer trips). |
| `trip_type_zone`    | String | Granular classification of distance.                 | `intra_zone`, `intra_borough`, `inter_borough`                                | Differentiates local errands from cross-city commutes.                            |
| `trip_archetype`    | String | A behavioral classification of the trip's purpose.   | `commute`, `nightlife`, `airport`, `leisure`                                  | **Storytelling.** Inferred based on Time + Location + Day.                        |

#### **Group 3: Physics & Service Metrics (11 Features)**
*Measurements of speed, distance, and system efficiency.*


| Feature Name               | Type    | Definition                                                     | Range / Values | Rationale & Usage                                                                                                          |
| :------------------------- | :------ | :------------------------------------------------------------- | :------------- | :------------------------------------------------------------------------------------------------------------------------- |
| `trip_km`                  | Float32 | The actual distance driven (Odometer).                         | 0.15 – 120.0   | The basis for billing and cost analysis.                                                                                   |
| `duration_seconds`         | Float64 | Total trip time in seconds.                                    | 60 – 15,000    | The raw measure of time spent.                                                                                             |
| `duration_min`             | Float64 | Total trip time in minutes.                                    | 1.0 – 250.0    | Human-readable duration.                                                                                                   |
| `straight_line_dist_km`    | Float64 | The "As the crow flies" distance between centroids.            | > 0            | Used to calculate efficiency.                                                                                              |
| `bearing_degrees`          | Float64 | The compass direction of travel (0=North).                     | 0.0 – 360.0    | Analyzes flow direction (e.g., "Everyone heads North in the evening").                                                     |
| `speed_kmh`                | Float64 | Average speed based on *driven* distance (`trip_km` / `time`). | 1.0 – 100.0    | Measures how fast the wheels turned. High on highways.                                                                     |
| `displacement_speed_kmh`   | Float64 | Effective speed based on *straight line* distance.             | > 0            | **The Gridlock Detector.** Measures how fast you *actually* got closer to your destination. Low values = Stuck in traffic. |
| `tortuosity_index`         | Float64 | Ratio of Driven Dist / Straight Line Dist.                     | >= 1.0         | **Efficiency Metric.** 1.0 = Straight line. > 1.5 = Detours or complex street grids.                                       |
| `total_wait_time_min`      | Float64 | Time between App Request and Pickup.                           | > 0 (or Null)  | Measures system latency and passenger wait pain. **Nulls:** Negative values (Time Travel paradox) forced to `Null`.        |
| `driver_response_time_min` | Float64 | Time between App Request and Driver Arrival.                   | > 0 (or Null)  | Measures driver supply availability. **Nulls:** *same as above.*                                                           |
| `boarding_time_min`        | Float64 | Time between Driver Arrival and Trip Start.                    | > 0 (or Null)  | Measures "Curb Friction" (Passenger lateness). **Nulls:** *same as above.*                                                 |


#### **Group 4: Financials & Economics (17 Features)**
*The complete breakdown of pricing, pay, and taxation.*

| Feature Name           | Type    | Definition                                      | Range / Values | Rationale & Usage                                                             |
| :--------------------- | :------ | :---------------------------------------------- | :------------- | :---------------------------------------------------------------------------- |
| `base_passenger_fare`  | Float32 | The core price of the ride (before taxes/tips). | 0.10 – 300.0   | The baseline demand signal.                                                   |
| `tolls`                | Float32 | Bridge and tunnel fees.                         | 0.0 – 50.0     | Pass-through costs.                                                           |
| `bcf`                  | Float32 | Black Car Fund tax (~2.5-3%).                   | 0.0 – 15.0     | Mandatory NY State tax.                                                       |
| `sales_tax`            | Float32 | NY Sales Tax (~8.875%).                         | 0.0 – 40.0     | Mandatory tax.                                                                |
| `congestion_surcharge` | Float32 | Fee for entering Manhattan (< 96th St).         | 0.0 – 2.75     | Policy impact metric.                                                         |
| `airport_fee`          | Float32 | Fee for pickup/dropoff at JFK/LGA/EWR.          | 0.0 – 6.0      | Tourism tax.                                                                  |
| `cbd_congestion_fee`   | Float32 | The new congestion zone fee (Jan 2025+).        | 0.0 or 2.50    | **New.** Tracks the impact of the 2025 policy change.                         |
| `tips`                 | Float32 | Voluntary gratuity from passenger.              | 0.0 – 300.0    | Measures customer satisfaction/generosity.                                    |
| `driver_pay`           | Float32 | The net earnings of the driver.                 | 0.01 – 200.0   | **Key Target.** Used to calculate driver wages.                               |
| `total_rider_cost`     | Float32 | Sum of ALL 8 financial columns above.           | > 0            | **The True Cost.** What the rider actually saw on their credit card bill.     |
| `cost_per_km`          | Float32 | Total Cost / Trip KM.                           | > 0            | **Luxury Index.** High values indicate premium zones (Short Manhattan trips). |
| `driver_revenue_share` | Float32 | Driver Pay / Base Fare.                         | 0.0 – 1.0+     | **Fairness Metric.** What % of the core fare goes to the driver?              |
| `uber_take_rate_proxy` | Float32 | 1 - Driver Revenue Share.                       | < 1.0          | **Platform Tax.** Estimated % kept by Uber.                                   |
| `pay_per_hour`         | Float32 | Driver Pay / Duration (Hours).                  | > 0            | **Livability Metric.** Can be compared to Minimum Wage.                       |
| `tipping_pct`          | Float32 | Tips / Base Fare.                               | 0.0 – 4.0      | Normalized generosity metric.                                                 |
| `is_generous_tip`      | UInt8   | Flag if Tip > 25% of Fare.                      | 0 / 1          | Identifies "Whale" tippers.                                                   |
| `is_subsidized`        | UInt8   | Flag if Driver Pay > Base Fare.                 | 0 / 1          | Identifies trips where Uber likely lost money to incentivize drivers.         |

#### **Group 5: Service Flags (5 Features)**
*Binary indicators (0/1) regarding specific ride attributes and accessibility.*

| Feature Name          | Type  | Definition                               | Range / Values | Rationale & Usage                  |
| :-------------------- | :---- | :--------------------------------------- | :------------- | :--------------------------------- |
| `shared_request_flag` | UInt8 | Did passenger request a pool?            | 0 / 1          | Measures willingness to share.     |
| `shared_match_flag`   | UInt8 | Did the pool actually match?             | 0 / 1          | Measures system liquidity/density. |
| `access_a_ride_flag`  | UInt8 | Administered by MTA?                     | 0 / 1          | Paratransit integration.           |
| `wav_request_flag`    | UInt8 | Requested Wheelchair Accessible Vehicle? | 0 / 1          | Equity/Accessibility metric.       |
| `wav_match_flag`      | UInt8 | Was the vehicle WAV?                     | 0 / 1          | Supply side of accessibility.      |

#### **Group 6: Meteorological Context (10 Features)**
*External weather conditions matched to the trip hour.*

| Feature Name         | Type    | Definition                            | Range / Values                                         | Rationale & Usage                                                        |
| :------------------- | :------ | :------------------------------------ | :----------------------------------------------------- | :----------------------------------------------------------------------- |
| `temp`               | Float64 | Air temperature in Celsius.           | idk man                                                | Raw thermal comfort.                                                     |
| `conditions`         | String  | Raw summary from API.                 | e.g., "Rain, Overcast"                                 | Descriptive text.                                                        |
| `rain_intensity`     | String  | Categorical rain volume.              | `none`, `light`, `moderate`, `heavy`                   | Granular impact of rain on traffic.                                      |
| `snow_intensity`     | String  | Categorical snow volume.              | `none`, `trace_light`, `moderate`, `heavy`, `severe`   | **Chaos Metric.** Snow stops the city.                                   |
| `wind_intensity`     | String  | Categorical wind speed.               | `calm`, `breezy`, `windy`, `gale`                      | High wind increases "Walk Aversion".                                     |
| `visibility_status`  | String  | Categorical visibility distance.      | `clear`, `reduced`, `poor_fog`                         | Safety metric impacting speed.                                           |
| `weather_state`      | String  | Hierarchical summary of the hour.     | `snowing`, `snow_on_ground`, `raining`, `clear_cloudy` | **Best for Viz.** Prioritizes the most disruptive weather (Snow > Rain). |
| `is_bad_weather`     | UInt8   | Flag for generally poor conditions.   | 0 / 1                                                  | Simple filter for "Miserable Days".                                      |
| `is_extreme_weather` | UInt8   | Flag for severe/dangerous conditions. | 0 / 1                                                  | Identifies outlier days (blizzards, hurricanes).                         |
| `temp_bin`           | String  | Categorical temperature bucket.       | `freezing`, `cold`, `mild`, `warm`, `hot`              | Simplifies thermal analysis.                                             |


### **Hardware Benchmark**
To validate the architecture, we benchmarked the full ETL pipeline on consumer hardware.

*   **Test Machine:** Windows 11, Intel i5-12600K, 32GB RAM, NVMe SSD.
*   **Dataset:** 80 files, corresponding to 80 months (Feb 2019 – Sep 2025), initially ~33GB, after filtering, then feature engineering, goes to ~70GB Parquet.
*   **Performance:**
    *   **Downloading (80 files):** ~10-20 minutes (network dependent)
    *   **Filtering + Feature Engineering (Full Pass):** ~12 minutes.
    *   **Aggregation (4 Marts):** ~3 minutes.
    *   **Throughput:** Processed ~1.9 Million rows/second.
*   **Optimization Note:** By using Polars Streaming and strict type casting (`Float32`), memory usage peaked but does not overflow, CPU BBQ'd but did not crash, utilizing all available resources efficiently.
*   **Note:** Also tested on 16GB laptop, while taking substantially longer (due to less RAM, thus handling less at once), it was still able to run the full pipeline successfully. Yay, a run of the mill laptop just handled 1.4 billion rows of data 🏋🏻‍♀️!

---

# **5. Aggregation & Sampling Strategy**

### **The "Two-Tier" Data Architecture**
A 70GB dataset is too large for rapid exploration and visualization. We solved this by splitting the output into two distinct tiers, serving different analytical needs.

#### **Tier 1: Stratified Sampling**
*   **File:** `tlc_sample_20XX.parquet` (split yearly, or monthly - can be configured).
*   **Size:** ~800MB total (1% of population) - that is still 10M rows!
*   **Methodology:**
    *   We applied a **1% Stratified Random Sample** to each monthly file.
    *   **Why Stratified?** Randomly sampling the whole dataset might bias towards recent years (higher volume), or randomly sampling most lower fare trips, etc. Our method ensures Jan 2019 is represented exactly as proportionally as Jan 2025, and for most use cases, simply multiplying back up by 100x gives a decent estimate of totals, preserving the trends and more.
*   **Use Case:** This dataset preserves the **micro-structure** of the data. Use it for:
    *   Distribution analysis (Boxplots, Histograms).
    *   Correlation studies (e.g., "Do tips increase when speed decreases?").
    *   Machine Learning training.

#### **Tier 2: Aggregated Data Marts**
*   **Files:** 4 Aggregated Parquet/CSV files.
*   **Size:** ~250MB total.
*   **Methodology:**
    *   These files contain **100% of the data volume**, pre-aggregated by specific dimensions.
    *   Because they use the full population, they are the "Ground Truth" for volume and revenue reporting.
*   **Use Case:** High-level dashboards, Maps, and KPI tracking.

---

### **Aggregate Data Dictionaries**

#### **Mart 1: The Timeline Backbone (`agg_timeline_hourly.parquet`)**
*   **Grain:** Hourly.
*   **Purpose:** The source of truth for volume trends, seasonality, and long-term growth. It captures the exact moment demand collapsed during COVID and the recovery curve, and everything else in between.

| Feature Name            | Type    | Definition                                    | Rationale                                    |
| :---------------------- | :------ | :-------------------------------------------- | :------------------------------------------- |
| `pickup_year`           | Int32   | Year of trip.                                 | Time hierarchy.                              |
| `pickup_month`          | UInt32  | Month of trip (1-12).                         | Seasonality.                                 |
| `pickup_day`            | UInt32  | Day of month (1-31).                          | Daily volume tracking.                       |
| `pickup_hour`           | UInt32  | Hour of day (0-23).                           | Intraday patterns.                           |
| `borough_flow_type`     | String  | e.g., `manhattan_internal`.                   | Analyzing Commuter vs. Intra-borough trends. |
| `trip_archetype`        | String  | e.g., `commute`, `nightlife`.                 | Analyzing behavioral segments.               |
| `cultural_day_type`     | String  | e.g., `weekend_night`.                        | Distinguishing nightlife demand.             |
| `trip_count`            | UInt32  | Total number of trips in this hour/group.     | **Primary Volume Metric.**                   |
| `total_fare_amt`        | Float64 | Sum of Base Fares.                            | Core Revenue (Platform + Driver).            |
| `total_driver_pay`      | Float64 | Sum of Driver Pay.                            | Total Driver Earnings pool.                  |
| `total_cbd_fee`         | Float64 | Sum of CBD Congestion Fees.                   | Impact of 2025 policy.                       |
| `total_revenue_gross`   | Float64 | Sum of Total Rider Cost.                      | **Gross Booking Value (GBV).**               |
| `total_tips`            | Float64 | Sum of Tips.                                  | Total "Generosity Economy".                  |
| `avg_trip_km`           | Float64 | Mean distance traveled.                       | Detecting "Short Trip" trends.               |
| `avg_speed_kmh`         | Float64 | Mean travel speed.                            | Detecting systemic congestion.               |
| `bad_weather_count`     | UInt32  | Count of trips during `is_bad_weather=1`.     | Weather impact volume.                       |
| `extreme_weather_count` | UInt32  | Count of trips during `is_extreme_weather=1`. | Chaos impact volume.                         |

#### **Mart 2: The Network Backbone (`agg_network_monthly.parquet`)**
*   **Grain:** Monthly by Route (Origin-Destination).
*   **Purpose:** The engine for geospatial analysis. By pre-calculating the volume and speed for every Zone-to-Zone pair, we enable instant heatmap generation without crunching a billion rows.


| Feature Name             | Type    | Definition                         | Rationale                                            |
| :----------------------- | :------ | :--------------------------------- | :--------------------------------------------------- |
| `pickup_year`            | Int32   | Year.                              | Time hierarchy.                                      |
| `pickup_month`           | UInt32  | Month.                             | Time hierarchy.                                      |
| `PULocationID`           | Int32   | Origin Zone ID.                    | **Map Join Key.**                                    |
| `DOLocationID`           | Int32   | Destination Zone ID.               | **Map Join Key.**                                    |
| `pickup_borough`         | String  | Origin Borough.                    | High-level filtering.                                |
| `dropoff_borough`        | String  | Destination Borough.               | High-level filtering.                                |
| `trip_count`             | UInt32  | Volume on this specific route.     | Route Popularity (Edge Weight).                      |
| `avg_duration_min`       | Float64 | Mean trip time.                    | Route Duration.                                      |
| `avg_cost`               | Float64 | Mean Total Rider Cost.             | "How expensive is this route?"                       |
| `avg_displacement_speed` | Float64 | Mean straight-line speed.          | **Gridlock Metric.** Low values = inefficient route. |
| `avg_wait_time`          | Float64 | Mean time (Request $\to$ Pickup).  | **Service Quality.** Identifies underserved zones.   |
| `avg_driver_response`    | Float64 | Mean time (Request $\to$ Arrival). | Driver Supply proximity.                             |

#### **Mart 3: The Economic Backbone (`agg_pricing_distribution.parquet`)**
*   **Grain:** Daily by Context (Weather/Time).
*   **Purpose:** A deep dive into the "Wallet War." It tracks how Driver Pay Share and Pricing Surges fluctuate based on weather and time of day.


| Feature Name           | Type    | Definition                         | Rationale                                             |
| :--------------------- | :------ | :--------------------------------- | :---------------------------------------------------- |
| `pickup_date`          | Date    | Calendar Date.                     | Daily grouping.                                       |
| `time_of_day_bin`      | String  | e.g., `morning_rush`.              | Intraday economic cycles.                             |
| `weather_state`        | String  | e.g., `snowing`.                   | Weather impact on price.                              |
| `borough_flow_type`    | String  | e.g., `outer_inter`.               | Geographic economic variance.                         |
| `trip_count`           | UInt32  | Volume.                            | Sample size context.                                  |
| `avg_driver_share`     | Float64 | Mean (Driver Pay / Base Fare).     | **Fairness.** Does share drop during surges?          |
| `std_driver_share`     | Float64 | Standard Deviation of Share.       | **Volatility.** Is driver pay consistent or gambling? |
| `avg_take_rate`        | Float64 | Mean (1 - Driver Share).           | Platform Margin proxy.                                |
| `avg_tip_pct`          | Float64 | Mean (Tips / Base Fare).           | **Generosity.** Does rain increase tips?              |
| `avg_hourly_wage`      | Float64 | Mean (Driver Pay / Duration).      | **Livability.** Earnings per hour worked.             |
| `median_fare`          | Float64 | 50th Percentile Base Fare.         | "Typical Price."                                      |
| `p90_fare_surge_proxy` | Float64 | 90th Percentile Base Fare.         | **Surge Detector.** High P90 indicates price spikes.  |
| `dominant_rain`        | String  | Most common rain intensity (Mode). | Context for the day.                                  |

#### **Mart 4: The Executive Summary (`agg_executive_daily.csv`)**
*   **Grain:** Daily (Global).
*   **Purpose:** A lightweight CSV for quick KPI cards (Total Revenue, Total Volume) and Excel-based "Big Number" tracking.

| Feature Name                 | Type    | Definition                    | Rationale                     |
| :--------------------------- | :------ | :---------------------------- | :---------------------------- |
| `pickup_date`                | Date    | Calendar Date.                | Time Index.                   |
| `total_trips`                | Int64   | Total daily volume.           | **Headline Volume.**          |
| `total_fare_revenue`         | Float64 | Total Base Fare.              | Core Revenue.                 |
| `total_gross_booking_value`  | Float64 | Total Rider Cost (All in).    | **Headline GBV.**             |
| `total_tips`                 | Float64 | Total Tips.                   | Total Tip Economy.            |
| `total_km_traveled`          | Float64 | Sum of Trip KMs.              | Fleet Mileage.                |
| `bad_weather_trip_count`     | Int64   | Trips during bad weather.     | "How many miserable rides?"   |
| `extreme_weather_trip_count` | Int64   | Trips during extreme weather. | "How many chaotic rides?"     |
| `avg_wait_time`              | Float64 | Global mean wait time.        | **Headline Service Quality.** |

---

# **6. Impact Showcase: Before vs. After**

Data preparation is not just "cleaning"—it is the act of refining raw noise into a signal that can be trusted for decision-making. The following visualizations demonstrate the tangible impact of our pipeline.

### **1. Volume Filtration: The "Noise Ratio"**
![alt text](assets/plots/raw_vs_processed_trip_volume.png)

*   **Observation:** Raw data suggests ~240M annual Saturday trips. After filtering for Uber-only, removing duplicates, and applying physical constraints, the valid volume is ~165M.
*   **Impact:** A **~30-40% reduction** in row count. Using raw data would have massively inflated market size estimates and skewed demand forecasting models.

### **2. Revenue Reality Check**
![alt text](assets/plots/raw_vs_processed_yearly_revenue.png)

*   **Observation:** In 2024, raw records indicate a total fare revenue of **$7.5 Billion**. The validated Uber revenue is **$4.2 Billion**.
*   **Impact:** The gap represents competitor revenue (Lyft/Via) and "fat-finger" data entry errors (e.g., million-dollar fares). Filtering protects financial models from multi-billion dollar overestimation errors.

### **3. The Timeline Signal**
![alt text](assets/plots/signal_vs_noise_daily_trip_volume.png)

*   **Observation:** The shape of the curve remains identical (validating our sampling strategy), but the amplitude shifts.
*   **Impact:** The 7-day rolling average reveals that while noise is high volume, it is *consistent*. This confirms that our filtering logic did not accidentally purge specific time periods (e.g., COVID or holidays) disproportionately.

### **4. Unlockable Insights (Processed Data Only)**
*Because raw data lacks context (Weather, Economics, Routing), the following insights were impossible to generate before feature engineering. Note that these are **just 2 examples of the many insights and business questions** we have been able to answer through the processing and analysis process.*

#### **A. The Era Matrix: Efficiency vs. Subsidies**
![alt text](assets/plots/era_matrix.png)

*   **The Insight:** High driver revenue share does not equal high wages.
*   **The Evidence:** In 2019 (Blue Cluster), drivers kept **~100%** of the fare (subsidized) but earned only **$45/hr**. In 2025 (Green Cluster), drivers keep only **~80%** but earn **$60/hr** due to higher system efficiency and utilization.
*   **Value:** Disproves the assumption that lower commission always benefits the driver.

#### **B. Strategic Surge Windows**
![alt text](assets/plots/fig_2_2_surge_pricing_temporal_dynamics.png)

*   **The Insight:** Demand volume does not perfectly correlate with price efficiency.
*   **The Evidence:**
    *   **Late Night (2-5 AM):** Volume is low, but Price/Km spikes (Scarcity Surge).
    *   **Evening Rush (5-8 PM):** Volume is maxed out, but Price/Km is moderate.
*   **Value:** Identifies specific "Arbitrage Windows" for drivers where effort (driving) yields the highest marginal return per kilometer.


---

# **7. The Modeling Proof: Predictive Power**

To mathematically prove the value of our data preparation, we ran a controlled experiment: **Predicting Base Fare** using a gradient boosting model (LightGBM).

### **The Constraint (No Leakage)**
We enforced a strict "Pre-Trip" rule. The model can **only** see information available at the moment a user clicks "Request Ride."
*   **Excluded:** Duration, Driven Distance, Tips, Tolls, Surcharges (unknown before trip end).
*   **Included:** Location, Time, Weather, and our **Engineered Estimates**.

### **Model A: The Raw Baseline**
*   **Input:** Only the raw, e.g. `PULocationID`, `DOLocationID`, and `timestamp`.
*   **Performance:**
    *   **R² Score:** `0.05` (Explains only 5% of variance).
    *   **Error (MAE):** `$12.61` per trip.
*   **Verdict:** **Random Guessing.** Without distance or geospatial context, the model cannot distinguish a $10 local ride from a $100 airport run.

### **Model B: The Processed Challenger**
*   **Input:** Our engineered feature set (such as `straight_line_dist_km`, `borough_flow`, `bearing`, `weather_state`).
*   **Performance:**
    *   **R² Score:** `0.73` (Explains 73% of variance).
    *   **Error (MAE):** `$5.78` per trip.
*   **Verdict:** **Precision.** By reconstructing the physical distance (`straight_line_dist_km`) and traffic context (`borough_flow`), we reduced the prediction error by **54%**.

### **Visual Proof**

#### **1. The Accuracy Gap**
![alt text](assets/plots/modeling_raw_vs_processed_mae_rmse.png)

* **Impact:** We **reduced the errors** in both MAE and RMSE to **under half of the baseline model** operating on the raw data only.

#### **2. Prediction Fidelity**
![alt text](assets/plots/modeling_raw_vs_processed_actual_vs_predicted_r2.png)

*   **Left (Raw):** A flat cloud. The model guesses the mean ($20) for almost every trip, failing completely on high-value rides.
*   **Right (Processed):** A strong diagonal correlation. The model successfully captures the price dynamics of both short and long trips.
*   **Impact:** A 14x improvement in R² score confirms that "Raw Data" is insufficient for economic modeling.

#### **3. The Winning Signals**
![alt text](assets/plots/modeling_raw_vs_processed_engineered_features_importance.png)

*   **Insight:** The top predictor is **`straight_line_dist_km`**, a feature that *did not exist* in the raw data. It was created by joining Shapefile centroids. This proves that external enrichment is the single biggest driver of model performance.

---

# **8. Reproduction & Usage**

We provide two paths to use this project: **Fast Track** (Analysis only) and **Full Rebuild** (Cleaning and Engineering).

### **Option 1: Fast Track (Recommended)**
*For analysts and data scientists who want to jump straight to insights.*

1.  Navigate to the **Releases** section of this repository.
2.  Download the **`TLC_NYC_Processed_Sample.zip`** and **`TLC_NYC_Aggregates.zip`** (Total ~1.3GB).
3.  Unzip them into the `data/` folder.
4.  Open any notebook in `analysis_notebooks/` and run. No *(too)* heavy computation required.

### **Option 2: Full Rebuild (From Scratch)**
*For engineers who want to reproduce the full ETL pipeline.*

**1. Setup Environment**
```bash
pip install -r requirements.txt
```

**2. Ingest Data**
*   Edit `scripts/download_TLC_data.py`: Set `DEST_DIR` to your local storage (Needs ~35GB space initially, and at least ~110GB total).
*   Run the script to download the raw monthly Parquet files.

**3. Prepare External Assets**
*   Run `scripts/get_weather_data.ipynb` to fetch hourly weather. Needs Visual Crossing API key - free tier is sufficient. The weather data cannot be uploaded here due to API licensing.
*   Run `scripts/shapefile_processing.ipynb` to convert Taxi Zones to centroids.

**4. Execute Transformation**
*   Edit `scripts/process_data.py`: Update `RAW_DATA_DIR` and `OUTPUT_DIR`.
*   *Performance Tip:* Adjust `POLARS_MAX_THREADS` based on your CPU.
*   Run the script. This will clean, feature engineer, and save partitioned files.
*   Run `scripts/move_files.py` to flatten the directory structure for easier access.

**5. Aggregate & Sample**
*   Run `scripts/aggregate_datasets.py` to generate the 4 Data Marts.
*   Run `scripts/stratified_sampling.py` to generate the 1% Stratified Sample.

**6. Audit (Optional)**
*   Run `scripts/tlc_universal_audit.py` on any folder (Raw/Processed) to generate a health report.
*   Visualize the report using `notebooks/Data_health_audit_*.ipynb` (current files already have output saved to them).

---

# **9. Legal & Constraints**

### **Known Limitations**
*   **Driver Anonymity:** The dataset does not contain unique Driver IDs or Vehicle IDs. We cannot track individual driver shifts, fatigue, or long-term earnings. All analysis is trip-based, not driver-based.
*   **Wait Time Proxy:** `request_datetime` relies on app telemetry. High wait times may reflect passenger lateness ("curb time") rather than driver unavailability.
*   **Centroid Assumption:** Spatial metrics (`straight_line_dist`, `bearing`) use Zone Centroids. This underestimates distance for intra-zone trips (where dist = 0).
*   **Weather Localization:** Weather data is sourced from a single station (Central Park). Conditions at JFK or Staten Island may vary.

### **License & Citation**
This project utilizes public data provided by the **New York City Taxi and Limousine Commission (TLC)**.
*   **Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
*   **Rights:** Data is made available under the **Freedom of Information Law (FOIL)** and is in the public domain for immediate download and analysis.
*   **Disclaimer:** The TLC makes no representations as to the accuracy of these data. This project is an independent analysis and is not endorsed by the NYC TLC, nor is it related to Uber Technologies, Inc.

**External Data Credits:**
*   **Weather:** [Visual Crossing Weather API](https://www.visualcrossing.com/).
*   **Shapefiles:** [TLC NYC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
*   **Inflation data:** Consumer Price Index for All Urban Consumers: Private Transportation in New York-Newark-Jersey City (CUURA101SAT1) - [FRED, Federal Reserve Bank of St. Louis](https://fred.stlouisfed.org/series/CUURA101SAT1)
*   **Fuel price:** [U.S. Energy Information Administration (EIA)](https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=pet&s=emm_epmr_pte_y35ny_dpg&f=m)
