# **TLC NYC Project: Methodology & Transformation Log**

**Version:** 2.0 (Post-Audit)

**Scope:** HVFHS (Uber only) 2019–2025

**Objective:** Transform raw, noisy telemetry into a high-fidelity, economic storytelling dataset.

---

## 1. The Filtering (Data Hygiene)
*Rationale: Raw telemetry contains GPS drift, system glitches, and test records. We apply strict bounds based on physics and NYC TLC regulations.*

### **A. Physical Bounds**
| Metric            | Range / Condition           | Rationale                                                                                                          |
| :---------------- | :-------------------------- | :----------------------------------------------------------------------------------------------------------------- |
| **Trip Distance** | `0.15 km` to `120 km`       | Removes micro-trips (GPS jitter/cancellations) and regional outliers.                                              |
| **Duration**      | `60s` to `15,000s` (4.1 hr) | Removes immediate cancellations and "zombie sessions" (driver asleep, or forgot to end session when eating, etc.). |
| **Speed**         | `1 km/h` to `100 km/h`      | Cap set above NYC speed limits to account for highway speeding; removes teleportation/GPS jumps.                   |
| **Location**      | `LocationID` $\in [1, 263]$ | Removes `264` (Unknown) and `265` (void) to ensure geospatial integrity.                                           |

### **B. Economic Bounds**
| Metric          | Range / Condition    | Rationale                                                                                                                                                                     |
| :-------------- | :------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Base Fare**   | `$0.10` to `$300.00` | Ensures non-zero revenue; caps extreme outliers.                                                                                                                              |
| **Driver Pay**  | `$0.01` to `$200.00` | Removes "Slave Labor Paradox" (Work for \$0) and system errors. This might contain legit rides that are promos, but since we can't determine which is which, they're removed. |
| **Surcharge**   | `$0.00` to `$2.75`   | Aligned with max Congestion Surcharge policy.                                                                                                                                 |
| **Tolls**       | `$0.00` to `$50.00`  | Covers major crossings (Verrazzano, GWB) without allowing error spikes.                                                                                                       |
| **Sales Tax**   | `$0.00` to `$40.00`  | Proportional to max fare, usually noted around 8.875%.                                                                                                                        |
| **Airport Fee** | `$0.00` to `$6.00`   | Covers JFK/LGA/EWR pickup/drop-off combos ($2.50 per leg). Trips that covers 3+ airports in a single session are present, but very unlikely in real world, even with pooling. |
| **BCF**         | `$0.00` to `$15.00`  | Black Car Fund tax (approx 2.5-3%).                                                                                                                                           |

### **C. The "Smart Tip" Filter**
*Logic:* `Tips <= $50` **OR** `(Tips > $50 AND Tips <= 400% of Base Fare)`
*   **Purpose:** Preserves genuine generosity (e.g., \$20 tip on \$10 ride) while removing "Fat Finger" errors (e.g., \$500 tip on \$10 ride) or potential money laundering signals.

---

## 2. Feature Engineering

### **A. Temporal & Service Metrics**
*   **Granularity:** Extracted `Year`, `Month`, `Day`, `Hour`, `DOW` *(day of week)*.
*   **Cultural Time:** Created `cultural_day_type` to align with human behavior:
    *   **Workday:** Mon 06:00 $\to$ Fri 17:00.
    *   **Weekend Night:** Fri 17:00 $\to$ Sun 05:00 (Captures Nightlife).
    *   **Sunday Rest:** Sun 05:00 $\to$ Mon 06:00.
*   **Service KPIs:** Calculated from raw timestamps. Negative values (Time Travel paradox) forced to `Null`.
    *   `total_wait_time_min`: Request $\to$ Pickup.
    *   `driver_response_time_min`: Request $\to$ On Scene.
    *   `boarding_time_min`: On Scene $\to$ Pickup.

### **B. Geospatial Intelligence**
*   **Geometry:** Joined `taxi_zones.shp` centroids to `PULocationID` / `DOLocationID`.
*   **Metrics:**
    *   `straight_line_dist_km`: Haversine formula between centroids.
    *   `tortuosity_index`: `trip_km` / `straight_line_dist_km`. (>1.5 implies inefficiency/detour).
    *   `bearing_degrees`: Direction of travel (0-360°).
*   **Flow:** Categorized `borough_flow_type` (e.g., `manhattan_outer_commute`) to isolate transit patterns.

### **C. Economic Reconstruction**
*   **Total Cost:** Summation of all 8 financial components (Fare + Tolls + Tips + Surcharge + Airport + Tax + BCF + CBD Fee).
*   **Unit Economics:**
    *   `cost_per_km`: Yield per kilometer.
    *   `pay_per_hour`: Driver earnings normalized by duration.
    *   `uber_take_rate_proxy`: `1 - (Driver Pay / Base Fare)`. (Driver Pay / Base Fare) is an industry-standard metric commonly used to calculate the driver revenue share. We don't have IDs to identify the driver, or the vehicle, so this metric is not 100% accurate - only a proxy.
    *   `is_subsidized`: Flagged where `Driver Pay > Base Fare` (Platform loss-leader), or in other words, trips where `driver_revenue_share` > 100%.

### **D. Meteorological Integration**
*   **Source:** Visual Crossing API (New York, Hourly).
*   **Join Key:** `pickup_datetime` truncated to hour.
*   **Logic:** Categorical assignment based on intensity thresholds. Derived from standard meteorological thresholds, but slightly adjusted to account for NYC specifically, in the context of traffic (impact on human).
    * **Temperature:** `Freezing` ($<0$), `Cold` ($0-10$), `Mild` ($10-20$), `Warm` ($20-28$), `Hot` ($>28$).
    * **Rain:** `Light` ($<1mm$), `Moderate` ($1-5mm$), `Heavy` ($>5mm$).
    * **Snow:** `Trace` ($<2.5cm$), `Moderate` ($2.5-10cm$), `Heavy` ($10-20cm$), `Severe` ($>20cm$).
    * **Wind:** `Breezy` ($15-40km/h$), `Windy` ($40-62km/h$), `Gale` ($>62km/h$).
    * **Visibility:** `Reduced` ($1-10km$), `Poor/Fog` ($<1km$).
*   **State Hierarchy:** `Snowing` overrides `Raining` overrides `Cloudy`.

---

## 3. Known Limitations & Context
1.  **Driver Anonymity:** HVFHS data does not contain unique Driver IDs. Longitudinal analysis (shift length, total daily income) is impossible.
2.  **Wait Time Proxy:** `request_datetime` relies on app telemetry. High wait times may reflect passenger lateness ("curb time"), not just driver delay.
3.  **Centroid Assumption:** Spatial metrics (`straight_line_dist`, `bearing`) use Zone Centroids. Intra-zone trips have `straight_line_dist = 0`.
4.  **Weather Localization:** Weather data is sourced from Visual Crossing, using the location "New York, NY, United States". Weather outside of NYC, even if close (like EWR) might not match weather inside NYC.
