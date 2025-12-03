"""
Generate complete feature-engineering.html with ALL 76 features from README
"""

import re

# Read README
readme_path = r"x:\Programming\Python\[Y3S1] Year 3, Autumn semester\[Y3S1] Data preparation and Visualisation\Projects\Final term (hck)\TLC NYC filtered\ui\README_content (almost complete).md"

with open(readme_path, "r", encoding="utf-8") as f:
    lines = f.readlines()


# Helper to convert markdown table to HTML
def parse_table_section(start_line, end_line):
    features = []
    for i in range(start_line, min(end_line, len(lines))):
        line = lines[i].strip()
        if (
            line.startswith("|")
            and "|" in line[1:]
            and not ("Feature Name" in line or ":---" in line)
        ):
            parts = [p.strip() for p in line.split("|")[1:-1]]
            if len(parts) >= 3 and parts[0] and not parts[0].startswith(":"):
                # Feature name, definition/type, rationale
                features.append(
                    {
                        "name": parts[0].replace("`", ""),
                        "type": parts[1] if len(parts) > 3 else "",
                        "definition": parts[2] if len(parts) > 3 else parts[1],
                        "range": parts[3] if len(parts) > 4 else "",
                        "rationale": parts[4]
                        if len(parts) > 4
                        else (parts[2] if len(parts) > 2 else ""),
                    }
                )
    return features


# Parse all 6 groups
group1 = parse_table_section(233, 252)  # Temporal (17)
group2 = parse_table_section(257, 269)  # Geospatial (10)
group3 = parse_table_section(274, 287)  # Physics (11)
group4 = parse_table_section(292, 311)  # Financials (17)
group5 = parse_table_section(315, 322)  # Service Flags (5)
group6 = parse_table_section(326, 338)  # Meteorological (10)

print(
    f"Parsed: G1={len(group1)}, G2={len(group2)}, G3={len(group3)}, G4={len(group4)}, G5={len(group5)}, G6={len(group6)}"
)
print(
    f"Total: {len(group1) + len(group2) + len(group3) + len(group4) + len(group5) + len(group6)}"
)

# Generate HTML
html_template = """<section id="feature-engineering" class="mb-24 scroll-mt-32">
    <h2 class="text-3xl font-bold mb-8 flex items-center gap-3">
        <span class="text-uber-green">3.3</span> Feature Engineering: The Insight Engine
    </h2>
    <div class="prose prose-lg dark:prose-invert max-w-none">
        <p class="mb-6">
            Cleaning the data was only the first step. To answer questions about "Inefficiency," "Nightlife,"
            and "Inflation," we engineered <strong>over 45 new features</strong>, transforming raw logs into a rich analytical dataset.
        </p>

        <!-- Subsection descriptions -->
        <h4 class="text-xl font-semibold mb-3">1. Temporal Reconstruction</h4>
        <p class="mb-2">Raw timestamps (<code>2019-02-01 14:32:05</code>) are useless for behavioral analysis. We deconstructed time into:</p>
        <ul class="list-disc pl-6 space-y-1 mb-6 text-uber-gray600 dark:text-uber-gray500">
            <li><strong>Granularity:</strong> Extracted <code>Year</code>, <code>Month</code>, <code>Day</code>, <code>Hour</code>, <code>DOW</code> (Monday=1, Sunday=7).</li>
            <li><strong>Cyclical Features:</strong> Converted Hour/Day/Month into <code>Sin</code> and <code>Cos</code> components. This allows ML models to understand that Hour 23 (11 PM) is "close" to Hour 0 (Midnight), preventing the "cliff" effect of linear time.</li>
            <li><strong>Cultural Time:</strong> We created <code>cultural_day_type</code> to map rides to human behavior, not the calendar. <em>Example:</em> A ride at 2 AM Saturday is technically "Saturday Morning," but culturally it is <strong>"Friday Night Nightlife."</strong> This feature captures that nuance.</li>
            <li><strong>Pandemic Phase:</strong> We tagged every ride with its COVID era (<code>Pre-Pandemic</code>, <code>Lockdown</code>, <code>Recovery</code>, <code>New Normal</code>) to enable Era-based comparisons.</li>
        </ul>

        <h4 class="text-xl font-semibold mb-3">2. Geospatial Intelligence</h4>
        <p class="mb-2">We moved beyond simple "Zone IDs" to measure flow and efficiency.</p>
        <ul class="list-disc pl-6 space-y-1 mb-6 text-uber-gray600 dark:text-uber-gray500">
            <li><strong>Centroid Physics:</strong> By joining Shapefiles, we calculated the <strong>Straight-Line Distance</strong> (Haversine) between Pickup and Dropoff centroids.</li>
            <li><strong>Tortuosity Index:</strong> The ratio of <code>Driven Distance / Straight Line Distance</code>. <em>Why?</em> This is our "Gridlock Detector." A high index (>1.5) means the driver had to take a massive detour (highway) or got lost in complex streets. It separates "Distance due to Speed" from "Distance due to Inefficiency."</li>
            <li><strong>Borough Flow:</strong> We categorized trips into transit archetypes: <code>Manhattan Internal</code>, <code>Manhattan &lt;-&gt; Outer</code>, <code>Outer &lt;-&gt; Outer</code>. This highlights the "Transit Desert" economy where Uber replaces the G subway.</li>
        </ul>

        <h4 class="text-xl font-semibold mb-3">3. Economic Reconstruction</h4>
        <p class="mb-2">We rebuilt the entire financial stack to track money flow.</p>
        <ul class="list-disc pl-6 space-y-1 mb-6 text-uber-gray600 dark:text-uber-gray500">
            <li><strong>Total Cost:</strong> We summed all 8 financial components (Fare, Tips, Surcharges, Taxes, Tolls, Fees) to derive the <strong>Gross Booking Value (GBV)</strong>—the actual price paid by the rider.</li>
            <li><strong>The "Subsidy" Detector:</strong> We calculated <code>driver_revenue_share</code> (Driver Pay / Base Fare). <em>Why?</em> This acts as a proxy for platform subsidies, and also the proxy for Uber take rate (1 - driver_revenue_share) - while not accurate (actual values are classified), it gives us a general sense of how much Uber is taking from each fare, and how much of the base fare for each trip is going to the driver.</li>
            <li><strong>Generosity Metrics:</strong> We standardized tipping behavior into <code>tipping_pct</code> and <code>is_generous_tip</code> (>25%) to study the psychology of gratitude versus obligation.</li>
        </ul>

        <h4 class="text-xl font-semibold mb-3">4. Meteorological Context</h4>
        <p class="mb-2">We didn't just dump raw weather data; we categorized it based on <strong>human impact</strong>, binned into intensity thresholds. Derived from standard meteorological thresholds, but slightly adjusted to account for NYC specifically, in the context of traffic impact.</p>
        <ul class="list-disc pl-6 space-y-1 mb-6 text-uber-gray600 dark:text-uber-gray500">
            <li><strong>Temperature:</strong> <code>Freezing</code> (&lt;0), <code>Cold</code> (0-10), <code>Mild</code> (10-20), <code>Warm</code> (20-28), <code>Hot</code> (>28).</li>
            <li><strong>Rain:</strong> <code>Light</code> (&lt;1mm), <code>Moderate</code> (1-5mm), <code>Heavy</code> (>5mm).</li>
            <li><strong>Snow:</strong> <code>Trace</code> (&lt;2.5cm), <code>Moderate</code> (2.5-10cm), <code>Heavy</code> (10-20cm), <code>Severe</code> (>20cm).</li>
            <li><strong>Wind:</strong> <code>Breezy</code> (15-40km/h), <code>Windy</code> (40-62km/h), <code>Gale</code> (>62km/h).</li>
            <li><strong>Visibility:</strong> <code>Reduced</code> (1-10km), <code>Poor/Fog</code> (&lt;1km).</li>
            <li><strong>The "Chaos" Hierarchy:</strong> We created a <code>weather_state</code> categorical that prioritizes disruption. <em>Logic:</em> If it is raining <em>and</em> snowing, the state is <strong>Snowing</strong> (because snow have larger impact on traffic than simply raining).</li>
        </ul>

        <h3 class="text-2xl font-semibold mb-4 mt-12">Processed Data Dictionary (Schema)</h3>
        <p class="text-sm text-uber-gray600 dark:text-uber-gray500 mb-6"><em>Total Features: 70. Grouped by Analytical Domain.</em></p>
"""


# Generate each group's HTML
def generate_group_html(group_name, group_num, features):
    html = f"""
        <div class="mb-10">
            <h4 class="text-xl font-bold mb-4 flex items-center gap-2">
                <span class="w-2 h-8 bg-uber-black dark:bg-uber-white rounded-sm"></span>
                {group_name} ({len(features)} Features)
            </h4>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead class="bg-uber-gray100 dark:bg-uber-darkcard text-uber-gray600 dark:text-uber-gray500 uppercase tracking-wider">
                        <tr>
                            <th class="p-3 rounded-tl-lg">Feature Name</th>
                            <th class="p-3">Type</th>
                            <th class="p-3">Definition</th>
                            <th class="p-3">Range / Values</th>
                            <th class="p-3 rounded-tr-lg">Rationale & Usage</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-uber-gray300 dark:divide-uber-darkborder">
"""
    for feat in features:
        html += f"""                        <tr>
                            <td class="p-3 font-mono text-uber-green">{feat["name"]}</td>
                            <td class="p-3">{feat.get("type", "")}</td>
                            <td class="p-3">{feat.get("definition", "")}</td>
                            <td class="p-3">{feat.get("range", "")}</td>
                            <td class="p-3 text-uber-gray600 dark:text-uber-gray500">{feat.get("rationale", "")}</td>
                        </tr>
"""
    html += """                    </tbody>
                </table>
            </div>
        </div>
"""
    return html


html_template += generate_group_html("Group 1: Temporal Context", 1, group1)
html_template += generate_group_html("Group 2: Geospatial & Trip Context", 2, group2)
html_template += generate_group_html("Group 3: Physics & Service Metrics", 3, group3)
html_template += generate_group_html("Group 4: Financials & Economics", 4, group4)
html_template += generate_group_html("Group 5: Service Flags", 5, group5)
html_template += generate_group_html("Group 6: Meteorological Context", 6, group6)

html_template += """    </div>
</section>"""

# Write output
output_path = r"x:\Programming\Python\[Y3S1] Year 3, Autumn semester\[Y3S1] Data preparation and Visualisation\Projects\Final term (hck)\TLC NYC filtered\ui\sections\feature-engineering.html"
with open(output_path, "w", encoding="utf-8") as f:
    f.write(html_template)

print(
    f"✓ Generated feature-engineering.html with {len(group1) + len(group2) + len(group3) + len(group4) + len(group5) + len(group6)} features"
)
