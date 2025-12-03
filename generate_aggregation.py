"""
Generate complete aggregation.html with ALL 4 data mart dictionaries from README
"""

# Read README lines 355-463 for Aggregation section
readme_path = r"x:\Programming\Python\[Y3S1] Year 3, Autumn semester\[Y3S1] Data preparation and Visualisation\Projects\Final term (hck)\TLC NYC filtered\ui\README_content (almost complete).md"

with open(readme_path, "r", encoding="utf-8") as f:
    lines = f.readlines()


# Helper to parse table
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
                features.append(
                    {
                        "name": parts[0].replace("`", ""),
                        "type": parts[1],
                        "definition": parts[2],
                        "rationale": parts[3] if len(parts) > 3 else "",
                    }
                )
    return features


# Parse all 4 data marts
mart1 = parse_table_section(387, 406)  # Timeline
mart2 = parse_table_section(412, 426)  # Network
mart3 = parse_table_section(432, 447)  # Pricing
mart4 = parse_table_section(452, 463)  # Executive

print(
    f"Parsed marts: M1={len(mart1)}, M2={len(mart2)}, M3={len(mart3)}, M4={len(mart4)}"
)

# Generate HTML
html_output = """<section id="aggregation" class="mb-24 scroll-mt-32">
    <h2 class="text-3xl font-bold mb-8 flex items-center gap-3">
        <span class="text-uber-green">4.</span> Aggregation & Sampling Strategy
    </h2>
    <div class="prose prose-lg dark:prose-invert max-w-none">
        
        <h3 class="text-xl font-semibold mb-4">The "Two-Tier" Data Architecture</h3>
        <p class="mb-6">
            A 70GB dataset is too large for rapid exploration and visualization. We solved this by splitting the output into two distinct tiers, serving different analytical needs.
        </p>

        <h4 class="text-2xl font-semibold mb-3">Tier 1: Stratified Sampling</h4>
        <ul class="list-disc pl-6 space-y-2 mb-8 text-uber-gray600 dark:text-uber-gray500">
            <li><strong>File:</strong> <code>tlc_sample_20XX.parquet</code> (split yearly, or monthly - can be configured).</li>
            <li><strong>Size:</strong> ~800MB total (1% of population) - that is still 10M rows!</li>
            <li><strong>Methodology:</strong>
                <ul class="list-circle pl-6 mt-2">
                    <li>We applied a <strong>1% Stratified Random Sample</strong> to each monthly file.</li>
                    <li><strong>Why Stratified?</strong> Randomly sampling the whole dataset might bias towards recent years (higher volume), or randomly sampling most lower fare trips, etc. Our method ensures Jan 2019 is represented exactly as proportionally as Jan 2025, and for most use cases, simply multiplying back up by 100x gives a decent estimate of totals, preserving the trends and more.</li>
                </ul>
            </li>
            <li><strong>Use Case:</strong> This dataset preserves the <strong>micro-structure</strong> of the data. Use it for:
                <ul class="list-circle pl-6 mt-2">
                    <li>Distribution analysis (Boxplots, Histograms).</li>
                    <li>Correlation studies (e.g., "Do tips increase when speed decreases?").</li>
                    <li>Machine Learning training.</li>
                </ul>
            </li>
        </ul>

        <h4 class="text-2xl font-semibold mb-3">Tier 2: Aggregated Data Marts</h4>
        <ul class="list-disc pl-6 space-y-2 mb-8 text-uber-gray600 dark:text-uber-gray500">
            <li><strong>Files:</strong> 4 Aggregated Parquet/CSV files.</li>
            <li><strong>Size:</strong> ~250MB total.</li>
            <li><strong>Methodology:</strong>
                <ul class="list-circle pl-6 mt-2">
                    <li>These files contain <strong>100% of the data volume</strong>, pre-aggregated by specific dimensions.</li>
                    <li>Because they use the full population, they are the "Ground Truth" for volume and revenue reporting.</li>
                </ul>
            </li>
            <li><strong>Use Case:</strong> High-level dashboards, Maps, and KPI tracking.</li>
        </ul>

        <h3 class="text-2xl font-semibold mb-6 mt-12">Aggregate Data Dictionaries</h3>

        <!-- Mart 1 -->
        <div class="mb-10">
            <h4 class="text-xl font-bold mb-3">Mart 1: The Timeline Backbone (<code>agg_timeline_hourly.parquet</code>)</h4>
            <ul class="list-disc pl-6 space-y-1 mb-4 text-sm text-uber-gray600 dark:text-uber-gray500">
                <li><strong>Grain:</strong> Hourly.</li>
                <li><strong>Purpose:</strong> The source of truth for volume trends, seasonality, and long-term growth. It captures the exact moment demand collapsed during COVID and the recovery curve, and everything else in between.</li>
            </ul>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead class="bg-uber-gray100 dark:bg-uber-darkcard text-uber-gray600 dark:text-uber-gray500 uppercase tracking-wider">
                        <tr>
                            <th class="p-3 rounded-tl-lg">Feature Name</th>
                            <th class="p-3">Type</th>
                            <th class="p-3">Definition</th>
                            <th class="p-3 rounded-tr-lg">Rationale</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-uber-gray300 dark:divide-uber-darkborder">
"""

for feat in mart1:
    html_output += f"""                        <tr>
                            <td class="p-3 font-mono text-uber-green">{feat["name"]}</td>
                            <td class="p-3">{feat["type"]}</td>
                            <td class="p-3">{feat["definition"]}</td>
                            <td class="p-3 text-uber-gray600 dark:text-uber-gray500">{feat["rationale"]}</td>
                        </tr>
"""

html_output += """                    </tbody>
                </table>
            </div>
        </div>

        <!-- Mart 2 -->
        <div class="mb-10">
            <h4 class="text-xl font-bold mb-3">Mart 2: The Network Backbone (<code>agg_network_monthly.parquet</code>)</h4>
            <ul class="list-disc pl-6 space-y-1 mb-4 text-sm text-uber-gray600 dark:text-uber-gray500">
                <li><strong>Grain:</strong> Monthly by Route (Origin-Destination).</li>
                <li><strong>Purpose:</strong> The engine for geospatial analysis. By pre-calculating the volume and speed for every Zone-to-Zone pair, we enable instant heatmap generation without crunching a billion rows.</li>
            </ul>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead class="bg-uber-gray100 dark:bg-uber-darkcard text-uber-gray600 dark:text-uber-gray500 uppercase tracking-wider">
                        <tr>
                            <th class="p-3 rounded-tl-lg">Feature Name</th>
                            <th class="p-3">Type</th>
                            <th class="p-3">Definition</th>
                            <th class="p-3 rounded-tr-lg">Rationale</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-uber-gray300 dark:divide-uber-darkborder">
"""

for feat in mart2:
    html_output += f"""                        <tr>
                            <td class="p-3 font-mono text-uber-green">{feat["name"]}</td>
                            <td class="p-3">{feat["type"]}</td>
                            <td class="p-3">{feat["definition"]}</td>
                            <td class="p-3 text-uber-gray600 dark:text-uber-gray500">{feat["rationale"]}</td>
                        </tr>
"""

html_output += """                    </tbody>
                </table>
            </div>
        </div>

        <!-- Mart 3 -->
        <div class="mb-10">
            <h4 class="text-xl font-bold mb-3">Mart 3: The Economic Backbone (<code>agg_pricing_distribution.parquet</code>)</h4>
            <ul class="list-disc pl-6 space-y-1 mb-4 text-sm text-uber-gray600 dark:text-uber-gray500">
                <li><strong>Grain:</strong> Daily by Context (Weather/Time).</li>
                <li><strong>Purpose:</strong> A deep dive into the "Wallet War." It tracks how Driver Pay Share and Pricing Surges fluctuate based on weather and time of day.</li>
            </ul>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead class="bg-uber-gray100 dark:bg-uber-darkcard text-uber-gray600 dark:text-uber-gray500 uppercase tracking-wider">
                        <tr>
                            <th class="p-3 rounded-tl-lg">Feature Name</th>
                            <th class="p-3">Type</th>
                            <th class="p-3">Definition</th>
                            <th class="p-3 rounded-tr-lg">Rationale</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-uber-gray300 dark:divide-uber-darkborder">
"""

for feat in mart3:
    html_output += f"""                        <tr>
                            <td class="p-3 font-mono text-uber-green">{feat["name"]}</td>
                            <td class="p-3">{feat["type"]}</td>
                            <td class="p-3">{feat["definition"]}</td>
                            <td class="p-3 text-uber-gray600 dark:text-uber-gray500">{feat["rationale"]}</td>
                        </tr>
"""

html_output += """                    </tbody>
                </table>
            </div>
        </div>

        <!-- Mart 4 -->
        <div class="mb-10">
            <h4 class="text-xl font-bold mb-3">Mart 4: The Executive Summary (<code>agg_executive_daily.csv</code>)</h4>
            <ul class="list-disc pl-6 space-y-1 mb-4 text-sm text-uber-gray600 dark:text-uber-gray500">
                <li><strong>Grain:</strong> Daily (Global).</li>
                <li><strong>Purpose:</strong> A lightweight CSV for quick KPI cards (Total Revenue, Total Volume) and Excel-based "Big Number" tracking.</li>
            </ul>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead class="bg-uber-gray100 dark:bg-uber-darkcard text-uber-gray600 dark:text-uber-gray500 uppercase tracking-wider">
                        <tr>
                            <th class="p-3 rounded-tl-lg">Feature Name</th>
                            <th class="p-3">Type</th>
                            <th class="p-3">Definition</th>
                            <th class="p-3 rounded-tr-lg">Rationale</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-uber-gray300 dark:divide-uber-darkborder">
"""

for feat in mart4:
    html_output += f"""                        <tr>
                            <td class="p-3 font-mono text-uber-green">{feat["name"]}</td>
                            <td class="p-3">{feat["type"]}</td>
                            <td class="p-3">{feat["definition"]}</td>
                            <td class="p-3 text-uber-gray600 dark:text-uber-gray500">{feat["rationale"]}</td>
                        </tr>
"""

html_output += """                    </tbody>
                </table>
            </div>
        </div>

    </div>
</section>"""

# Write output
output_path = r"x:\Programming\Python\[Y3S1] Year 3, Autumn semester\[Y3S1] Data preparation and Visualisation\Projects\Final term (hck)\TLC NYC filtered\ui\sections\aggregation.html"
with open(output_path, "w", encoding="utf-8") as f:
    f.write(html_output)

print(
    f"✓ Generated aggregation.html with 4 data marts ({len(mart1) + len(mart2) + len(mart3) + len(mart4)} features total)"
)
