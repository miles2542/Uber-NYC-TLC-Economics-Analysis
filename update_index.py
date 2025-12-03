"""
Update index.html to use section containers instead of inline content
"""

from pathlib import Path
import re

ui_dir = Path(
    r"x:\Programming\Python\[Y3S1] Year 3, Autumn semester\[Y3S1] Data preparation and Visualisation\Projects\Final term (hck)\TLC NYC filtered\ui"
)
index_file = ui_dir / "index.html"
backup_file = ui_dir / "index_backup.html"

# Backup the original
with open(index_file, "r", encoding="utf-8") as f:
    original_content = f.read()

with open(backup_file, "w", encoding="utf-8") as f:
    f.write(original_content)

# Define replacement patterns
replacements = [
    # Overview - keep as is (it's already in the file)
    # Acquisition
    (
        r"<!-- Data Acquisition -->.*?</section>",
        '<!-- Data Acquisition -->\n        <div id="acquisition-container"></div>',
        re.DOTALL,
    ),
    # Architecture
    (
        r"<!-- Technical Architecture -->.*?(?=<!-- Transformation Pipeline)",
        '<!-- Technical Architecture -->\n        <div id="architecture-container"></div>\n\n        ',
        re.DOTALL,
    ),
    # Transformation
    (
        r"<!-- Transformation Pipeline \(The Meat\) -->.*?(?=<!-- Feature Engineering -->)",
        '<!-- Transformation Pipeline -->\n        <div id="transformation-container"></div>\n\n        ',
        re.DOTALL,
    ),
    # Feature Engineering
    (
        r"<!-- Feature Engineering -->.*?(?=<!-- Aggregation Strategy -->)",
        '<!-- Feature Engineering -->\n        <div id="feature-engineering-container"></div>\n\n        ',
        re.DOTALL,
    ),
    # Aggregation
    (
        r"<!-- Aggregation Strategy -->.*?(?=<!-- Impact Showcase -->)",
        '<!-- Aggregation Strategy -->\n        <div id="aggregation-container"></div>\n\n        ',
        re.DOTALL,
    ),
    # Impact
    (
        r"<!-- Impact Showcase -->.*?(?=<!-- Reproduction \(Accordion UI\) -->)",
        '<!-- Impact Showcase -->\n        <div id="impact-container"></div>\n\n        ',
        re.DOTALL,
    ),
    # Reproduction
    (
        r"<!-- Reproduction \(Accordion UI\) -->.*?(?=<!-- Legal -->)",
        '<!-- Reproduction -->\n        <div id="reproduction-container"></div>\n\n        ',
        re.DOTALL,
    ),
    # Legal
    (
        r"<!-- Legal -->.*?(?=<footer)",
        '<!-- Legal -->\n        <div id="legal-container"></div>\n\n        ',
        re.DOTALL,
    ),
]

# Apply replacements
content = original_content
for pattern, replacement, flags in replacements:
    content = re.sub(pattern, replacement, content, flags=flags)

# Also update the stat IDs to match the new JS
content = content.replace('id="stat-rows"', 'id="rows-value"')
content = content.replace('id="stat-size"', 'id="size-value"')

# Write the updated content
with open(index_file, "w", encoding="utf-8") as f:
    f.write(content)

print("✓ index.html updated successfully!")
print(f"✓ Backup saved to: {backup_file}")
