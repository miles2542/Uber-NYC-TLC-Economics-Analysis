"""
Extract sections from index.html into separate modular files
"""
from pathlib import Path
import re

# Read the current index.html
ui_dir = Path(r"x:\Programming\Python\[Y3S1] Year 3, Autumn semester\[Y3S1] Data preparation and Visualisation\Projects\Final term (hck)\TLC NYC filtered\ui")
index_file = ui_dir / "index.html"
sections_dir = ui_dir / "sections"
sections_dir.mkdir(exist_ok=True)

with open(index_file, 'r', encoding='utf-8') as f:
    content = f.read()

# Define section patterns to extract
section_patterns = {
    'acquisition': (r'<!-- Data Acquisition -->(.*?)(?=<!-- Technical Architecture -->)', re.DOTALL),
    'architecture': (r'<!-- Technical Architecture -->(.*?)(?=<!-- Transformation Pipeline)', re.DOTALL),
    'transformation': (r'<!-- Transformation Pipeline \(The Meat\) -->(.*?)(?=<!-- Feature Engineering -->)', re.DOTALL),
    'feature-engineering': (r'<!-- Feature Engineering -->(.*?)(?=<!-- Aggregation Strategy -->)', re.DOTALL),
    'aggregation': (r'<!-- Aggregation Strategy -->(.*?)(?=<!-- Impact Showcase -->)', re.DOTALL),
    'impact': (r'<!-- Impact Showcase -->(.*?)(?=<!-- Reproduction \(Accordion UI\) -->)', re.DOTALL),
    'reproduction': (r'<!-- Reproduction \(Accordion UI\) -->(.*?)(?=<!-- Legal -->)', re.DOTALL),
    'legal': (r'<!-- Legal -->(.*?)(?=<footer)', re.DOTALL),
}

# Extract and save each section
for section_name, (pattern, flags) in section_patterns.items():
    match = re.search(pattern, content, flags)
    if match:
        section_content = match.group(1).strip()
        output_file = sections_dir / f"{section_name}.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(section_content)
        print(f"✓ Extracted {section_name}.html")
    else:
        print(f"✗ Could not find section: {section_name}")

print("\n✓ All sections extracted successfully!")
