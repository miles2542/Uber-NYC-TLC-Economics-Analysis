# Modular Website Structure

## Overview
The website has been refactored into a modular architecture for easier content management and updates.

## File Structure

```
ui/
├── index.html              # Main HTML file (now only 221 lines - containers only)
├── index_backup.html       # Backup of original monolithic file
├── css/
│   └── style.css          # Custom styles
├── js/
│   └── app.js             # Refactored JS - dynamically loads sections
├── sections/              # ✨ NEW: Modular section files
│   ├── overview.html
│   ├── acquisition.html
│   ├── architecture.html
│   ├── transformation.html
│   ├── feature-engineering.html
│   ├── aggregation.html
│   ├── impact.html
│   ├── reproduction.html
│   └── legal.html
├── serve.py               # Local test server
├── extract_sections.py    # Utility script (already run)
└── update_index.py        # Utility script (already run)
```

## Key Changes

### 1. **Modular Sections**
- Each major section is now in its own HTML file in `sections/`
- Easy to find, edit, and manage individual sections
- No more scrolling through 940 lines of HTML

### 2. **Dynamic Loading**
- `app.js` now uses `fetch()` to load section content on page load
- Sections are injected into their respective containers
- All features (dark mode, sticky header, scroll spy) work as before

### 3. **Removed Features**
- ✅ **Data Dictionary Search** - Removed from all sections as requested
- Search input and filtering logic completely removed

### 4. **Simplified index.html**
- Reduced from **940 lines** to **221 lines**
- Only contains:
  - Header (sticky header with theme toggle)
  - Left rail navigation
  - Route scrollbar
  - Section containers (empty divs)
  - Footer

## How to Test Locally

Since the website now uses `fetch()` to load sections, you **must** run it through a local server (GitHub Pages will work fine).

### Option 1: Using the included server
```bash
python serve.py
```
Then open: http://localhost:8000

### Option 2: Using Python's built-in server
```bash
cd ui
python -m http.server 8000
```

### Option 3: Using Node.js
```bash
npx http-server ui -p 8000
```

## Editing Content

To modify content in any section:

1. Navigate to `sections/[section-name].html`
2. Edit the HTML directly
3. Refresh the browser (no build step needed)

Example: To update the Overview section, edit `sections/overview.html`

## GitHub Pages Compatibility

✅ This structure works perfectly with GitHub Pages:
- No build process required
- All files are static HTML/CSS/JS
- Dynamic loading works via fetch (same-origin)

## Notes

- The original monolithic `index.html` is backed up as `index_backup.html`
- All styling and JavaScript functionality remains unchanged
- The website is still a single-page application (SPA)
- Sections load asynchronously but appear instantly on modern browsers
