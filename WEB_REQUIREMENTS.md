# **`WEB_REQUIREMENTS.md`**

## **1. Project Identity & Goal**
*   **Goal:** Convert a technical Data Science `README.md` into a high-end, immersive "Data Product" website hosted on GitHub Pages.
*   **Visual Style:** **Uber Base Design System** (adapted). Minimalist, geometric, high-contrast, professional.
*   **Core Vibe:** "Radical Clarity." The site should feel like using the Uber app—smooth, frictionless, and premium.

## **2. Technical Constraints**
*   **Stack:** Pure **HTML5, CSS3 (Tailwind CSS allowed), Vanilla JavaScript**. No complex build steps (React/Vue) unless compiled to a single deployable `index.html` structure.
*   **Deployment:** Must run natively on GitHub Pages (static hosting).
*   **Responsiveness:** Mobile-responsive, but optimized for **Desktop/Laptop** viewing (where the charts live).

## **3. Asset & Resource Protocol**
*   **Rule Zero (No Assumptions):** If a specific icon (SVG), logo, or font file is required but the URL is unknown, **DO NOT generate a placeholder.**
    *   **Action:** Stop and ask the user: *"I need the SVG for [Asset Name]. Please provide the code or a link."*
*   **Charts:** All plots will be embedded as `<iframe>` or interactive `<div>` containers holding Plotly HTML exports.

## **4. Core Layout & UI Architecture**

### **4.1. The "Canvas"**
*   **Width:** Centered container (`max-w-4xl`), breathable margins (`px-6` or `px-12`).
*   **Typography:** Sans-serif stack (`Uber Move` proxy: Inter or Roboto). High contrast.

### **4.2. Navigation: The "Intelligent Left Rail"**
*   **Appearance:** A thin vertical line (`Gray 300`) on the left edge.
*   **Passive State:** A black dot tracks the user's scroll position.
*   **Hover State:** The rail expands to show the full Table of Contents.
*   **Easter Egg (The "ETA"):** When hovering over a link, display an **Estimated Reading Time** styled like a ride arrival.
    *   *Format:* "Feature Engineering • **2 min away**"

### **4.3. The "Route" Scrollbar**
*   **Concept:** Replace the browser's default scrollbar.
*   **Track:** A thin "Road" line.
*   **Thumb:** A minimalist **Car Icon (Top-Down View)** that drives down the page as the user scrolls.

## **5. Key Interactive Modules (The "Features")**

### **5.1. The Sticky Header: "Raw vs. Processed"**
*   **Position:** Sticky top.
*   **UI:** A "Status Bar" with a Toggle Switch: `[ RAW | PROCESSED ]`.
*   **Behavior (Auto):**
    *   Page Top: Switch is on **RAW**. Data Cards show raw stats (1.4B rows).
    *   Scroll past "Cleaning": Switch slides to **PROCESSED**. Data Cards update (983M rows).
*   **Behavior (Manual):** User can click to override. Clicking disables auto-scroll logic.

### **5.2. Distribution Slider (Vertical)**
*   **Usage:** Comparing "Raw Data Distribution" vs "Cleaned Data Distribution" (Section 4.2).
*   **Interaction:** A vertical handle. Dragging down reveals the "Cleaned" image overlaying the "Raw" image.
*   **Constraint:** **Handle-Only Activation.** The slider must NOT move with mouse scroll. User must click-and-drag the handle.

### **5.3. Dictionary "Highlight" Search**
*   **Section:** Processed Data Dictionary.
*   **UI:** A minimal search input: *"Search features (e.g. 'tip')..."*
*   **Behavior:**
    *   **Do Not Hide** non-matches (preserves context).
    *   **Dim** non-matches (Opacity 0.3).
    *   **Highlight** matches (Yellow/Green background).
    *   Auto-expand Accordion group if a match is found inside.

### **5.4. Pipeline Animation**
*   **Format:** SVG / CSS Animation.
*   **Content:** A flowchart of the ETL process.
*   **Interaction:** Simple flow animation (dots moving through pipes). Hovering over nodes shows specific stats (e.g., "Feature Engine: +45 Cols").

### **5.5. Reproduction: "Ride Request" UI**
*   **Concept:** Styling the download links like the Uber App vehicle selector.
*   **Option 1 (UberX):** "Fast Track (Download Data)" - *Est. 2 mins*.
*   **Option 2 (Uber Black):** "Full Rebuild (Run Code)" - *Est. 45 mins*.

### **5.6. The 5-Star Footer**
*   **Content:**
    *   "Your Driver: **Team [Name]**"
    *   "Vehicle: **Python / Polars**"
    *   "Rating: **5.0 ⭐**"

## **6. "Uber Black" Mode (Dark Theme)**

### **6.1. Activation**
*   **UI:** A toggle switch in the Top Right corner labeled **"Uber Black"**.
*   **Transition:** Smooth CSS transition (`0.3s ease-in-out`).

### **6.2. Color Mapping**
| Element             | Light Mode (UberX)     | Dark Mode (Uber Black)                      |
| :------------------ | :--------------------- | :------------------------------------------ |
| **Background**      | `#FFFFFF` (White)      | `#000000` (True Black)                      |
| **Card / Surface**  | `#F6F6F6` (Gray 100)   | `#121212` (Dark Gray)                       |
| **Primary Text**    | `#000000` (Black)      | `#FFFFFF` (White)                           |
| **Secondary Text**  | `#545454` (Gray 600)   | `#AFAFAF` (Gray 500)                        |
| **Accent (Action)** | `#47B275` (Uber Green) | `#FFFFFF` (White) or `#47B275` (Keep Green) |
| **Borders/Lines**   | `#E2E2E2` (Gray 300)   | `#333333` (Dark Gray)                       |

*   **Note:** Charts (Plotly) are usually white-background images or IFrames. If possible, apply a CSS filter `invert(1) hue-rotate(180deg)` **ONLY** if the charts are simple black/white lines. **OTHERWISE**, keep charts in a "White Card" container to preserve color accuracy. *Prefer "White Card" method for safety.*
