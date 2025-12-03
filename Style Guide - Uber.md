# **Uber Data Visualization Style Guide: The "Power of Prep" Edition**

**Version:** 1.3

**Target Output:** High-Definition Business Intelligence (Plotly/Python)

**Core Philosophy:** Radical Clarity, Intentionality, and "Order from Chaos."

---

## **I. The Visual Philosophy**

Our visualizations do not look like standard Matplotlib charts, nor even slightly customized Seaborn/Plotly ones. They look like **native UI components** of the Uber ecosystem or high-end investor reports.

1.  **Data Ink Ratio:** Maximal. If it doesn't inform, delete it.
2.  **The "White Room" Approach:** We use a pure white canvas. Data must stand out through contrast, not saturation.
3.  **Hierarchy of Intent:** We do not simply plot data. We direct the eye.
    *   **Level 1 (The Insight):** Uber Green / Bold Typography.
    *   **Level 2 (The Data):** Uber Black / Geometric Shapes.
    *   **Level 3 (The Context):** Cool Grays / Fine Lines.

---

## **II. The Color System**

We utilize the **Uber Base Web** palette, adapted for data visualization contrast requirements on a White background.

### **2.1. Primary Data Palette (The "Traffic" System)**
These are the only colors allowed for primary data representation.

| Role                 | Color Name      | Hex       | RGB            | Usage Rules                                                                                              |
| :------------------- | :-------------- | :-------- | :------------- | :------------------------------------------------------------------------------------------------------- |
| **Primary Positive** | **Uber Green**  | `#47B275` | `71, 178, 117` | **The Hero Color.** Used for growth, "Clean Data," active states, and the main data story.               |
| **Primary Neutral**  | **Uber Black**  | `#000000` | `0, 0, 0`      | **The Body Color.** Used for raw data, volume, totals, and "After" states where green is too aggressive. |
| **Critical/Alert**   | **Uber Red**    | `#F25138` | `242, 81, 56`  | **Negative Only.** Used for loss, "Dirty Data," outliers, and drops in performance.                      |
| **Warning**          | **Uber Orange** | `#FF7D49` | `255, 125, 73` | **Caution.** Used for potentially anomalous data or warnings.                                            |

### **2.2. The Neutral Architecture (Grays)**
Derived from Uber's `Mono` tokens. Used for non-data UI elements.

| Token        | Hex       | Usage                                                                         |
| :----------- | :-------- | :---------------------------------------------------------------------------- |
| **Canvas**   | `#FFFFFF` | **Background Only.** No exceptions.                                           |
| **Gray 100** | `#F6F6F6` | **Subtle Fills.** Alternating table rows, map water background, hover states. |
| **Gray 300** | `#E2E2E2` | **Structure.** Gridlines, axis ticks, inactive borders.                       |
| **Gray 500** | `#AFAFAF` | **De-emphasized Text.** Placeholder text, extremely low-priority annotations. |
| **Gray 600** | `#545454` | **Secondary Text.** Subtitles, axis titles, footnotes, context labels.        |

### **2.3. Categorical Palettes (Multi-Variable)**
When charting more than 2 categories, strict ordering is required to maintain brand voice.

**Sequence Order (Strict):**
1.  **Uber Green** (`#47B275`) - *Focus*
2.  **Uber Black** (`#000000`) - *Contrast*
3.  **Uber Purple** (`#7356BF`) - *Differentiation* (High contrast against Green)
4.  **Uber Orange** (`#FF7D49`) - *Warmth*
5.  **Uber Brown** (`#99644C`) - *Earth* (Use sparingly)
6.  **Uber Yellow** (`#FFC043`) - *Fill Only* (Never use for text or thin lines; low contrast on white).

### **2.4. Continuous & Diverging Scales**
For Choropleths (Maps) and Heatmaps.

**A. The "Density" Scale (Sequential: 0 to Max)**
*   *Concept:* From "Nothing" (White) to "Active" (Green) to "Intense" (Deep Green/Black).
*   *Implementation:*
    *   0%: `#FFFFFF` (White)
    *   20%: `#D3EFDE` (Lightest Green tint)
    *   60%: `#47B275` (Uber Green)
    *   100%: `#0E3F25` (Deep Forest Green - *Custom blend of Green+Black for legibility*)
*   *Note:* We avoid ending in pure Black for maps to allow boundaries to remain visible.

**B. The "Performance" Scale (Diverging: Negative to Positive)**
*   *Concept:* Traffic Light Logic.
*   *Implementation:*
    *   Low (Negative): `#F25138` (Uber Red)
    *   Mid (Neutral): `#F6F6F6` (Gray 100 - effectively White)
    *   High (Positive): `#47B275` (Uber Green)

---

## **III. Typography & Hierarchy**

We simulate the **Uber Move** type system. All text is Sans-Serif.

**Font Stack:** `"Uber Move Text", "Uber Move", "Inter", "Roboto", "Arial", "sans-serif"`

### **3.1. The Typesetting Table**

| Component              | Style              | Size     | Weight           | Color       | Spacing/Leading          |
| :--------------------- | :----------------- | :------- | :--------------- | :---------- | :----------------------- |
| **Chart Title**        | `Heading Small`    | **24px** | **Medium (500)** | Black       | Bottom Margin: `8px`     |
| **Subtitle**           | `Paragraph Medium` | **16px** | Light (300)      | Gray 600    | Bottom Margin: `24px`    |
| **Axis Labels**        | `Label Small`      | **14px** | Regular (400)    | Black       | -                        |
| **Axis Titles**        | `Label XSmall`     | **12px** | Regular (400)    | Gray 600    | Uppercase optional       |
| **Annotation (Focus)** | `Label Small`      | **14px** | **Medium (500)** | Green/Black | Background pill optional |
| **Annotation (Note)**  | `Label XSmall`     | **12px** | Light (300)      | Gray 600    | -                        |
| **Footer/Source**      | `Label XSmall`     | **10px** | Light (300)      | Gray 600    | Top Margin: `24px`       |

### **3.2. Special Text Formatting**
*   **Dual-Tone Titles:** Use HTML styling to highlight key insights within the title.
    *   *Correct:* `<b>Total Trips in 2024</b> <span style="color:#47B275; font-size: 16px">↑ 12% YoY</span>`
*   **Metric formatting:**
    *   Currency: `$12.5M` (No cents unless precision required).
    *   Percentages: `12.4%` (1 decimal place).
    *   Large Numbers: `1.2M`, `450k` (Use engineering notation for clarity).

---

## **IV. Layout & Spatial Specs**

We adhere to the **Base Web Layout Grid** (Medium/Large Breakpoints).

### **4.1. The Canvas Geometry**
*   **Outer Margins:**
    *   Top: `64px` (Clear space for branding).
    *   Bottom: `64px` (Clear space for attribution).
    *   Left/Right: `36px` (Standard Gutter).
*   **Internal Padding:** The chart plotting area sits inside these margins.

### **4.2. The Grid System**
*   **X-Axis:**
    *   Visible Line: **Yes**. Color: `#000000` (Width 2px). The "Ground" line.
    *   Gridlines: **None**. Vertical gridlines create clutter.
*   **Y-Axis:**
    *   Visible Line: **No**.
    *   Gridlines: **Yes**. Color: `#E2E2E2` (Width 1px).
    *   Tick Marks: Outside, 4px length, `#E2E2E2`.

---

## **V. Component Specifics**

### **5.1. Bar Charts (The Workhorse)**
*   **Corner Radius:** **`4px`** on top-left and top-right corners only.
*   **Width:** Bars should act as "Columns." Gap between bars should be ~30% of bar width.
*   **Stroke:** None.
*   **Pattern:**
    *   *Single Series:* All bars **Uber Black**. Highlighted bar **Uber Green**.
    *   *Comparison:* Series A = Black, Series B = Green.

### **5.2. Line Charts (The Trend)**
*   **Line Width:** **`3px`**. Thick and confident.
*   **Smoothing:** **`0`**. No splines. Uber is geometric and precise.
*   **Markers:** Hidden by default. Show only on hover or for single data points.
*   **Area Charts:** If filling area under line, use `opacity=0.1` (10%) of the line color.

### **5.3. Scatter Plots (The Analysis)**
*   **Marker Size:** `10px` (Default).
*   **Opacity:** `0.7` (To handle overlap).
*   **Stroke:** `1px` White stroke around markers (creates separation).

### **5.4. Maps (The Brand Core)**
*   **Projection:** Mercator (Standard for city data).
*   **Land Color:** `#F6F6F6` (Gray 100).
*   **Border Color:** White (0.5px).
*   **Choropleth:** Use the "Density" scale defined in Section 2.4.

---

## **VI. Branding & Professionalism**

### **6.1. The Logo**
*   **Asset:** [Uber Logo (Black)](https://upload.wikimedia.org/wikipedia/commons/5/58/Uber_logo_2018.svg).
*   **Placement:** Bottom Right corner of the canvas (Layout coordinates `x: 1.01, y: -0.24`).
*   **Size:** Height ~12px.

### **6.2. The Attribution Footer**
*   **Content:** Must be strictly formatted.
*   **Template:**
    > *Source: NYC Taxi & Limousine Commission (TLC) High Volume For-Hire Vehicle Records (2024). Data filtered for Uber Technologies Inc.*
    > Can also add something like "*Analysis: [Team Name] | Course: Data Preparation & Visualization*", either at the end of the previous one or right-aligned on the same line. Only add this if explicitly required.
*   **Style:** Tiny, Gray 600, Bottom-Left aligned.

### **6.3. The "Data Prep" Storytelling Mode**
Since the project is about "Raw vs. Clean":

*   **State A: Raw/Dirty Data**
    *   **Color:** `Gray 500` or `Uber Red` (if showing errors).
    *   **Style:** High opacity, smaller markers (noise).
    *   **Label:** "Raw Input"
*   **State B: Processed/Clean Data**
    *   **Color:** `Uber Green`.
    *   **Style:** Sharp, defined, `Uber Black` trendlines.
    *   **Label:** "Refined Output"
