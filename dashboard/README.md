## 📊 Dashboard Overview: Firemai

The Firemai dashboard visualizes forest fire incidents in Thailand and their nearby critical areas to support alerting and response.

### 🔥 Section 1: Fire Summary (Top bar)
- **Country**: Current scope is fixed to Thailand.
- **Record Count**: Total fire incidents recorded.
- **Confidence**: Average satellite confidence level of detected fires.
- **FRP**: Fire Radiative Power – average fire intensity.

### 🗺️ Section 2: Regional Overview
- **Map**: Highlighted area where fire was detected.
- **Time series**: Fire record trends over time.
- **Pie chart**: Distribution of fire by land use (e.g. forest, agricultural).

### 📈 Section 3: Filters
- **pv_en, ap_en, tb_en**: Dropdown filters for Province, Amphoe, Tambon.

### 📉 Section 4: Province-level Detail
- **Scatter plot**: Fire distribution across provinces.
- **Bar chart**: Ranking of provinces with highest fire counts.
- **Grouped bar chart**: Fire case counts in each province.

### 🚨 Section 5: Nearby Alert Areas (Enriched from POI)
- **Stacked bar**: Shows POI types near each fire incident.
- Useful for assessing potential impact on infrastructure, education, or urban centers.
- use data from view: hotspot_with_nearest_tag

This dashboard helps visualize hotspots over time and detect proximity to sensitive locations.