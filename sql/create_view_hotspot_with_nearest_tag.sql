CREATE OR REPLACE VIEW `Firemai.hotspot_with_nearest_tag` AS
SELECT 
  f.th_date,
  f.th_time,
  f.village,
  f.pv_en,
  f.ap_en,
  f.latitude,
  f.longitude,
  f.brightness,
  f.hotspot_id,
  g.poi_lat,
  g.poi_lon,
  g.tags[SAFE_OFFSET(0)] AS first_tag
FROM `Firemai.fire_hotspot_main` f
JOIN `Firemai.fire_poi_grouped` g
ON f.pv_en = g.pv_en
AND ST_DISTANCE(ST_GEOGPOINT(f.longitude, f.latitude), ST_GEOGPOINT(g.poi_lon, g.poi_lat)) < 5000;