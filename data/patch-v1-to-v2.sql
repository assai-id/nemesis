ALTER TABLE region_metrics ADD COLUMN central_priority_packages INTEGER NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN provincial_priority_packages INTEGER NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN local_priority_packages INTEGER NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN other_priority_packages INTEGER NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN central_potential_waste REAL NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN provincial_potential_waste REAL NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN local_potential_waste REAL NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN other_potential_waste REAL NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN central_budget INTEGER NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN provincial_budget INTEGER NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN local_budget INTEGER NOT NULL DEFAULT 0;
ALTER TABLE region_metrics ADD COLUMN other_budget INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS owner_metrics (
  owner_type TEXT NOT NULL,
  owner_name TEXT NOT NULL,
  total_packages INTEGER NOT NULL DEFAULT 0,
  total_priority_packages INTEGER NOT NULL DEFAULT 0,
  total_flagged_packages INTEGER NOT NULL DEFAULT 0,
  total_potential_waste REAL NOT NULL DEFAULT 0,
  total_budget INTEGER NOT NULL DEFAULT 0,
  med_severity_packages INTEGER NOT NULL DEFAULT 0,
  high_severity_packages INTEGER NOT NULL DEFAULT 0,
  absurd_severity_packages INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (owner_type, owner_name)
);
BEGIN TRANSACTION;

-- Populate owner_metrics
INSERT OR REPLACE INTO owner_metrics (
  owner_type, owner_name, total_packages, total_priority_packages, 
  total_flagged_packages, total_potential_waste, total_budget, 
  med_severity_packages, high_severity_packages, absurd_severity_packages
)
SELECT 
  owner_type, owner_name, COUNT(id), COALESCE(SUM(is_priority), 0), COALESCE(SUM(is_flagged), 0),
  COALESCE(SUM(potential_waste), 0), COALESCE(SUM(COALESCE(budget, 0)), 0),
  COALESCE(SUM(CASE WHEN severity = 'med' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN severity = 'absurd' THEN 1 ELSE 0 END), 0)
FROM packages
GROUP BY owner_type, owner_name;

-- Materialized view abstraction to speed up updating 12 columns by Region dynamically
CREATE TEMP TABLE temp_region_agg AS
SELECT 
  pr.region_key,
  p.owner_type,
  COALESCE(SUM(p.is_priority), 0) as sm_priority,
  COALESCE(SUM(p.potential_waste), 0) as sm_waste,
  COALESCE(SUM(p.budget), 0) as sm_budget
FROM packages p
INNER JOIN package_regions pr ON pr.package_id = p.id
GROUP BY pr.region_key, p.owner_type;

CREATE INDEX idx_temp_region_agg ON temp_region_agg(region_key, owner_type);

-- Central
UPDATE region_metrics SET 
  central_priority_packages = COALESCE((SELECT sm_priority FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'central'), 0),
  central_potential_waste = COALESCE((SELECT sm_waste FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'central'), 0),
  central_budget = COALESCE((SELECT sm_budget FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'central'), 0);

-- Provinsi
UPDATE region_metrics SET 
  provincial_priority_packages = COALESCE((SELECT sm_priority FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'provinsi'), 0),
  provincial_potential_waste = COALESCE((SELECT sm_waste FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'provinsi'), 0),
  provincial_budget = COALESCE((SELECT sm_budget FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'provinsi'), 0);

-- Local
UPDATE region_metrics SET 
  local_priority_packages = COALESCE((SELECT sm_priority FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'kabkota'), 0),
  local_potential_waste = COALESCE((SELECT sm_waste FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'kabkota'), 0),
  local_budget = COALESCE((SELECT sm_budget FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'kabkota'), 0);

-- Other
UPDATE region_metrics SET 
  other_priority_packages = COALESCE((SELECT sm_priority FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'other'), 0),
  other_potential_waste = COALESCE((SELECT sm_waste FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'other'), 0),
  other_budget = COALESCE((SELECT sm_budget FROM temp_region_agg WHERE region_key = region_metrics.region_key AND owner_type = 'other'), 0);

DROP TABLE temp_region_agg;
COMMIT;
