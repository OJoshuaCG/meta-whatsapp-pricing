-- ==============================================================
-- Migration: restructure waba_market and link country table
-- Run once on environments where tables already exist.
-- Safe to re-run (uses IF EXISTS / IF NOT EXISTS / INSERT IGNORE).
-- MariaDB 10.6+ required.
-- ==============================================================

-- ── Step 1: Fix waba_market ────────────────────────────────────
-- Remove country_id if it was added by a previous migration attempt.
ALTER TABLE waba_market
    DROP FOREIGN KEY IF EXISTS fk_market_country,
    DROP INDEX     IF EXISTS idx_market_country,
    DROP COLUMN    IF EXISTS country_id;

-- Add region column if not present.
ALTER TABLE waba_market
    ADD COLUMN IF NOT EXISTS region VARCHAR(100) NULL
        COMMENT 'Region label from Meta_Countries.csv; NULL = individual country market'
        AFTER name;

-- ── Step 2: Seed waba_market ───────────────────────────────────
-- Individual country markets (region = NULL).
INSERT IGNORE INTO waba_market (name, region) VALUES
    ('Argentina',             NULL),
    ('Brazil',                NULL),
    ('Chile',                 NULL),
    ('Colombia',              NULL),
    ('Egypt',                 NULL),
    ('France',                NULL),
    ('Germany',               NULL),
    ('India',                 NULL),
    ('Indonesia',             NULL),
    ('Israel',                NULL),
    ('Italy',                 NULL),
    ('Malaysia',              NULL),
    ('Mexico',                NULL),
    ('Netherlands',           NULL),
    ('Nigeria',               NULL),
    ('Pakistan',              NULL),
    ('Peru',                  NULL),
    ('Russia',                NULL),
    ('Saudi Arabia',          NULL),
    ('South Africa',          NULL),
    ('Spain',                 NULL),
    ('Turkey',                NULL),
    ('United Arab Emirates',  NULL),
    ('United Kingdom',        NULL),
    ('United States',         NULL),
    -- Regional group markets.
    ('North America',                    'North America'),
    ('Rest of Africa',                   'Rest of Africa'),
    ('Rest of Asia Pacific',             'Rest of Asia Pacific'),
    ('Rest of Central & Eastern Europe', 'Rest of Central & Eastern Europe'),
    ('Rest of Latin America',            'Rest of Latin America'),
    ('Rest of Middle East',              'Rest of Middle East'),
    ('Rest of Western Europe',           'Rest of Western Europe'),
    ('Other',                            'Other');

-- Backfill region = NULL for existing individual-country rows
-- that were inserted before this migration (e.g. by --file loads).
-- Regional rows already have region set via INSERT IGNORE above.
-- No action needed: NULL is already the default.

-- ── Step 3: Add waba_market_id to the country table ───────────
ALTER TABLE country
    ADD COLUMN IF NOT EXISTS waba_market_id SMALLINT UNSIGNED NULL
        COMMENT 'FK to waba_market.id; resolves billing market for this country'
        AFTER id,
    ADD KEY IF NOT EXISTS idx_country_waba_market (waba_market_id),
    ADD CONSTRAINT IF NOT EXISTS fk_country_waba_market
        FOREIGN KEY (waba_market_id) REFERENCES waba_market (id)
        ON DELETE SET NULL ON UPDATE CASCADE;

-- ── Step 4: Map country.waba_market_id ────────────────────────

-- 4a. Individual country markets: country.name matches waba_market.name directly.
UPDATE country c
JOIN waba_market wm ON wm.name = c.name AND wm.region IS NULL
SET c.waba_market_id = wm.id
WHERE c.waba_market_id IS NULL;

-- 4b. Countries whose name in `country` differs from Meta's market name.
UPDATE country SET waba_market_id = (SELECT id FROM waba_market WHERE name = 'United Arab Emirates')
WHERE name = 'United Arab Emirates' AND waba_market_id IS NULL;

-- 4c. Regional market: Rest of Africa
UPDATE country c
JOIN waba_market wm ON wm.name = 'Rest of Africa'
SET c.waba_market_id = wm.id
WHERE c.name IN (
    'Algeria','Angola','Benin','Botswana','Burkina Faso','Burundi',
    'Cameroon','Chad','Eritrea','Ethiopia','Gabon','Gambia','Ghana',
    'Guinea-Bissau','Ivory Coast','Kenya','Lesotho','Liberia','Libya',
    'Madagascar','Malawi','Mali','Mauritania','Morocco','Mozambique',
    'Namibia','Niger','Republic of the Congo (Brazzaville)','Rwanda',
    'Senegal','Sierra Leone','Somalia','South Sudan','Sudan','Swaziland',
    'Tanzania','Togo','Tunisia','Uganda','Zambia','Zimbabwe'
) AND c.waba_market_id IS NULL;

-- 4d. Regional market: Rest of Asia Pacific
UPDATE country c
JOIN waba_market wm ON wm.name = 'Rest of Asia Pacific'
SET c.waba_market_id = wm.id
WHERE c.name IN (
    'Afghanistan','Australia','Bangladesh','Cambodia','China',
    'Hong Kong','Japan','Laos','Mongolia','Nepal','New Zealand',
    'Papua New Guinea','Philippines','Singapore','Sri Lanka',
    'Taiwan','Tajikistan','Thailand','Turkmenistan','Uzbekistan','Vietnam'
) AND c.waba_market_id IS NULL;

-- 4e. Regional market: Rest of Central & Eastern Europe
UPDATE country c
JOIN waba_market wm ON wm.name = 'Rest of Central & Eastern Europe'
SET c.waba_market_id = wm.id
WHERE c.name IN (
    'Albania','Armenia','Azerbaijan','Belarus','Bulgaria','Croatia',
    'Czech Republic','Georgia','Greece','Hungary','Latvia','Lithuania',
    'Moldova','North Macedonia','Poland','Romania','Serbia',
    'Slovakia','Slovenia','Ukraine'
) AND c.waba_market_id IS NULL;

-- 4f. Regional market: Rest of Latin America
UPDATE country c
JOIN waba_market wm ON wm.name = 'Rest of Latin America'
SET c.waba_market_id = wm.id
WHERE c.name IN (
    'Bolivia','Costa Rica','Dominican Republic','Ecuador','El Salvador',
    'Guatemala','Haiti','Honduras','Jamaica','Nicaragua','Panama',
    'Paraguay','Puerto Rico','Uruguay','Venezuela'
) AND c.waba_market_id IS NULL;

-- 4g. Regional market: Rest of Middle East
UPDATE country c
JOIN waba_market wm ON wm.name = 'Rest of Middle East'
SET c.waba_market_id = wm.id
WHERE c.name IN (
    'Bahrain','Iraq','Jordan','Kuwait','Lebanon','Oman','Qatar','Yemen'
) AND c.waba_market_id IS NULL;

-- 4h. Regional market: Rest of Western Europe
UPDATE country c
JOIN waba_market wm ON wm.name = 'Rest of Western Europe'
SET c.waba_market_id = wm.id
WHERE c.name IN (
    'Austria','Belgium','Denmark','Finland','Ireland',
    'Norway','Portugal','Sweden','Switzerland'
) AND c.waba_market_id IS NULL;

-- 4i. Regional market: North America (Canada only; US is an individual market)
UPDATE country c
JOIN waba_market wm ON wm.name = 'North America'
SET c.waba_market_id = wm.id
WHERE c.name = 'Canada'
AND c.waba_market_id IS NULL;

-- ── Verify ─────────────────────────────────────────────────────
-- Countries with no market assigned (investigate manually):
-- SELECT id, nombre, name FROM country WHERE waba_market_id IS NULL;
--
-- Market coverage summary:
-- SELECT wm.name, wm.region, COUNT(c.id) AS countries
-- FROM waba_market wm
-- LEFT JOIN country c ON c.waba_market_id = wm.id
-- GROUP BY wm.id ORDER BY wm.name;
