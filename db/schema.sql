-- ==============================================================
-- WhatsApp Business Platform Pricing – Database Schema
-- Compatible with: MariaDB 10.6+
-- ==============================================================

-- Market catalogue: one row per billing market published by Meta.
-- The `country` table holds the reverse FK (country.waba_market_id → waba_market.id)
-- so that multiple countries can resolve to the same regional market.
CREATE TABLE IF NOT EXISTS waba_market (
    id   SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL COMMENT 'Market name as used in Meta pricing CSVs',
    UNIQUE KEY uq_market_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Meta WhatsApp billing market catalogue';

-- Message-type catalogue (fixed set defined by Meta)
CREATE TABLE IF NOT EXISTS waba_message_type (
    id   TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(30)  NOT NULL,
    name VARCHAR(100) NOT NULL,
    UNIQUE KEY uq_msg_type_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='WhatsApp message category catalogue';

INSERT IGNORE INTO waba_message_type (code, name) VALUES
    ('MARKETING',      'Marketing'),
    ('UTILITY',        'Utility'),
    ('AUTHENTICATION', 'Authentication'),
    ('AUTH_INTL',      'Authentication-International'),
    ('SERVICE',        'Service');

-- Pricing load: one row per CSV file ingested
--   valid_to NULL  => currently active for that currency + file_type combination
--   When a new file is loaded, the script sets valid_to on the previous active row
CREATE TABLE IF NOT EXISTS waba_pricing_load (
    id          INT UNSIGNED        AUTO_INCREMENT PRIMARY KEY,
    currency    CHAR(3)             NOT NULL                    COMMENT 'ISO 4217 code (e.g. USD)',
    file_type   ENUM('BASE','TIER') NOT NULL                    COMMENT 'BASE = Pricing.csv | TIER = Tier Pricing.csv',
    file_name   VARCHAR(255)        NULL,
    valid_from  DATE                NOT NULL,
    valid_to    DATE                NULL                        COMMENT 'NULL = currently active',
    uploaded_at DATETIME            NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uploaded_by VARCHAR(100)        NULL,
    notes       TEXT                NULL,
    UNIQUE KEY uq_load (currency, file_type, valid_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Audit log of every CSV file ingested';

-- Base rates: one row per market × message-type (from Pricing.csv)
--   rate NULL => the combination is not applicable (n/a) for that market
CREATE TABLE IF NOT EXISTS waba_base_rate (
    id              BIGINT UNSIGNED   AUTO_INCREMENT PRIMARY KEY,
    load_id         INT UNSIGNED      NOT NULL,
    market_id       SMALLINT UNSIGNED NOT NULL,
    message_type_id TINYINT UNSIGNED  NOT NULL,
    rate            DECIMAL(10,6)     NULL                     COMMENT 'NULL = n/a for this market/type',
    KEY idx_base_load   (load_id),
    KEY idx_base_market (market_id, message_type_id),
    CONSTRAINT fk_base_load    FOREIGN KEY (load_id)         REFERENCES waba_pricing_load (id),
    CONSTRAINT fk_base_market  FOREIGN KEY (market_id)       REFERENCES waba_market (id),
    CONSTRAINT fk_base_msgtype FOREIGN KEY (message_type_id) REFERENCES waba_message_type (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Flat (non-tiered) rates per market and message type';

-- Tier rates: one row per market × message-type × volume band (from Tier Pricing.csv)
--   volume_to NULL => unlimited (shown as "--" in the CSV)
CREATE TABLE IF NOT EXISTS waba_tier_rate (
    id              BIGINT UNSIGNED     AUTO_INCREMENT PRIMARY KEY,
    load_id         INT UNSIGNED        NOT NULL,
    market_id       SMALLINT UNSIGNED   NOT NULL,
    message_type_id TINYINT UNSIGNED    NOT NULL,
    volume_from     INT UNSIGNED        NOT NULL,
    volume_to       INT UNSIGNED        NULL                   COMMENT 'NULL = unlimited',
    rate_type       ENUM('LIST','TIER') NOT NULL,
    rate            DECIMAL(10,6)       NOT NULL,
    discount_pct    TINYINT             NOT NULL DEFAULT 0     COMMENT 'Discount applied vs. list rate: 0, -5, -10, -15, -20, -25',
    KEY idx_tier_load   (load_id),
    KEY idx_tier_market (market_id, message_type_id),
    CONSTRAINT fk_tier_load    FOREIGN KEY (load_id)         REFERENCES waba_pricing_load (id),
    CONSTRAINT fk_tier_market  FOREIGN KEY (market_id)       REFERENCES waba_market (id),
    CONSTRAINT fk_tier_msgtype FOREIGN KEY (message_type_id) REFERENCES waba_message_type (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Volume-tiered rates per market and message type';
