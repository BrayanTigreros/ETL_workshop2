USE workshop2;
SET NAMES utf8mb4;

-- Asegurar base de datos en UTF8
ALTER DATABASE workshop2 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

-- =========================================
-- DIMENSION: TRACK
-- =========================================
CREATE TABLE IF NOT EXISTS dim_track (
    track_key INT AUTO_INCREMENT PRIMARY KEY,
    track_id TEXT,
    track_name TEXT,
    album_name TEXT,
    gender TEXT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- =========================================
-- DIMENSION: ARTIST
-- =========================================
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key INT AUTO_INCREMENT PRIMARY KEY,
    artists TEXT,
    workers TEXT,
    img TEXT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- =========================================
-- DIMENSION: GRAMMY
-- =========================================
CREATE TABLE IF NOT EXISTS dim_grammy (
    grammy_key INT AUTO_INCREMENT PRIMARY KEY,
    title TEXT,
    category TEXT,
    winner BOOLEAN
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- =========================================
-- DIMENSION: DATE
-- =========================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT AUTO_INCREMENT PRIMARY KEY,
    year INT,
    published_at DATETIME,
    updated_at DATETIME
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- =========================================
-- FACT TABLE
-- =========================================
CREATE TABLE IF NOT EXISTS fact_table (
    id INT AUTO_INCREMENT PRIMARY KEY,

    track_key INT,
    artist_key INT,
    grammy_key INT,
    date_key INT,

    popularity INT,
    duration_ms INT,

    CONSTRAINT fk_track
        FOREIGN KEY (track_key) REFERENCES dim_track(track_key),

    CONSTRAINT fk_artist
        FOREIGN KEY (artist_key) REFERENCES dim_artist(artist_key),

    CONSTRAINT fk_grammy
        FOREIGN KEY (grammy_key) REFERENCES dim_grammy(grammy_key),

    CONSTRAINT fk_date
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;