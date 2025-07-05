IF OBJECT_ID('stg_tmp_barchart_HON25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_HON25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_HON25_uco_price_temp (
    timing            DATETIME       NOT NULL,
    prev_open         FLOAT          NULL,
    high              FLOAT          NULL,
    low               FLOAT          NULL,
    last              FLOAT          NULL,
    price_change      FLOAT          NULL,
    percent_change    FLOAT          NULL,
    volume            BIGINT         NULL,
    oi                BIGINT         NULL,
    contract          VARCHAR(10)    NULL,
    raw               NVARCHAR(MAX)  NULL
);

IF OBJECT_ID('stg_tmp_barchart_LFN25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_LFN25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_LFN25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_HOQ25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_HOQ25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_HOQ25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_LFQ25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_LFQ25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_LFQ25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_HOU25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_HOU25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_HOU25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_LFU25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_LFU25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_LFU25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_HOV25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_HOV25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_HOV25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_LFV25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_LFV25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_LFV25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_HOX25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_HOX25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_HOX25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_LFX25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_LFX25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_LFX25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_HOZ25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_HOZ25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_HOZ25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);

IF OBJECT_ID('stg_tmp_barchart_LFZ25_uco_price_temp', 'U') IS NOT NULL
    DROP TABLE stg_tmp_barchart_LFZ25_uco_price_temp;
CREATE TABLE stg_tmp_barchart_LFZ25_uco_price_temp (
    timing DATETIME NOT NULL,
    prev_open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    last FLOAT NULL,
    price_change FLOAT NULL,
    percent_change FLOAT NULL,
    volume BIGINT NULL,
    oi BIGINT NULL,
    contract VARCHAR(10) NULL,
    raw NVARCHAR(MAX) NULL
);
