-- Tạo database
create database TLC;
create database TLC_DTM	;
create database TLC_ADMIN_DA;
GO

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Tạo schema
USE TLC;
CREATE SCHEMA stg_tmp;
GO

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

USE TLC;
-- Tạo bảng trong database TLC
-- Tạo bảng stg_meta_data
IF OBJECT_ID('dbo.stg_meta_data', 'U') IS NOT NULL
    DROP TABLE dbo.stg_meta_data;
GO

CREATE TABLE dbo.stg_meta_data
(
    source_id         INT IDENTITY(1,1) PRIMARY KEY,
    domain            NVARCHAR(50)  NOT NULL,
    sub_domain        NVARCHAR(50)  NOT NULL,
    source_name       NVARCHAR(255) NOT NULL,
    source_path       NVARCHAR(500) NOT NULL,
    source_type       NVARCHAR(20)  NOT NULL,
    temp_name         NVARCHAR(128) NOT NULL,
    target_name       NVARCHAR(128) NOT NULL,
    created_date      DATETIME      NOT NULL DEFAULT GETDATE(),
    updated_date      DATETIME      NULL,
    created_by        NVARCHAR(128) NOT NULL
);
GO

CREATE UNIQUE INDEX IX_stg_meta_data_target
    ON dbo.stg_meta_data (target_name);

CREATE INDEX IX_stg_meta_data_domain_sub
    ON dbo.stg_meta_data (domain, sub_domain);
GO

--------------------------------------------------------------------

-- Tạo bảng stg_checking_logs
IF OBJECT_ID('dbo.stg_checking_logs', 'U') IS NOT NULL
    DROP TABLE dbo.stg_checking_logs;
GO

CREATE TABLE dbo.stg_checking_logs (
    log_id        INT IDENTITY(1,1) PRIMARY KEY,
    script        NVARCHAR(255) NOT NULL,
    source_name   NVARCHAR(255) NOT NULL,
    target_name   NVARCHAR(255) NOT NULL,
    source_row    INT           NOT NULL,
    target_row    INT           NOT NULL,
    duration      FLOAT         NOT NULL,
    created_by    DATETIME      NOT NULL DEFAULT GETDATE()
);
GO

--------------------------------------------------------------------

-- Tạo bảng stg_barchart_HOQ25_uco_price
IF OBJECT_ID('dbo.stg_barchart_HON25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_HON25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_HON25_uco_price
(
    timing            DATETIME        NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT           NULL,
    high              FLOAT           NULL,
    low               FLOAT           NULL,
    last              FLOAT           NULL,
    price_change      FLOAT           NULL,
    percent_change    FLOAT           NULL,
    volume            BIGINT          NULL,
    oi                BIGINT          NULL,
    raw               NVARCHAR(MAX)   NULL,

    source_table      NVARCHAR(100)   NOT NULL,
    created_date      DATETIME        NOT NULL,
    snapshot_date     DATE            NOT NULL,
    snapshot_date_ol  DATE            NOT NULL
);
GO

--------------------------------------------------------------------

IF OBJECT_ID('dbo.stg_barchart_HOQ25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_HOQ25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_HOQ25_uco_price
(
    timing            DATETIME        NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT           NULL,
    high              FLOAT           NULL,
    low               FLOAT           NULL,
    last              FLOAT           NULL,
    price_change      FLOAT           NULL,
    percent_change    FLOAT           NULL,
    volume            BIGINT          NULL,
    oi                BIGINT          NULL,
    raw               NVARCHAR(MAX)   NULL,

    source_table      NVARCHAR(100)   NOT NULL,
    created_date      DATETIME        NOT NULL,
    snapshot_date     DATE            NOT NULL,
    snapshot_date_ol  DATE            NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_barchart_HOU25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_HOU25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_HOU25_uco_price
(
    timing            DATETIME        NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT           NULL,
    high              FLOAT           NULL,
    low               FLOAT           NULL,
    last              FLOAT           NULL,
    price_change      FLOAT           NULL,
    percent_change    FLOAT           NULL,
    volume            BIGINT          NULL,
    oi                BIGINT          NULL,
    raw               NVARCHAR(MAX)   NULL,

    source_table      NVARCHAR(100)   NOT NULL,
    created_date      DATETIME        NOT NULL,
    snapshot_date     DATE            NOT NULL,
    snapshot_date_ol  DATE            NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_barchart_HOV25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_HOV25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_HOV25_uco_price
(
    timing            DATETIME        NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT           NULL,
    high              FLOAT           NULL,
    low               FLOAT           NULL,
    last              FLOAT           NULL,
    price_change      FLOAT           NULL,
    percent_change    FLOAT           NULL,
    volume            BIGINT          NULL,
    oi                BIGINT          NULL,
    raw               NVARCHAR(MAX)   NULL,

    source_table      NVARCHAR(100)   NOT NULL,
    created_date      DATETIME        NOT NULL,
    snapshot_date     DATE            NOT NULL,
    snapshot_date_ol  DATE            NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_barchart_HOX25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_HOX25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_HOX25_uco_price
(
    timing            DATETIME        NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT           NULL,
    high              FLOAT           NULL,
    low               FLOAT           NULL,
    last              FLOAT           NULL,
    price_change      FLOAT           NULL,
    percent_change    FLOAT           NULL,
    volume            BIGINT          NULL,
    oi                BIGINT          NULL,
    raw               NVARCHAR(MAX)   NULL,

    source_table      NVARCHAR(100)   NOT NULL,
    created_date      DATETIME        NOT NULL,
    snapshot_date     DATE            NOT NULL,
    snapshot_date_ol  DATE            NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_barchart_HOZ25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_HOZ25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_HOZ25_uco_price
(
    timing            DATETIME        NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT           NULL,
    high              FLOAT           NULL,
    low               FLOAT           NULL,
    last              FLOAT           NULL,
    price_change      FLOAT           NULL,
    percent_change    FLOAT           NULL,
    volume            BIGINT          NULL,
    oi                BIGINT          NULL,
    raw               NVARCHAR(MAX)   NULL,

    source_table      NVARCHAR(100)   NOT NULL,
    created_date      DATETIME        NOT NULL,
    snapshot_date     DATE            NOT NULL,
    snapshot_date_ol  DATE            NOT NULL
);
GO

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Tạo bảng stg_barchart_LFN25_uco_price
IF OBJECT_ID('dbo.stg_barchart_LFN25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_LFN25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_LFN25_uco_price
(
    timing            DATETIME       NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT          NULL,
    high              FLOAT          NULL,
    low               FLOAT          NULL,
    last              FLOAT          NULL,
    price_change      FLOAT          NULL,
    percent_change    FLOAT          NULL,
    volume            BIGINT         NULL,
    oi                BIGINT         NULL,
    raw               NVARCHAR(MAX)  NULL,
    source_table      NVARCHAR(100)  NOT NULL,
    created_date      DATETIME       NOT NULL,
    snapshot_date     DATE           NOT NULL,
    snapshot_date_ol  DATE           NOT NULL
);
GO

-- Tạo bảng stg_barchart_LFQ25_uco_price
IF OBJECT_ID('dbo.stg_barchart_LFQ25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_LFQ25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_LFQ25_uco_price
(
    timing            DATETIME       NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT          NULL,
    high              FLOAT          NULL,
    low               FLOAT          NULL,
    last              FLOAT          NULL,
    price_change      FLOAT          NULL,
    percent_change    FLOAT          NULL,
    volume            BIGINT         NULL,
    oi                BIGINT         NULL,
    raw               NVARCHAR(MAX)  NULL,
    source_table      NVARCHAR(100)  NOT NULL,
    created_date      DATETIME       NOT NULL,
    snapshot_date     DATE           NOT NULL,
    snapshot_date_ol  DATE           NOT NULL
);
GO

-- Tạo bảng stg_barchart_LFU25_uco_price
IF OBJECT_ID('dbo.stg_barchart_LFU25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_LFU25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_LFU25_uco_price
(
    timing            DATETIME       NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT          NULL,
    high              FLOAT          NULL,
    low               FLOAT          NULL,
    last              FLOAT          NULL,
    price_change      FLOAT          NULL,
    percent_change    FLOAT          NULL,
    volume            BIGINT         NULL,
    oi                BIGINT         NULL,
    raw               NVARCHAR(MAX)  NULL,
    source_table      NVARCHAR(100)  NOT NULL,
    created_date      DATETIME       NOT NULL,
    snapshot_date     DATE           NOT NULL,
    snapshot_date_ol  DATE           NOT NULL
);
GO

-- Tạo bảng stg_barchart_LFV25_uco_price
IF OBJECT_ID('dbo.stg_barchart_LFV25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_LFV25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_LFV25_uco_price
(
    timing            DATETIME       NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT          NULL,
    high              FLOAT          NULL,
    low               FLOAT          NULL,
    last              FLOAT          NULL,
    price_change      FLOAT          NULL,
    percent_change    FLOAT          NULL,
    volume            BIGINT         NULL,
    oi                BIGINT         NULL,
    raw               NVARCHAR(MAX)  NULL,
    source_table      NVARCHAR(100)  NOT NULL,
    created_date      DATETIME       NOT NULL,
    snapshot_date     DATE           NOT NULL,
    snapshot_date_ol  DATE           NOT NULL
);
GO

-- Tạo bảng stg_barchart_LFX25_uco_price
IF OBJECT_ID('dbo.stg_barchart_LFX25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_LFX25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_LFX25_uco_price
(
    timing            DATETIME       NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT          NULL,
    high              FLOAT          NULL,
    low               FLOAT          NULL,
    last              FLOAT          NULL,
    price_change      FLOAT          NULL,
    percent_change    FLOAT          NULL,
    volume            BIGINT         NULL,
    oi                BIGINT         NULL,
    raw               NVARCHAR(MAX)  NULL,
    source_table      NVARCHAR(100)  NOT NULL,
    created_date      DATETIME       NOT NULL,
    snapshot_date     DATE           NOT NULL,
    snapshot_date_ol  DATE           NOT NULL
);
GO

-- Tạo bảng stg_barchart_LFZ25_uco_price
IF OBJECT_ID('dbo.stg_barchart_LFZ25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.stg_barchart_LFZ25_uco_price;
GO

CREATE TABLE dbo.stg_barchart_LFZ25_uco_price
(
    timing            DATETIME       NOT NULL,
    contract          VARCHAR(10)     NULL,
    prev_open         FLOAT          NULL,
    high              FLOAT          NULL,
    low               FLOAT          NULL,
    last              FLOAT          NULL,
    price_change      FLOAT          NULL,
    percent_change    FLOAT          NULL,
    volume            BIGINT         NULL,
    oi                BIGINT         NULL,
    raw               NVARCHAR(MAX)  NULL,
    source_table      NVARCHAR(100)  NOT NULL,
    created_date      DATETIME       NOT NULL,
    snapshot_date     DATE           NOT NULL,
    snapshot_date_ol  DATE           NOT NULL
);
GO



























