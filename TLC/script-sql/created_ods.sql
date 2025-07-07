
CREATE TABLE ods_date (
    date_id INT PRIMARY KEY,              -- YYYYMMDD
    date_actual DATE,
    crops VARCHAR(255),
    epoch BIGINT,
    day_suffix VARCHAR(4),                -- 1st, 2nd, 3rd, ...
    day_name VARCHAR(9),
    day_of_week INT,                      -- Monday = 1, Sunday = 7
    day_of_month INT,
    day_of_quarter INT,
    day_of_year INT,
    week_of_month INT,
    week_of_year INT,
    week_of_year_iso CHAR(10),
    month_actual INT,
    month_name VARCHAR(9),
    month_name_abbreviated CHAR(3),
    quarter_actual INT,
    quarter_name VARCHAR(9),
    year_actual INT,
    first_day_of_week DATE,
    last_day_of_week DATE
);
GO

IF OBJECT_ID('dbo.ods_contract','U') IS NOT NULL
    DROP TABLE dbo.ods_contract;
GO

-- Creating table ods_contract
CREATE TABLE ods_contract (
    contract_id INT PRIMARY KEY IDENTITY(1,1),  -- Khóa chính, tự động tăng
    contract_code VARCHAR(50) NOT NULL,        -- Mã hợp đồng, không cho phép NULL
    contract_name VARCHAR(255),                -- Tên hợp đồng, cho phép NULL
    created_date DATETIME,                     -- Ngày tạo hợp đồng
    eff_dt INT,                               -- Ngày hiệu lực (YYYYMMDD)
    exp_dt INT,                               -- Ngày hết hạn (YYYYMMDD)
    mo_ship VARCHAR(10),                      -- Tháng giao hàng (ví dụ: '1', '2', ..., '12')
    year_ship VARCHAR(10)                     -- Năm giao hàng (ví dụ: '2025', '2050')
);


IF OBJECT_ID('dbo.ods_barchart_HON25_uco_price','U') IS NOT NULL
    DROP TABLE dbo.ods_barchart_HON25_uco_price;
GO

-- Creating table ods_barchart_HON25_uco_price
CREATE TABLE ods_barchart_HON25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_HOQ25_uco_price
CREATE TABLE ods_barchart_HOQ25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_HOU25_uco_price
CREATE TABLE ods_barchart_HOU25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_HOV25_uco_price
CREATE TABLE ods_barchart_HOV25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_HOX25_uco_price
CREATE TABLE ods_barchart_HOX25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_HOZ25_uco_price
CREATE TABLE ods_barchart_HOZ25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_LFN25_uco_price
CREATE TABLE ods_barchart_LFN25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_LFQ25_uco_price
CREATE TABLE ods_barchart_LFQ25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_LFU25_uco_price
CREATE TABLE ods_barchart_LFU25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_LFV25_uco_price
CREATE TABLE ods_barchart_LFV25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_LFX25_uco_price
CREATE TABLE ods_barchart_LFX25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

-- Creating table ods_barchart_LFZ25_uco_price
CREATE TABLE ods_barchart_LFZ25_uco_price (
    contract_id INT,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(50),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    oi BIGINT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT,
    contract_date_id INT,
    lbd DATE,
    contract_date DATETIME,
    contract_date_fmt VARCHAR(50),
    FOREIGN KEY (contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (prev_contract_id) REFERENCES ods_contract(contract_id),
    FOREIGN KEY (date_id) REFERENCES ods_date(date_id),
    FOREIGN KEY (contract_date_id) REFERENCES ods_date(date_id)
);

--Import ods_date
DECLARE @current_date DATE = '20000101';
DECLARE @end_date DATE = '20501231';

WHILE @current_date <= @end_date
BEGIN
    DECLARE @date_id INT = CAST(CONVERT(VARCHAR(😎, @current_date, 112) AS INT);
    DECLARE @epoch BIGINT = DATEDIFF(SECOND, '19700101', @current_date);
    DECLARE @day INT = DAY(@current_date);
    DECLARE @day_suffix VARCHAR(4);

    IF @day IN (11, 12, 13)
        SET @day_suffix = CAST(@day AS VARCHAR) + 'th';
    ELSE
        SET @day_suffix = CAST(@day AS VARCHAR) +
            CASE RIGHT(CAST(@day AS VARCHAR), 1)
                WHEN '1' THEN 'st'
                WHEN '2' THEN 'nd'
                WHEN '3' THEN 'rd'
                ELSE 'th'
            END;

    DECLARE @day_name VARCHAR(9) = DATENAME(WEEKDAY, @current_date);
    DECLARE @day_of_week INT = DATEPART(WEEKDAY, @current_date); -- Sunday = 1
    SET @day_of_week = CASE @day_of_week WHEN 1 THEN 7 ELSE @day_of_week - 1 END; -- Shift Mon = 1

    DECLARE @day_of_month INT = DAY(@current_date);
    DECLARE @day_of_year INT = DATEPART(DAYOFYEAR, @current_date);
    DECLARE @quarter INT = DATEPART(QUARTER, @current_date);
    DECLARE @quarter_name VARCHAR(9) =
        CASE @quarter WHEN 1 THEN 'First'
                      WHEN 2 THEN 'Second'
                      WHEN 3 THEN 'Third'
                      WHEN 4 THEN 'Fourth' END;

    DECLARE @month_actual INT = MONTH(@current_date);
    DECLARE @month_name VARCHAR(9) = DATENAME(MONTH, @current_date);
    DECLARE @month_abbr CHAR(3) = LEFT(DATENAME(MONTH, @current_date), 3);
    DECLARE @year_actual INT = YEAR(@current_date);

    DECLARE @first_day_of_quarter DATE = DATEADD(QUARTER, DATEDIFF(QUARTER, 0, @current_date), 0);
    DECLARE @day_of_quarter INT = DATEDIFF(DAY, @first_day_of_quarter, @current_date) + 1;

    DECLARE @week_of_year INT = DATEPART(WEEK, @current_date);
    DECLARE @week_of_month INT = (DAY(@current_date) + DATEPART(WEEKDAY, DATEFROMPARTS(YEAR(@current_date), MONTH(@current_date), 1)) - 2) / 7 + 1;

    DECLARE @week_of_year_iso CHAR(10) =
        CAST(DATEPART(ISO_WEEK, @current_date) AS VARCHAR) + '-' + RIGHT(CAST(@year_actual AS VARCHAR), 4);

    DECLARE @first_day_of_week DATE = DATEADD(DAY, -(@day_of_week - 1), @current_date);
    DECLARE @last_day_of_week DATE = DATEADD(DAY, 7 - @day_of_week, @current_date);

    INSERT INTO ods_date (
        date_id, date_actual, crops, epoch, day_suffix, day_name, day_of_week,
        day_of_month, day_of_quarter, day_of_year,
        week_of_month, week_of_year, week_of_year_iso,
        month_actual, month_name, month_name_abbreviated,
        quarter_actual, quarter_name, year_actual,
        first_day_of_week, last_day_of_week
    )
    VALUES (
        @date_id, @current_date, NULL, @epoch, @day_suffix, @day_name, @day_of_week,
        @day_of_month, @day_of_quarter, @day_of_year,
        @week_of_month, @week_of_year, @week_of_year_iso,
        @month_actual, @month_name, @month_abbr,
        @quarter, @quarter_name, @year_actual,
        @first_day_of_week, @last_day_of_week
    );

    SET @current_date = DATEADD(DAY, 1, @current_date);
END;
GO