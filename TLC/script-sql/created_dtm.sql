-----------------------------------------------

--Created d_date
IF OBJECT_ID('dbo.d_date', 'U') IS NOT NULL DROP TABLE dbo.d_date;
GO

CREATE TABLE dbo.d_date (
    date_id INT PRIMARY KEY,              -- YYYYMMDD
    date_actual DATE,
    crops VARCHAR(255),
    epoch BIGINT,
    day_suffix VARCHAR(4),
    day_name VARCHAR(9),
    day_of_week INT,
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
Go

--d_contract
IF OBJECT_ID('dbo.d_contract', 'U') IS NOT NULL DROP TABLE dbo.d_contract;
GO

CREATE TABLE dbo.d_contract (
    contract_id INT PRIMARY KEY,
    contract_code VARCHAR(50) NOT NULL,
    contract_name VARCHAR(255),
    created_date DATETIME,
    eff_dt INT,
    exp_dt INT,
    mo_ship VARCHAR(10),
    year_ship VARCHAR(10)
);
GO

--f_HO
CREATE TABLE f_barchart_HO_uco_price (
    contract_id INT NOT NULL,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(10),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume FLOAT,
    oi FLOAT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT NOT NULL,
    contract_date_id INT,
    lbd DATE,
    contract_date DATE,
    contract_date_fmt VARCHAR(20),
    source_table VARCHAR(100),

    CONSTRAINT PK_f_barchart_HO_uco_price PRIMARY KEY (contract_id, date_id),
    CONSTRAINT FK_f_barchart_HO_uco_price_contract FOREIGN KEY (contract_id) REFERENCES d_contract(contract_id),
    CONSTRAINT FK_f_barchart_HO_uco_price_date FOREIGN KEY (date_id) REFERENCES d_date(date_id)
);
GO

--f_LF
CREATE TABLE f_barchart_LF_uco_price (
    contract_id INT NOT NULL,
    prev_contract_id INT,
    prev_open FLOAT,
    mo VARCHAR(10),
    last FLOAT,
    prev_last FLOAT,
    change FLOAT,
    high FLOAT,
    low FLOAT,
    volume FLOAT,
    oi FLOAT,
    spread FLOAT,
    ma_200 FLOAT,
    ma_50 FLOAT,
    date_id INT NOT NULL,
    contract_date_id INT,
    lbd DATE,
    contract_date DATE,
    contract_date_fmt VARCHAR(20),
    source_table VARCHAR(100),

    CONSTRAINT PK_f_barchart_LF_uco_price PRIMARY KEY (contract_id, date_id),
    CONSTRAINT FK_f_barchart_LF_uco_price_contract FOREIGN KEY (contract_id) REFERENCES d_contract(contract_id),
    CONSTRAINT FK_f_barchart_LF_uco_price_date FOREIGN KEY (date_id) REFERENCES d_date(date_id)
);
GO

