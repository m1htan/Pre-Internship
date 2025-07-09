-- Danh sách các contract
DECLARE @contracts TABLE (contract_code VARCHAR(10))
INSERT INTO @contracts (contract_code)
VALUES 
('HOQ25'), ('HOU25'), ('HOV25'), ('HOX25'), ('HOZ25'),
('HOF26'), ('HOG26'), ('HOH26'), ('HOJ26'), ('HOK26'), ('HOM26'), ('HON26'), ('HOQ26'), ('HOU26'), ('HOV26'), ('HOX26'), ('HOZ26'),
('LFQ25'), ('LFU25'), ('LFV25'), ('LFX25'), ('LFZ25'),
('LFF26'), ('LFG26'), ('LFH26'), ('LFJ26'), ('LFK26'), ('LFM26'), ('LFN26'), ('LFQ26'), ('LFU26'), ('LFV26'), ('LFX26'), ('LFZ26');

-- Biến hỗ trợ
DECLARE @sql NVARCHAR(MAX) = ''

-- Sinh câu lệnh DROP + CREATE cho mỗi contract
SELECT @sql = @sql + '
IF OBJECT_ID(''stg_tmp_barchart_' + contract_code + '_uco_price_temp'', ''U'') IS NOT NULL
    DROP TABLE stg_tmp_barchart_' + contract_code + '_uco_price_temp;
CREATE TABLE stg_tmp_barchart_' + contract_code + '_uco_price_temp (
    timing            DATETIME       NOT NULL,
    [open]            FLOAT          NULL,
    high              FLOAT          NULL,
    low               FLOAT          NULL,
    last              FLOAT          NULL,
    price_change      FLOAT          NULL,
    percent_change    FLOAT          NULL,
    volume            BIGINT         NULL,
    oi                BIGINT         NULL,
    contract          VARCHAR(10)    NULL,
    raw               NVARCHAR(MAX)  NULL
);'
FROM @contracts
EXEC sp_executesql @sql