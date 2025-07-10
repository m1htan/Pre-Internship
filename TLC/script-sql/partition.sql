CREATE OR ALTER PROCEDURE dbo.sp_CreateAllPartitionedTables
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartYear INT = 2020;
    DECLARE @EndYear INT = 2050;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @TableName NVARCHAR(128);

    -- 0. TỰ ĐỘNG DROP các bảng đang dùng partition scheme
    DECLARE drop_cursor CURSOR FOR
        SELECT t.name
        FROM sys.tables t
        JOIN sys.indexes i ON t.object_id = i.object_id
        JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
        WHERE ps.name = 'ps_snapshot_date_range';

    OPEN drop_cursor;
    FETCH NEXT FROM drop_cursor INTO @TableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = 'DROP TABLE IF EXISTS dbo.[' + @TableName + ']';
        EXEC sp_executesql @sql;
        FETCH NEXT FROM drop_cursor INTO @TableName;
    END

    CLOSE drop_cursor;
    DEALLOCATE drop_cursor;

    -- 1. DROP Partition Scheme & Function
    IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'ps_snapshot_date_range')
        DROP PARTITION SCHEME ps_snapshot_date_range;

    IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_snapshot_date_range')
        DROP PARTITION FUNCTION pf_snapshot_date_range;

    -- 2. Tạo Partition Function động
    DECLARE @values NVARCHAR(MAX) = '(';
    DECLARE @y INT = @StartYear, @m INT;

    SET @y = @StartYear;
    WHILE @y <= @EndYear
    BEGIN
        SET @m = 1;
        WHILE @m <= 12
        BEGIN
            SET @values += '''' + CONVERT(VARCHAR(10), DATEFROMPARTS(@y, @m, 1), 120) + ''',';
            SET @m += 1;
        END
        SET @y += 1;
    END
    -- Xóa dấu phẩy cuối + đóng ngoặc
    SET @values = LEFT(@values, LEN(@values) - 1) + ')';

    SET @sql = '
        CREATE PARTITION FUNCTION pf_snapshot_date_range (DATE)
        AS RANGE LEFT FOR VALUES ' + @values + ';';
    EXEC sp_executesql @sql;

    -- 3. Tạo Partition Scheme
    SET @sql = '
        CREATE PARTITION SCHEME ps_snapshot_date_range
        AS PARTITION pf_snapshot_date_range
        ALL TO ([PRIMARY]);';
    EXEC sp_executesql @sql;

    -- 4. Danh sách bảng cần tạo lại
    DECLARE @TableList TABLE (TableName NVARCHAR(128));
    INSERT INTO @TableList (TableName)
    VALUES
        ('stg_barchart_HOQ25_uco_price'), ('stg_barchart_HOU25_uco_price'),
        ('stg_barchart_HOV25_uco_price'), ('stg_barchart_HOX25_uco_price'),
        ('stg_barchart_HOZ25_uco_price'), ('stg_barchart_LFQ25_uco_price'),
        ('stg_barchart_LFU25_uco_price'), ('stg_barchart_LFV25_uco_price'),
        ('stg_barchart_LFX25_uco_price'), ('stg_barchart_LFZ25_uco_price'),
        ('stg_barchart_HOF26_uco_price'), ('stg_barchart_HOG26_uco_price'),
        ('stg_barchart_HOH26_uco_price'), ('stg_barchart_HOJ26_uco_price'),
        ('stg_barchart_HOK26_uco_price'), ('stg_barchart_HON26_uco_price'),
        ('stg_barchart_HOM26_uco_price'), ('stg_barchart_HOQ26_uco_price'),
        ('stg_barchart_HOU26_uco_price'), ('stg_barchart_HOV26_uco_price'),
        ('stg_barchart_HOX26_uco_price'), ('stg_barchart_HOZ26_uco_price'),
        ('stg_barchart_LFF26_uco_price'), ('stg_barchart_LFG26_uco_price'),
        ('stg_barchart_LFH26_uco_price'), ('stg_barchart_LFJ26_uco_price'),
        ('stg_barchart_LFK26_uco_price'), ('stg_barchart_LFN26_uco_price'),
        ('stg_barchart_LFM26_uco_price'), ('stg_barchart_LFQ26_uco_price'),
        ('stg_barchart_LFU26_uco_price'), ('stg_barchart_LFV26_uco_price'),
        ('stg_barchart_LFX26_uco_price'), ('stg_barchart_LFZ26_uco_price');

    -- 5. Tạo bảng mới
    DECLARE table_cursor CURSOR FOR SELECT TableName FROM @TableList;
    OPEN table_cursor;
    FETCH NEXT FROM table_cursor INTO @TableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = '
        CREATE TABLE dbo.[' + @TableName + '] (
            timing            DATETIME        NOT NULL,
            contract          VARCHAR(10)     NULL,
            [open]            FLOAT           NULL,
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
            snapshot_date_oi  DATE            NOT NULL
        ) ON ps_snapshot_date_range(snapshot_date);';

        EXEC sp_executesql @sql;
        FETCH NEXT FROM table_cursor INTO @TableName;
    END

    CLOSE table_cursor;
    DEALLOCATE table_cursor;
END;
GO

EXEC dbo.sp_CreateAllPartitionedTables;
