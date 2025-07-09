INSERT INTO stg_meta_data (
    domain, sub_domain, source_name, source_path, source_type,
    temp_name, target_name, created_date, updated_date, created_by
)
VALUES
-- ================= HO Contracts =================
('exchange', 'usda', 'barchart_loadms_HOQ25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOQ25_uco_price_temp', 'stg_barchart_HOQ25_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOU25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOU25_uco_price_temp', 'stg_barchart_HOU25_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOV25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOV25_uco_price_temp', 'stg_barchart_HOV25_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOX25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOX25_uco_price_temp', 'stg_barchart_HOX25_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOZ25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOZ25_uco_price_temp', 'stg_barchart_HOZ25_uco_price', GETDATE(), GETDATE(), 'minhtan'),

('exchange', 'usda', 'barchart_loadms_HOF26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOF26_uco_price_temp', 'stg_barchart_HOF26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOG26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOG26_uco_price_temp', 'stg_barchart_HOG26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOH26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOH26_uco_price_temp', 'stg_barchart_HOH26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOJ26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOJ26_uco_price_temp', 'stg_barchart_HOJ26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOK26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOK26_uco_price_temp', 'stg_barchart_HOK26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOM26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOM26_uco_price_temp', 'stg_barchart_HOM26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HON26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HON26_uco_price_temp', 'stg_barchart_HON26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOQ26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOQ26_uco_price_temp', 'stg_barchart_HOQ26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOU26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOU26_uco_price_temp', 'stg_barchart_HOU26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOV26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOV26_uco_price_temp', 'stg_barchart_HOV26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOX26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOX26_uco_price_temp', 'stg_barchart_HOX26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_HOZ26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_HOZ26_uco_price_temp', 'stg_barchart_HOZ26_uco_price', GETDATE(), GETDATE(), 'minhtan'),

-- ================= LF Contracts =================
('exchange', 'usda', 'barchart_loadms_LFQ25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFQ25_uco_price_temp', 'stg_barchart_LFQ25_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFU25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFU25_uco_price_temp', 'stg_barchart_LFU25_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFV25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFV25_uco_price_temp', 'stg_barchart_LFV25_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFX25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFX25_uco_price_temp', 'stg_barchart_LFX25_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFZ25.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFZ25_uco_price_temp', 'stg_barchart_LFZ25_uco_price', GETDATE(), GETDATE(), 'minhtan'),

('exchange', 'usda', 'barchart_loadms_LFF26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFF26_uco_price_temp', 'stg_barchart_LFF26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFG26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFG26_uco_price_temp', 'stg_barchart_LFG26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFH26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFH26_uco_price_temp', 'stg_barchart_LFH26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFJ26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFJ26_uco_price_temp', 'stg_barchart_LFJ26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFK26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFK26_uco_price_temp', 'stg_barchart_LFK26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFM26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFM26_uco_price_temp', 'stg_barchart_LFM26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFN26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFN26_uco_price_temp', 'stg_barchart_LFN26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFQ26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFQ26_uco_price_temp', 'stg_barchart_LFQ26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFU26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFU26_uco_price_temp', 'stg_barchart_LFU26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFV26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFV26_uco_price_temp', 'stg_barchart_LFV26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFX26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFX26_uco_price_temp', 'stg_barchart_LFX26_uco_price', GETDATE(), GETDATE(), 'minhtan'),
('exchange', 'usda', 'barchart_loadms_LFZ26.csv', '/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data', '.csv', 'stg_tmp_barchart_LFZ26_uco_price_temp', 'stg_barchart_LFZ26_uco_price', GETDATE(), GETDATE(), 'minhtan');

