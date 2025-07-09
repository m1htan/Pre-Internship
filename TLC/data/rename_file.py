import os

# Danh sách các mã hợp đồng
contracts = [
    'HOQ25', 'HOU25', 'HOV25', 'HOX25', 'HOZ25',
    'HOF26', 'HOG26', 'HOH26', 'HOJ26', 'HOK26', 'HOM26', 'HON26',
    'HOQ26', 'HOU26', 'HOV26', 'HOX26', 'HOZ26',
    'LFQ25', 'LFU25', 'LFV25', 'LFX25', 'LFZ25',
    'LFF26', 'LFG26', 'LFH26', 'LFJ26', 'LFK26',
    'LFM26', 'LFN26', 'LFQ26', 'LFU26', 'LFV26',
    'LFX26', 'LFZ26',
]

# Đường dẫn tới thư mục chứa các file CSV
folder_path = "/Users/minhtan/Documents/GitHub/Pre-Internship/TLC/data/raw_data"

def rename_files():
    for contract in contracts:
        old_name = f"{contract}.csv"
        new_name = f"barchart_loadms_{contract}.csv"

        old_path = os.path.join(folder_path, old_name)
        new_path = os.path.join(folder_path, new_name)

        if os.path.exists(old_path):
            os.rename(old_path, new_path)
            print(f"Đã đổi tên: {old_name} → {new_name}")
        else:
            print(f"Không tìm thấy: {old_name}")

if __name__ == "__main__":
    rename_files()
