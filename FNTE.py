import os
from openpyxl import Workbook

def list_folders_to_excel(root_folder, output_excel):
    # Create a new Excel workbook and select the active worksheet
    wb = Workbook()
    ws = wb.active
    ws.title = "Folder Names"

    # Write header
    ws.append(["Folder Name", "Full Path"])

    # List all folders in the root directory (non-recursive)
    for item in os.listdir(root_folder):
        item_path = os.path.join(root_folder, item)
        if os.path.isdir(item_path):
            ws.append([item, item_path])

    # Save the workbook
    wb.save(output_excel)
    print(f"Saved folder list to: {output_excel}")

# === USAGE ===
root_folder = r"C:\Users\tony.orimba\Downloads\_.dataloop_exports_68772e75d74018d45a11940b_1753104810417"   
output_excel = "DLBatch9.xlsx"
list_folders_to_excel(root_folder, output_excel)
