import os
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
def process_uncertain(original_sheet, index_data):
    precursor_mz = original_sheet['Precursor m/z'].iloc[0]
    difference = abs(index_data['TheoMz'] - precursor_mz)
    min_difference = difference.min()
    if min_difference >= 0.01:
        return None
    min_difference_index = difference.idxmin()
    matched_row = index_data.loc[min_difference_index]
    original_sheet.loc[:, matched_row.index] = matched_row.values
    return original_sheet
def process_others(original_sheet, index_data):
    if not isinstance(original_sheet, pd.DataFrame) or not isinstance(index_data, pd.DataFrame):
        raise TypeError("Both original_sheet and index_data must be pandas DataFrames")
    precursor_mz = original_sheet['Precursor m/z'].iloc[0]
    original_class = original_sheet['CLASS'].iloc[0]
    filtered_index_data = index_data[index_data['MAIN_CLASS'] == original_class]
    if filtered_index_data.empty:
        return None
    difference = abs(filtered_index_data['TheoMz'] - precursor_mz)
    min_difference = difference.min()
    if min_difference >= 0.01:
        return None
    min_difference_rows = filtered_index_data[difference == min_difference]
    if len(min_difference_rows) > 1:
        sheets = []
        for _, matched_row in min_difference_rows.iterrows():
            new_sheet = original_sheet.copy()
            new_sheet.loc[:, matched_row.index] = matched_row.values
            sheets.append(new_sheet)
        return sheets
    else:
        matched_row = min_difference_rows.iloc[0]
        original_sheet.loc[:, matched_row.index] = matched_row.values
        return original_sheet
def process_single_file(input_file, index_data_uncertain, index_data_other, output_folder):
    xls = pd.ExcelFile(input_file)
    processed_sheets = []
    for sheet_name in xls.sheet_names:
        input_data = pd.read_excel(input_file, sheet_name=sheet_name)
        if 'CLASS' in input_data.columns:
            try:
                if input_data['CLASS'].iloc[0] == 'Uncertain':
                    processed_data = process_uncertain(input_data, index_data_uncertain)
                else:
                    processed_data = process_others(input_data, index_data_other)
                if processed_data is not None:
                    if isinstance(processed_data, list):
                        for i, sheet in enumerate(processed_data):
                            processed_sheets.append((f"{sheet_name}_{i + 1}", sheet))
                    else:
                        processed_sheets.append((sheet_name, processed_data))
            except Exception as e:
                print(f"Error processing sheet {sheet_name} in file {os.path.basename(input_file)}: {e}")
        else:
            print(f"CLASS column missing in sheet {sheet_name} of file {os.path.basename(input_file)}")
    if processed_sheets:
        output_file = os.path.join(output_folder, os.path.basename(input_file))
        wb = Workbook()
        for sheet_name, data in processed_sheets:
            ws = wb.create_sheet(title=sheet_name)
            for r in dataframe_to_rows(data, index=False, header=True):
                ws.append(r)
        if len(wb.worksheets) > 1:
            wb.remove(wb.worksheets[0])
        wb.save(output_file)
    else:
        print(f"No data to save for file: {os.path.basename(input_file)}")
def process_original_data_optimized(input_folder, index_file, output_folder):
    index_data_uncertain = pd.read_excel(index_file, sheet_name='Uncertain')
    index_data_other = pd.read_excel(index_file, sheet_name='Others')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    input_files = [os.path.join(input_folder, filename) for filename in os.listdir(input_folder) if filename.endswith('.xlsx')]
    for input_file in input_files:
        process_single_file(input_file, index_data_uncertain, index_data_other, output_folder)

input_folder = 'output-step2'
index_file = 'index-step3.xlsx'
output_folder = 'output-step3'
process_original_data_optimized(input_folder, index_file, output_folder)
