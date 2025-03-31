import os
import pandas as pd
import numpy as np
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
# function for judging annotation level
nl_columns = ["NL-1 Matched", "NL-2 Matched", "NL-4 Matched", "NL-6 Matched", "NL-8 Matched", "NL-10 Matched",
              "NL-12 Matched"]
sn_columns = [("sn-1-C-diagnostic ions", "sn-1-diagnostic ions", "sn-1-com"),
              ("sn-2-C-diagnostic ions", "sn-2-diagnostic ions", "sn-2-com"),
              ("sn-3-C-diagnostic ions", "sn-3-diagnostic ions", "sn-3-com")]
required_columns = ["ID", "Subclass", "Precursor RT", "Precursor m/z", "TheoMz", "Formula", "ADDUCT_TYPE",
                    "sn-composition", "double bond-NL-2", "Annotation level", 'sn-diagnostic ions',
                    'C=C diagnostic ions']
def process_sheet(df):

    if 'Subclass' not in df.columns:
        print("Warning: 'Subclass' column not found!")
        return df
    subclass_targets = ['LPC', 'LPC-O', 'LPC-P', 'LPE', 'LPE-O', 'LPE-P']
    annotation_level = 'none'
    if df['Subclass'].iloc[0] in subclass_targets:
        annotation_level = 'level 1' if 'double bond-NL-2' in df.columns else 'level 2'
    else:
        annotation_level = 'level 1' if 'sn-2-com' in df.columns and 'double bond-NL-2' in df.columns else 'level 2' if 'sn-2-com' in df.columns else 'level 3'
    df.loc[0, 'Annotation level'] = annotation_level
    return df

 # function for arrangment some columns
def process_excel_step2(df):
    if 'MS2mz' not in df.columns:
        print("Warning: 'MS2mz' column not found!")
        return None
    columns_to_drop = ['sn_2_chain_b', 'sn_1_chain_d', 'sn_3_chain_f']
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
    df.loc[:, 'MS2mz'] = df['MS2mz'].astype(str)
    df.loc[:, 'MS2i'] = df['MS2i'].astype(str)
    df.at[0, 'MS2mz'] = ','.join(df['MS2mz'].dropna().tolist())
    df.at[0, 'MS2i'] = ','.join(df['MS2i'].dropna().tolist())
    df.loc[1:, ['MS2mz', 'MS2i']] = ''
    def create_C_C_diagnostic_ions(row):
        return ";".join(str(row[col]) for col in nl_columns if col in row and pd.notna(row[col]))
    df['C=C diagnostic ions'] = df.apply(create_C_C_diagnostic_ions, axis=1)
    def create_sn_diagnostic_ions(row):
        sn_values = []
        for cols in sn_columns:
            valid_parts = [str(row[col]) for col in cols if col in row and pd.notna(row[col])]
            if valid_parts:
                sn_values.append(",".join(valid_parts))
        return ";".join(sn_values)
    df['sn-diagnostic ions'] = df.apply(create_sn_diagnostic_ions, axis=1)
    for column in required_columns:
        if column not in df.columns:
            print(f"Warning: Missing column '{column}', filling with None.")
            df[column] = None
    df = df[required_columns]
    if 'ADDUCT_TYPE' in df.columns:
        df.loc[1:, 'ADDUCT_TYPE'] = ''
    return df

# function for dealing files' format
def process_and_merge_files(input_folder):
    formatted_data = {}
    for filename in os.listdir(input_folder):
        if filename.endswith(".xlsx"):
            input_path = os.path.join(input_folder, filename)
            workbook = pd.ExcelFile(input_path)
            sheet_data = {}
            for sheet_name in workbook.sheet_names:
                df = pd.read_excel(workbook, sheet_name=sheet_name)
                processed_df = process_sheet(df)
                formatted_df = process_excel_step2(processed_df)
                if formatted_df is not None:
                    sheet_data[sheet_name] = formatted_df
            if sheet_data:
                formatted_data[filename] = sheet_data
    return formatted_data

def join_unique_values(column_name, data):
    if column_name in data.columns:
        unique_values = data[column_name].dropna().unique()
        unique_values = [value for value in unique_values if value != '']
        return "/".join(map(str, unique_values))
    else:
        print(f"Warning: Column {column_name} does not exist!")
        return ""
    
# function for a readable excel
def simplify_and_integrate_data(merged_data):
    dataframes = {}
    for filename, id_data_combined in merged_data.items():
        for id_value, data in id_data_combined.items():
            sn_position_values = join_unique_values('sn-composition', data)
            c_c_position_values = join_unique_values('double bond-NL-2', data)
            sn_diagnostic_ions_values = join_unique_values('sn-diagnostic ions', data)
            c_c_diagnostic_ions_values = join_unique_values('C=C diagnostic ions', data)
            sn_position_values = sn_position_values if sn_position_values else "No data"
            c_c_position_values = c_c_position_values if c_c_position_values else "No data"
            sn_diagnostic_ions_values = sn_diagnostic_ions_values if sn_diagnostic_ions_values else "No data"
            c_c_diagnostic_ions_values = c_c_diagnostic_ions_values if c_c_diagnostic_ions_values else "No data"
            data.at[0, 'sn-position'] = sn_position_values
            data.at[0, 'C=C position'] = c_c_position_values
            data.at[0, 'sn-diagnostic ions'] = sn_diagnostic_ions_values
            data.at[0, 'C=C diagnostic ions'] = c_c_diagnostic_ions_values
            data = data.head(1)
            main_part = filename.rsplit('_', 1)[0]
            if main_part not in dataframes:
                dataframes[main_part] = []
            dataframes[main_part].append(data)
    return dataframes

# function for saving merged dataframe
def save_integrated_data(dataframes, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    for key, dfs in dataframes.items():
        output_file_path = os.path.join(output_folder, f"{key}.xlsx")
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            for i, df in enumerate(dfs):
                df = df.drop(columns=['sn-composition', 'double bond-NL-2'], errors='ignore')
                df.to_excel(writer, sheet_name=f"Sheet_{i + 1}", index=False)

# function for intergret all excels
def combine_sheets(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    for filename in os.listdir(input_folder):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(input_folder, filename)
            xls = pd.ExcelFile(file_path)
            combined_df = pd.DataFrame()
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            combined_wb = Workbook()
            combined_ws = combined_wb.active
            for r_idx, row in enumerate(dataframe_to_rows(combined_df, index=False, header=True), 1):
                for c_idx, value in enumerate(row, 1):
                    combined_ws.cell(row=r_idx, column=c_idx, value=value)
            output_file_path = os.path.join(output_folder, filename)
            combined_wb.save(output_file_path)
def main():
    input_folder = "output-step5"
    output_folder_step6_5_sheet = "output-step6"
    merged_data = process_and_merge_files(input_folder)
    integrated_data = simplify_and_integrate_data(merged_data)
    save_integrated_data(integrated_data, output_folder_step6_5_sheet)
    combine_sheets(output_folder_step6_5_sheet, output_folder_step6_5_sheet)

if __name__ == "__main__":
    main()
    print("Processing completed.")
