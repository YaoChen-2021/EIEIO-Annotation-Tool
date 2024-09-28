import os
import pandas as pd
import numpy as np
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
# function for assorting annotation level
nl_columns = ["NL-1 Matched", "NL-2 Matched", "NL-4 Matched", "NL-6 Matched", "NL-8 Matched", "NL-10 Matched", "NL-12 Matched"]
sn_columns = [("sn-1-C-diagnostic ions", "sn-1-diagnostic ions", "sn-1-com"),
              ("sn-2-C-diagnostic ions", "sn-2-diagnostic ions", "sn-2-com"),
              ("sn-3-C-diagnostic ions", "sn-3-diagnostic ions", "sn-3-com")]
required_columns = ["ID", "Subclass", "Precursor RT", "Precursor m/z", "TheoMz", "Formula", "ADDUCT_TYPE",
                    "sn-composition", "double bond-NL-2", "Annotation level", 'sn-diagnostic ions', 'C=C diagnostic ions']
def process_sheet(df):
    subclass_targets = ['LPC', 'LPC-O', 'LPC-P', 'LPE', 'LPE-O', 'LPE-P']
    annotation_level = 'none'
    if 'Subclass' in df.columns:
        if df['Subclass'].iloc[0] in subclass_targets:
            annotation_level = 'level 1' if 'double bond-NL-2' in df.columns else 'level 2'
        else:
            annotation_level = 'level 1' if 'sn-2-com' in df.columns and 'double bond-NL-2' in df.columns else 'level 2' if 'sn-2-com' in df.columns else 'level 3'
    df.loc[0, 'Annotation level'] = annotation_level
    return df
# function for arrangment some columns
def process_excel_step2(df):
    if 'MS2mz' not in df.columns:
        return None
    columns_to_drop = ['sn_2_chain_b', 'sn_1_chain_d', 'sn_3_chain_f']
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
    ms2mz_data = ','.join(map(str, df['MS2mz'].values.tolist()))
    df.at[0, 'MS2mz'] = ms2mz_data
    ms2i_data = ','.join(map(str, df['MS2i'].values.tolist()))
    df.at[0, 'MS2i'] = ms2i_data
    df.loc[1:, ['MS2mz', 'MS2i']] = ''
    df['C=C diagnostic ions'] = df.apply(
        lambda row: ';'.join([str(row[col]) for col in nl_columns if col in row and pd.notna(row[col])]), axis=1)
    def create_sn_diagnostic_ions(row):
        sn_values = []
        for cols in sn_columns:
            if all(col in row and pd.notna(row[col]) for col in cols):
                sn_values.append(f"{row[cols[0]]},{row[cols[1]]}-{row[cols[2]]}")
            elif any(col in row and pd.notna(row[col]) for col in cols):
                parts = [str(row[col]) for col in cols if col in row and pd.notna(row[col])]
                combined = ",".join(parts)
                if cols[2] in row and pd.notna(row[cols[2]]):
                    combined = f"{combined}-{row[cols[2]]}" if combined else str(row[cols[2]])
                sn_values.append(combined)
        return ";".join(sn_values)
    df['sn-diagnostic ions'] = df.apply(create_sn_diagnostic_ions, axis=1)
    for column in required_columns:
        if column not in df.columns:
            df[column] = None
    df = df[required_columns]
    if 'ADDUCT_TYPE' in df.columns:
        df.loc[1:, 'ADDUCT_TYPE'] = ''
    return df
# function for dealing excel format
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
    merged_data = {}
    for filename, sheets in formatted_data.items():
        id_data_combined = {}
        for sheet_name, data in sheets.items():
            if 'ID' in data.columns:
                grouped = data.groupby('ID')
                for id_value, group_data in grouped:
                    if id_value not in id_data_combined:
                        id_data_combined[id_value] = pd.DataFrame(columns=data.columns)
                    id_data_combined[id_value] = pd.concat([id_data_combined[id_value], group_data])
        merged_data[filename] = id_data_combined
    return merged_data
# function for a readable excel
def simplify_and_integrate_data(merged_data):
    dataframes = {}
    for filename, id_data_combined in merged_data.items():
        for id_value, data in id_data_combined.items():
            if 'TheoMz' not in data.columns or 'Precursor m/z' not in data.columns:
                continue
            first_theo_mz = data['TheoMz'].iloc[0]
            data['diff'] = np.abs(data['Precursor m/z'] - first_theo_mz)
            min_diff_index = data['diff'].idxmin()
            columns_to_keep = ['ID', 'Subclass', 'Precursor RT', 'Precursor m/z', 'TheoMz', 'Formula', 'ADDUCT_TYPE']
            selected_row = data.loc[min_diff_index, columns_to_keep]
            for col in columns_to_keep:
                data.at[0, col] = selected_row[col]
            def join_unique_values(column_name):
                if column_name in data.columns:
                    unique_values = data[column_name].dropna().unique()
                    unique_values_str = [str(value) for value in unique_values]
                    return "/".join(unique_values_str)
                return ""
            data.at[0, 'sn-position'] = join_unique_values('sn-composition')
            data.at[0, 'C=C position'] = join_unique_values('double bond-NL-2')
            data.at[0, 'sn-diagnostic ions'] = join_unique_values('sn-diagnostic ions')
            data.at[0, 'C=C diagnostic ions'] = join_unique_values('C=C diagnostic ions')
            if 'Annotation level' in data.columns:
                annotation_levels = data['Annotation level'].dropna().unique()
                if any(level in annotation_levels for level in ['level 1', 'level 2', 'level 3']):
                    for level in ['level 1', 'level 2', 'level 3']:
                        if level in annotation_levels:
                            data.at[0, 'Annotation level'] = level
                            break
                else:
                    data.at[0, 'Annotation level'] = ""
            data.drop(columns=['diff'], inplace=True)
            if 'sn-composition' in data.columns:
                data.drop(columns=['sn-composition'], inplace=True)
            if 'double bond-NL-2' in data.columns:
                data.drop(columns=['double bond-NL-2'], inplace=True)
            if 'Annotation level' in data.columns:
                annotation_level_index = data.columns.get_loc('Annotation level')
                data.insert(annotation_level_index, 'C=C position', data.pop('C=C position'))
                data.insert(annotation_level_index, 'sn-position', data.pop('sn-position'))
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
