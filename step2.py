import os
import pandas as pd
def process_excel(input_filename, output_filename):
    xls = pd.ExcelFile(input_filename)
    with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
        for sheet_name in xls.sheet_names:
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                if 'MS2mz' not in df.columns:
                    continue
                df['CLASS'] = None
                class_values = df['CLASS']
                MS2mz_values = df['MS2mz']
                if (((MS2mz_values - 184.073).abs() <= 0.5).any() and
                        (((MS2mz_values - 224.107).abs() <= 0.5).any() or
                         ((MS2mz_values - 226.085).abs() <= 0.5).any()) and
                        not ((MS2mz_values - 142.026).abs() <= 0.5).any()):
                    pc_index = class_values.index[class_values.isnull()].min()
                    df.loc[pc_index, 'CLASS'] = 'PC'
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                elif (((MS2mz_values - 184.073).abs() <= 0.5).any() and
                      (((MS2mz_values - 224.107).abs() <= 0.5).any() or
                       ((MS2mz_values - 226.085).abs() <= 0.5).any()) and
                      ((MS2mz_values - 142.026).abs() <= 0.5).any() and
                      ((MS2mz_values - 182.058).abs() <= 0.5).any()):
                    df_pc = df.copy()
                    df_pe = df.copy()
                    pc_index = class_values.index[class_values.isnull()].min()
                    df_pc.loc[pc_index, 'CLASS'] = 'PC'
                    df_pe.loc[pc_index, 'CLASS'] = 'PE'
                    df_pc.to_excel(writer, sheet_name=sheet_name + '_PC', index=False)
                    df_pe.to_excel(writer, sheet_name=sheet_name + '_PE', index=False)
                elif (((MS2mz_values - 184.073).abs() <= 0.5).any() and
                      (((MS2mz_values - 225.100).abs() <= 0.5).any() or
                       ((MS2mz_values - 253.085).abs() <= 0.5).any())):
                    sm_index = class_values.index[class_values.isnull()].min()
                    df.loc[sm_index, 'CLASS'] = 'SM'
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                elif (((MS2mz_values - 142.026).abs() <= 0.5).any() and
                      ((MS2mz_values - 182.058).abs() <= 0.5).any()):
                    pe_index = class_values.index[class_values.isnull()].min()
                    df.loc[pe_index, 'CLASS'] = 'PE'
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    uncertain_index = class_values.index[class_values.isnull()].min()
                    df.loc[uncertain_index, 'CLASS'] = 'Uncertain'
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            except Exception as e:
def process_files_and_profile(input_folder, output_folder):
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".xlsx"):
            input_file_path = os.path.join(input_folder, file_name)
            output_file_path = os.path.join(output_folder, file_name)
            process_excel(input_file_path, output_file_path)
input_folder = "output-step1"
output_folder = "output-step2"
process_files_and_profile(input_folder, output_folder)
