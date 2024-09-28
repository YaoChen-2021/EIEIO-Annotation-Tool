import os
import pandas as pd
import numpy as np
def process_PC(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2-C', 'sn-2', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
        input_data[f'{sn}-MS2i'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2-C diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-C-match'] = index_data.at[j, 'sn-2-C diagnostic ions']
                input_data.at[i, 'sn-2-C-chain'] = index_data.at[j, 'sn-2-C-chain']
                input_data.at[i, 'sn-2-C-MS2i'] = input_data.at[i, 'MS2i']
                sn_2_diagnostic_ion = index_data.at[j, 'sn-2 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_2_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-2-match'] = sn_2_diagnostic_ion
                        input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
                        input_data.at[i, 'sn-2-MS2i'] = input_data.at[k, 'MS2i']
                        if input_data.at[i, 'sn-2-C-MS2i'] < input_data.at[i, 'sn-2-MS2i']:
                            input_data.at[i, 'sn-2-C-match'] = ''
                            input_data.at[i, 'sn-2-C-chain'] = ''
                            input_data.at[i, 'sn-2-C-MS2i'] = ''
                            input_data.at[i, 'sn-2-match'] = ''
                            input_data.at[i, 'sn-2-chain'] = ''
                            input_data.at[i, 'sn-2-MS2i'] = ''
                        break
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-match'] = index_data.at[j, 'sn-1 diagnostic ions']
                input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
    # definite the format of Composition column as x:y
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    # recording the first data of 'Composition' column
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    # definite the sn-2-O-chain as “a:b”; sn-1-chain as “c:d”;
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"PC({row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['sn-2-MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        input_data.at[index, 'sn-2-C-diagnostic ions'] = row['sn-2-C-match']
                        input_data.at[index, 'sn-2-C-intensity'] = row['sn-2-C-MS2i']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_PCO(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2-C', 'sn-2', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
        input_data[f'{sn}-MS2i'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2-C diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-C-match'] = index_data.at[j, 'sn-2-C diagnostic ions']
                input_data.at[i, 'sn-2-C-chain'] = index_data.at[j, 'sn-2-C-chain']
                sn_2_o_diagnostic_ion = index_data.at[j, 'sn-2 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_2_o_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-2-match'] = sn_2_o_diagnostic_ion
                        input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
                        input_data.at[i, 'sn-2-MS2i'] = input_data.at[k, 'MS2i']
                        break
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-match'] = index_data.at[j, 'sn-1 diagnostic ions']
                input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"PC-O({row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['sn-2-MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_PCP(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2-C', 'sn-2', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
        input_data[f'{sn}-MS2i'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2-C diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-C-match'] = index_data.at[j, 'sn-2-C diagnostic ions']
                input_data.at[i, 'sn-2-C-chain'] = index_data.at[j, 'sn-2-C-chain']
                sn_2_o_diagnostic_ion = index_data.at[j, 'sn-2 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_2_o_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-2-match'] = sn_2_o_diagnostic_ion
                        input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
                        input_data.at[i, 'sn-2-MS2i'] = input_data.at[k, 'MS2i']
                        break
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-match'] = index_data.at[j, 'sn-1 diagnostic ions']
                input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"PC-P({row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_PE(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2-C', 'sn-2', 'sn-1-C', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
        input_data['sn-2-MS2i'] = ''
        input_data['sn-1-MS2i'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2-C diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-C-match'] = index_data.at[j, 'sn-2-C diagnostic ions']
                input_data.at[i, 'sn-2-C-chain'] = index_data.at[j, 'sn-2-C-chain']
                sn_2_o_diagnostic_ion = index_data.at[j, 'sn-2 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_2_o_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-2-match'] = sn_2_o_diagnostic_ion
                        input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
                        input_data.at[i, 'sn-2-MS2i'] = input_data.at[k, 'MS2i']
                        break
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1-C diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-C-match'] = index_data.at[j, 'sn-1-C diagnostic ions']
                input_data.at[i, 'sn-1-C-chain'] = index_data.at[j, 'sn-1-C-chain']
                sn_1_o_diagnostic_ion = index_data.at[j, 'sn-1 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_1_o_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-1-match'] = sn_1_o_diagnostic_ion
                        input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
                        input_data.at[i, 'sn-1-MS2i'] = input_data.at[k, 'MS2i']
                        break
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"PE({row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['sn-1-MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['sn-2-MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_PEO(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-match'] = index_data.at[j, 'sn-2 diagnostic ions']
                input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-match'] = index_data.at[j, 'sn-1 diagnostic ions']
                input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"PE-O({row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_PEP(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-match'] = index_data.at[j, 'sn-2 diagnostic ions']
                input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-match'] = index_data.at[j, 'sn-1 diagnostic ions']
                input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"PE-P({row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_SM(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-1-O', 'sn-1', 'sn-2']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
    input_data['sn-1-MS2i'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1-O diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-O-match'] = index_data.at[j, 'sn-1-O diagnostic ions']
                input_data.at[i, 'sn-1-O-chain'] = index_data.at[j, 'sn-1-O-chain']
                sn_1_o_diagnostic_ion = index_data.at[j, 'sn-1 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_1_o_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-1-match'] = sn_1_o_diagnostic_ion
                        input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
                        input_data.at[i, 'sn-1-MS2i'] = input_data.at[k, 'MS2i']
                        break
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-match'] = index_data.at[j, 'sn-2 diagnostic ions']
                input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"SM(d{row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['sn-1-MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_Cer(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2-O', 'sn-2', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
    input_data['sn-2-O-MS2i'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-match'] = index_data.at[j, 'sn-2 diagnostic ions']
                input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
                sn_2_o_diagnostic_ion = index_data.at[j, 'sn-2-O diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_2_o_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-2-O-match'] = sn_2_o_diagnostic_ion
                        input_data.at[i, 'sn-2-O-chain'] = index_data.at[j, 'sn-2-O-chain']
                        input_data.at[i, 'sn-2-O-MS2i'] = input_data.at[k, 'MS2i']
                        break
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-match'] = index_data.at[j, 'sn-1 diagnostic ions']
                input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"Cer(d{row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_Cer2OH(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2 diagnostic ion']) <= 0.05:
                input_data.at[i, 'sn-2-match'] = index_data.at[j, 'sn-2 diagnostic ion']
                input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1 diagnostic ion']) <= 0.05:
                input_data.at[i, 'sn-1-match'] = index_data.at[j, 'sn-1 diagnostic ion']
                input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"Cer(d{row2['sn-1-chain']}/{row['sn-2-chain']}(2OH))"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_DG(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-2-C', 'sn-2', 'sn-1']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chain'] = ''
        input_data[f'{sn}-MS2i'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-2-C diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-2-C-match'] = index_data.at[j, 'sn-2-C diagnostic ions']
                input_data.at[i, 'sn-2-C-chain'] = index_data.at[j, 'sn-2-C-chain']
                input_data.at[i, 'sn-2-C-MS2i'] = input_data.at[i, 'MS2i']
                sn_2_c_diagnostic_ion = index_data.at[j, 'sn-2 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_2_c_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-2-match'] = sn_2_c_diagnostic_ion
                        input_data.at[i, 'sn-2-chain'] = index_data.at[j, 'sn-2-chain']
                        input_data.at[i, 'sn-2-MS2i'] = input_data.at[k, 'MS2i']
                        break
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1 diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-match'] = index_data.at[j, 'sn-1 diagnostic ions']
                input_data.at[i, 'sn-1-chain'] = index_data.at[j, 'sn-1-chain']
                input_data.at[i, 'sn-1-MS2i'] = input_data.at[i, 'MS2i']
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chain'] = input_data['sn-2-chain'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chain'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chain'] = input_data['sn-1-chain'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chain'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chain'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    if sn_2_chain_a + sn_1_chain_c == first_composition_x and sn_2_chain_b + sn_1_chain_d == first_composition_y:
                        input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                        input_data.at[index, 'sn-composition'] = f"DG({row2['sn-1-chain']}/{row['sn-2-chain']})"
                        input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                        input_data.at[index, 'sn-1-intensity'] = row2['sn-1-MS2i']
                        input_data.at[index, 'sn-1-com'] = row2['sn-1-chain']
                        input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                        input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                        input_data.at[index, 'sn-2-intensity'] = row['sn-2-MS2i']
                        input_data.at[index, 'sn-2-com'] = row['sn-2-chain']
                        input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                        input_data.at[index, 'sn-2-C-diagnostic ions'] = row['sn-2-C-match']
                        input_data.at[index, 'sn-2-C-intensity'] = row['sn-2-C-MS2i']
                        break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_TG(input_data, index_file, index_sheet_name, columns_to_remove):
    index_data = pd.read_excel(index_file, sheet_name=index_sheet_name)
    for sn in ['sn-1-C', 'sn-1', 'sn-3-C', 'sn-3', 'sn-2']:
        input_data[f'{sn}-match'] = ''
        input_data[f'{sn}-chains'] = ''
        input_data[f'{sn}-MS2i'] = ''
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-1-C diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-1-C-match'] = index_data.at[j, 'sn-1-C diagnostic ions']
                input_data.at[i, 'sn-1-C-chains'] = index_data.at[j, 'sn-1-C-chains']
                input_data.at[i, 'sn-1-C-MS2i'] = input_data.at[i, 'MS2i']
                sn_1_o_diagnostic_ion = index_data.at[j, 'sn-1 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_1_o_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-1-match'] = sn_1_o_diagnostic_ion
                        input_data.at[i, 'sn-1-chains'] = index_data.at[j, 'sn-1-chains']
                        input_data.at[i, 'sn-1-MS2i'] = input_data.at[k, 'MS2i']
                        break
    for i in range(len(input_data)):
        for j in range(len(index_data)):
            if abs(input_data.at[i, 'MS2mz'] - index_data.at[j, 'sn-3-C diagnostic ions']) <= 0.05:
                input_data.at[i, 'sn-3-C-match'] = index_data.at[j, 'sn-3-C diagnostic ions']
                input_data.at[i, 'sn-3-C-chains'] = index_data.at[j, 'sn-3-C-chains']
                input_data.at[i, 'sn-3-C-MS2i'] = input_data.at[i, 'MS2i']
                sn_3_o_diagnostic_ion = index_data.at[j, 'sn-3 diagnostic ions']
                for k in range(len(input_data)):
                    if abs(input_data.at[k, 'MS2mz'] - sn_3_o_diagnostic_ion) <= 0.05:
                        input_data.at[i, 'sn-3-match'] = sn_3_o_diagnostic_ion
                        input_data.at[i, 'sn-3-chains'] = index_data.at[j, 'sn-3-chains']
                        input_data.at[i, 'sn-3-MS2i'] = input_data.at[k, 'MS2i']
                        break
    for i in range(len(input_data)): #sn-2 only output the best mathed 'MS2mz';
        ms2mz = input_data.at[i, 'MS2mz']
        min_difference = float('inf')
        min_index = -1
        for j in range(len(index_data)):
            difference = abs(ms2mz - index_data.at[j, 'sn-2 diagnostic ions'])
            if difference <= 0.01 and (input_data.at[i, 'sn-2-match'] is None or difference < min_difference):
                min_difference = difference
                min_index = j
        if min_index != -1:
            input_data.at[i, 'sn-2-match'] = index_data.at[min_index, 'sn-2 diagnostic ions']
            input_data.at[i, 'sn-2-chains'] = index_data.at[min_index, 'sn-2-chains']
            input_data.at[i, 'sn-2-MS2i'] = input_data.at[i, 'MS2i']
            # Clear previous matches if a new minimum difference is found
            for k in range(len(input_data)):
                if k != i and abs(input_data.at[k, 'MS2mz'] - index_data.at[min_index, 'sn-2 diagnostic ions']) <= 0.05:
                    input_data.at[k, 'sn-2-match'] = None
                    input_data.at[k, 'sn-2-chains'] = None
                    input_data.at[k, 'sn-2-MS2i'] = None
    input_data['x'] = input_data['Composition'].apply(lambda x: int(x.split(':')[0]) if not pd.isnull(x) else np.nan)
    input_data['y'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    first_composition_x, first_composition_y = None, None
    for index, row in input_data.iterrows():
        if not pd.isnull(row['Composition']):
            first_composition_x, first_composition_y = map(int, row['Composition'].split(':'))
            break
    input_data['sn-2-chains'] = input_data['sn-2-chains'].astype(str)
    input_data[['sn-2-chain-a', 'sn-2-chain-b']] = input_data['sn-2-chains'].str.extract(r'(\d+):(\d+)')
    input_data['sn-2-chains'] = input_data['sn-2-chain-a'] + ':' + input_data['sn-2-chain-b']
    input_data['sn-1-chains'] = input_data['sn-1-chains'].astype(str)
    input_data[['sn-1-chain-c', 'sn-1-chain-d']] = input_data['sn-1-chains'].str.extract(r'(\d+):(\d+)')
    input_data['sn-1-chains'] = input_data['sn-1-chain-c'] + ':' + input_data['sn-1-chain-d']
    input_data['sn-3-chains'] = input_data['sn-3-chains'].astype(str)
    input_data[['sn-3-chain-e', 'sn-3-chain-f']] = input_data['sn-3-chains'].str.extract(r'(\d+):(\d+)')
    input_data['sn-3-chains'] = input_data['sn-3-chain-e'] + ':' + input_data['sn-3-chain-f']
    input_data['sn-composition'] = np.nan
    for index, row in input_data.iterrows():
        if not pd.isnull(row['sn-2-chain-a']) and not pd.isnull(row['sn-2-chain-b']):
            sn_2_chain_a = int(row['sn-2-chain-a'])
            sn_2_chain_b = int(row['sn-2-chain-b'])
            for index2, row2 in input_data.iterrows():
                if not pd.isnull(row2['sn-1-chain-c']) and not pd.isnull(row2['sn-1-chain-d']):
                    sn_1_chain_c = int(row2['sn-1-chain-c'])
                    sn_1_chain_d = int(row2['sn-1-chain-d'])
                    for index3, row3 in input_data.iterrows():
                        if not pd.isnull(row3['sn-3-chain-e']) and not pd.isnull(row3['sn-3-chain-f']):
                            sn_3_chain_e = int(row3['sn-3-chain-e'])
                            sn_3_chain_f = int(row3['sn-3-chain-f'])
                            if sn_2_chain_a + sn_1_chain_c + sn_3_chain_e == first_composition_x and sn_2_chain_b + sn_1_chain_d + sn_3_chain_f == first_composition_y:
                                input_data['sn-composition'] = input_data['sn-composition'].astype(str)
                                input_data.at[index, 'sn-composition'] = f"TG({row2['sn-1-chains']}_/{row['sn-2-chains']}/_{row3['sn-3-chains']})"
                                input_data.at[index, 'sn-1-diagnostic ions'] = row2['sn-1-match']
                                input_data.at[index, 'sn-1-intensity'] = row2['sn-1-MS2i']
                                input_data.at[index, 'sn-1-com'] = row2['sn-1-chains']
                                input_data.at[index, 'sn_1_chain_d'] = row2['sn-1-chain-d']
                                input_data.at[index, 'sn-1-C-diagnostic ions'] = row2['sn-1-C-match']
                                input_data.at[index, 'sn-1-C-intensity'] = row2['sn-1-C-MS2i']
                                input_data.at[index, 'sn-2-diagnostic ions'] = row['sn-2-match']
                                input_data.at[index, 'sn-2-intensity'] = row['sn-2-MS2i']
                                input_data.at[index, 'sn-2-com'] = row['sn-2-chains']
                                input_data.at[index, 'sn_2_chain_b'] = row['sn-2-chain-b']
                                input_data.at[index, 'sn-3-diagnostic ions'] = row3['sn-3-match']
                                input_data.at[index, 'sn-3-intensity'] = row3['sn-3-MS2i']
                                input_data.at[index, 'sn-3-com'] = row3['sn-3-chains']
                                input_data.at[index, 'sn_3_chain_f'] = row3['sn-3-chain-f']
                                input_data.at[index, 'sn-3-C-diagnostic ions'] = row3['sn-3-C-match']
                                input_data.at[index, 'sn-3-C-intensity'] = row3['sn-3-C-MS2i']
                                break
    input_data = input_data.drop(columns=columns_to_remove, errors='ignore')
    return input_data
def process_LPC(input_data):
    input_data['sn_1_chain_d'] = input_data['Composition'].apply(lambda x: int(x.split(':')[1]) if not pd.isnull(x) else np.nan)
    return input_data
def process_original_data(input_folder, index_file, output_folder):
    index_df = pd.read_excel(index_file)
    for filename in os.listdir(input_folder):
        input_file = os.path.join(input_folder, filename)
        output_file = os.path.join(output_folder, filename)
        xls = pd.ExcelFile(input_file)
        writer = pd.ExcelWriter(output_file, engine='openpyxl')
        for sheet_name in xls.sheet_names:
            input_data = pd.read_excel(input_file, sheet_name=sheet_name)
            processed_data = None
            columns_to_remove = None
            if 'Subclass' in input_data.columns:
                subclass_value = input_data['Subclass'].iloc[0]
                if subclass_value == 'unmatched':
                    continue
                if subclass_value == 'PC':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-MS2i', 'sn-2-C-MS2i', 'sn-1-MS2i', 'sn-2-chain-b', 'sn-1-chain-c', 'sn-1-chain-d', 'sn-2-C-match', 'sn-2-C-chain', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_PC(input_data, index_file, 'PC', columns_to_remove)
                elif subclass_value == 'PC-O':
                    columns_to_remove = ['x', 'y', 'sn-1-MS2i','sn-2-C-MS2i','sn-2-chain-a', 'sn-2-chain-b', 'sn-2-MS2i', 'sn-1-chain-c', 'sn-2-C-chain', 'sn-1-chain-d', 'sn-2-C-match', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_PCO(input_data, index_file, 'PC-O', columns_to_remove)
                elif subclass_value == 'PC-P':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-2-MS2i', 'sn-1-chain-c', 'sn-2-C-chain', 'sn-1-chain-d', 'sn-2-C-match', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_PCP(input_data, index_file, 'PC-P', columns_to_remove)
                elif input_data['Subclass'].iloc[0] == 'PE':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-1-MS2i', 'sn-2-MS2i', 'sn-1-chain-c', 'sn-1-chain-d', 'sn-2-C-chain', 'sn-1-C-match', 'sn-1-C-chain', 'sn-2-C-match', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_PE(input_data, index_file, 'PE', columns_to_remove)
                elif input_data['Subclass'].iloc[0] == 'PE-O':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-1-chain-c', 'sn-1-chain-d', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_PEO(input_data, index_file, 'PE-O', columns_to_remove)
                elif input_data['Subclass'].iloc[0] == 'PE-P':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-1-chain-c', 'sn-1-chain-d', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_PEP(input_data, index_file, 'PE-P', columns_to_remove)
                elif input_data['Subclass'].iloc[0] == 'SM':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-1-MS2i', 'sn-1-chain-c', 'sn-1-chain-d', 'sn-1-O-match', 'sn-1-O-chain', 'sn-2-C-match', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_SM(input_data, index_file, 'SM', columns_to_remove)
                elif input_data['Subclass'].iloc[0] == 'Cer':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-2-O-MS2i', 'sn-2-O-match', 'sn-2-O-chain', 'sn-1-chain-c', 'sn-1-chain-d', 'sn-2-C-match', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_Cer(input_data, index_file, 'Cer', columns_to_remove)
                elif input_data['Subclass'].iloc[0] == 'Cer-2OH':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-1-chain-c', 'sn-1-chain-d', 'sn-2-C-match', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_Cer2OH(input_data, index_file, 'Cer-2OH', columns_to_remove)
                elif input_data['Subclass'].iloc[0] == 'DG':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-2-C-MS2i', 'sn-2-C-chain', 'sn-2-MS2i', 'sn-1-MS2i', 'sn-1-chain-c', 'sn-1-O-match', 'sn-1-O-chain',
                                         'sn-1-chain-d', 'sn-2-C-match', 'sn-2-match', 'sn-2-chain', 'sn-1-match', 'sn-1-chain']
                    processed_data = process_DG(input_data, index_file, 'DG', columns_to_remove)
                elif input_data['Subclass'].iloc[0] == 'TG':
                    columns_to_remove = ['x', 'y', 'sn-2-chain-a', 'sn-2-chain-b', 'sn-1-C-match', 'sn-3-C-match', 'sn-1-chain-c', 'sn-1-chain-d', 'sn-3-match', 'sn-3-chain-e', 'sn-3-chain-f',
                                         'sn-2-C-match', 'sn-2-match', 'sn-1-match', 'sn-1-C-chains', 'sn-1-C-MS2i', 'sn-1-chains', 'sn-1-MS2i', 'sn-3-C-chains', 'sn-3-C-MS2i', 'sn-3-chains', 'sn-3-MS2i', 'sn-2-chains', 'sn-2-MS2i']
                    processed_data = process_TG(input_data, index_file, 'TG', columns_to_remove)
                elif input_data['Subclass'].iloc[0] in ['LPC', 'LPC-O', 'LPC-P', 'LPE', 'LPE-O', 'LPE-P']:
                    processed_data = process_LPC(input_data)
                else:
                    print(f"No processing function defined for Subclass: {input_data['Subclass'].iloc[0]}")
            if processed_data is not None:
                processed_data.loc[1:, ['Precursor m/z', 'Precursor RT', 'CLASS', 'MAIN_CLASS', 'Subclass', 'TheoMz', 'ID', 'Composition', 'Formula', 'Mass', 'Adduct ion']] = float('nan')
                processed_data.to_excel(writer, sheet_name=sheet_name, index=False)
        if len(writer.sheets) > 0:
            for sheet_name in writer.sheets.keys():
                writer.book[sheet_name].sheet_state = 'visible'
            writer.close()
        else:
            print(f"No processing function defined for Subclass: {subclass_value}")
input_folder = 'output-step3'
index_file = 'index-step4.xlsx'
output_folder = 'output-step4'
process_original_data(input_folder, index_file, output_folder)