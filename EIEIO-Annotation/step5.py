import os
import pandas as pd
def process_excel_DU1(input_data, index_data):
    first_precursor_mz = input_data['Precursor m/z'].iloc[0]
    index_data['Precursor m/z-NL-1'] = first_precursor_mz - index_data['NL-1']
    index_data['Precursor m/z-NL-2'] = first_precursor_mz - index_data['NL-2']
    index_data['Precursor m/z-NL-db-1'] = first_precursor_mz - index_data['NL-db-1']
    ms2i_values = input_data['MS2i']
    for index, row in index_data.iterrows():
        ms2mz_NL1 = row['Precursor m/z-NL-1']
        ms2mz_NL2 = row['Precursor m/z-NL-2']
        ms2mz_NLdb = row['Precursor m/z-NL-db-1']
        matched_input_data_NL1 = input_data[(input_data['MS2mz'] - ms2mz_NL1).abs() < 0.05]
        matched_input_data_NL2 = input_data[(input_data['MS2mz'] - ms2mz_NL2).abs() < 0.05]
        matched_input_data_NLdb = input_data[(input_data['MS2mz'] - ms2mz_NLdb).abs() < 0.05]
        if not matched_input_data_NL1.empty and not matched_input_data_NL2.empty:
            if matched_input_data_NLdb.empty:
                nl_db_intensity = 0
                nl1_intensity = ms2i_values[matched_input_data_NL1.index]
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                max_nl1_index = nl1_intensity.idxmax()
                max_nl2_index = nl2_intensity.idxmax()
                nl1_intensity = nl1_intensity[max_nl1_index]
                nl2_intensity = nl2_intensity[max_nl2_index]
            else:
                nl1_intensity = ms2i_values[matched_input_data_NL1.index]
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                nl_db_intensity = ms2i_values[matched_input_data_NLdb.index]
                max_nl1_index = nl1_intensity.idxmax()
                max_nl2_index = nl2_intensity.idxmax()
                min_nl_db_index = (ms2mz_NLdb - input_data.loc[matched_input_data_NLdb.index, 'MS2mz']).abs().idxmin()
                nl1_intensity = nl1_intensity[max_nl1_index]
                nl2_intensity = nl2_intensity[max_nl2_index]
                nl_db_intensity = nl_db_intensity[min_nl_db_index]
            if (nl1_intensity > nl_db_intensity).all() and (nl2_intensity > nl_db_intensity).all():
                input_data.loc[matched_input_data_NL1.index, 'NL-1 Matched'] = row['Precursor m/z-NL-1']
                input_data.loc[matched_input_data_NL2.index, 'NL-2 Matched'] = row['Precursor m/z-NL-2']
                input_data.loc[matched_input_data_NL2.index, 'double bond-NL-2'] = row['double bond position']
    columns_to_check = ['NL-1 Matched', 'NL-2 Matched', 'double bond-NL-2']
    for column in columns_to_check:
        if column in input_data.columns:
            input_data['NL-1 Matched'] = input_data['NL-1 Matched'].mask(input_data['NL-1 Matched'].duplicated(), '')
            input_data['NL-2 Matched'] = input_data['NL-2 Matched'].mask(input_data['NL-2 Matched'].duplicated(), '')
            input_data['double bond-NL-2'] = input_data['double bond-NL-2'].mask(input_data['double bond-NL-2'].duplicated(), '')
    return input_data

def process_excel_DU2(input_data, index_data):
    first_precursor_mz = input_data['Precursor m/z'].iloc[0]
    index_data['Precursor m/z-NL-1'] = first_precursor_mz - index_data['NL-1']
    index_data['Precursor m/z-NL-2'] = first_precursor_mz - index_data['NL-2']
    index_data['Precursor m/z-NL-4'] = first_precursor_mz - index_data['NL-4']
    index_data['Precursor m/z-NL-db-1'] = first_precursor_mz - index_data['NL-db-1']
    ms2i_values = input_data['MS2i']
    for index, row in index_data.iterrows():
        ms2mz_NL1 = row['Precursor m/z-NL-1']
        ms2mz_NL2 = row['Precursor m/z-NL-2']
        ms2mz_NL4 = row['Precursor m/z-NL-4']
        ms2mz_NLdb = row['Precursor m/z-NL-db-1']
        matched_input_data_NL1 = input_data[(input_data['MS2mz'] - ms2mz_NL1).abs() < 0.05]
        matched_input_data_NL2 = input_data[(input_data['MS2mz'] - ms2mz_NL2).abs() < 0.05]
        matched_input_data_NL4 = input_data[(input_data['MS2mz'] - ms2mz_NL4).abs() < 0.05]
        matched_input_data_NLdb = input_data[(input_data['MS2mz'] - ms2mz_NLdb).abs() < 0.05]
        if not matched_input_data_NL1.empty and not matched_input_data_NL2.empty and not matched_input_data_NL4.empty:
            if matched_input_data_NLdb.empty:
                nl_db_intensity = 0
                nl1_intensity = ms2i_values[matched_input_data_NL1.index]
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                max_nl1_index = nl1_intensity.idxmax()
                max_nl2_index = nl2_intensity.idxmax()
                nl1_intensity = nl1_intensity[max_nl1_index]
                nl2_intensity = nl2_intensity[max_nl2_index]
            else:
                nl1_intensity = ms2i_values[matched_input_data_NL1.index]
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                nl_db_intensity = ms2i_values[matched_input_data_NLdb.index]
                max_nl1_index = nl1_intensity.idxmax()
                max_nl2_index = nl2_intensity.idxmax()
                min_nl_db_index = (ms2mz_NLdb - input_data.loc[matched_input_data_NLdb.index, 'MS2mz']).abs().idxmin()
                nl1_intensity = nl1_intensity[max_nl1_index]
                nl2_intensity = nl2_intensity[max_nl2_index]
                nl_db_intensity = nl_db_intensity[min_nl_db_index]
            if (nl1_intensity > nl_db_intensity).all() and (nl2_intensity > nl_db_intensity).all():
                input_data.loc[matched_input_data_NL2.index, 'double bond-NL-2'] = row['double bond position']
                input_data.loc[matched_input_data_NL2.index, 'NL-2 Matched'] = row['Precursor m/z-NL-2']
                input_data.loc[matched_input_data_NL4.index, 'NL-4 Matched'] = row['Precursor m/z-NL-4']
    columns_to_check = ['double bond-NL-2', 'NL-2 Matched', 'NL-4 Matched']
    for column in columns_to_check:
        if column in input_data.columns:
            input_data['NL-2 Matched'] = input_data['NL-2 Matched'].mask(input_data['NL-2 Matched'].duplicated(), '')
            input_data['double bond-NL-2'] = input_data['double bond-NL-2'].mask(input_data['double bond-NL-2'].duplicated(), '')
            input_data['NL-4 Matched'] = input_data['NL-4 Matched'].mask(input_data['NL-4 Matched'].duplicated(), '')
    return input_data
def process_excel_DU3(input_data, index_data):
    first_precursor_mz = input_data['Precursor m/z'].iloc[0]
    index_data['Precursor m/z-NL-db-1'] = first_precursor_mz - index_data['NL-db-1']
    index_data['Precursor m/z-NL-2'] = first_precursor_mz - index_data['NL-2']
    index_data['Precursor m/z-NL-4'] = first_precursor_mz - index_data['NL-4']
    index_data['Precursor m/z-NL-6'] = first_precursor_mz - index_data['NL-6']
    ms2i_values = input_data['MS2i']
    for index, row in index_data.iterrows():
        ms2mz_NLdb = row['Precursor m/z-NL-db-1']
        ms2mz_NL2 = row['Precursor m/z-NL-2']
        ms2mz_NL4 = row['Precursor m/z-NL-4']
        ms2mz_NL6 = row['Precursor m/z-NL-6']
        matched_input_data_NLdb = input_data[(input_data['MS2mz'] - ms2mz_NLdb).abs() < 0.05]
        matched_input_data_NL2 = input_data[(input_data['MS2mz'] - ms2mz_NL2).abs() < 0.05]
        matched_input_data_NL4 = input_data[(input_data['MS2mz'] - ms2mz_NL4).abs() < 0.05]
        matched_input_data_NL6 = input_data[(input_data['MS2mz'] - ms2mz_NL6).abs() < 0.05]
        if not matched_input_data_NL2.empty:
            if matched_input_data_NLdb.empty:
                nl_db_intensity = 0
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                max_nl2_index = nl2_intensity.idxmax()
                nl2_intensity = nl2_intensity[max_nl2_index]
            else:
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                nl_db_intensity = ms2i_values[matched_input_data_NLdb.index]
                max_nl2_index = nl2_intensity.idxmax()
                min_nl_db_index = (ms2mz_NLdb - input_data.loc[matched_input_data_NLdb.index, 'MS2mz']).abs().idxmin()
                nl2_intensity = nl2_intensity[max_nl2_index]
                nl_db_intensity = nl_db_intensity[min_nl_db_index]
            if (nl2_intensity > nl_db_intensity).all():
                input_data.loc[matched_input_data_NL2.index, 'double bond-NL-2'] = row['double bond position']
                input_data.loc[matched_input_data_NL2.index, 'NL-2 Matched'] = row['Precursor m/z-NL-2']
                input_data.loc[matched_input_data_NL4.index, 'NL-4 Matched'] = row['Precursor m/z-NL-4']
                input_data.loc[matched_input_data_NL6.index, 'NL-6 Matched'] = row['Precursor m/z-NL-6']
    columns_to_check = ['double bond-NL-2', 'NL-2 Matched', 'NL-4 Matched', 'NL-6 Matched']
    for column in columns_to_check:
        if column in input_data.columns:
            input_data['double bond-NL-2'] = input_data['double bond-NL-2'].mask(input_data['double bond-NL-2'].duplicated(), '')
            input_data['NL-2 Matched'] = input_data['NL-2 Matched'].mask(input_data['NL-2 Matched'].duplicated(), '')
            input_data['NL-4 Matched'] = input_data['NL-4 Matched'].mask(input_data['NL-4 Matched'].duplicated(), '')
            input_data['NL-6 Matched'] = input_data['NL-6 Matched'].mask(input_data['NL-6 Matched'].duplicated(), '')
    return input_data
def process_excel_DU4(input_data, index_data):
    first_precursor_mz = input_data['Precursor m/z'].iloc[0]
    index_data['Precursor m/z-NL-db-1'] = first_precursor_mz - index_data['NL-db-1']
    index_data['Precursor m/z-NL-2'] = first_precursor_mz - index_data['NL-2']
    index_data['Precursor m/z-NL-4'] = first_precursor_mz - index_data['NL-4']
    index_data['Precursor m/z-NL-6'] = first_precursor_mz - index_data['NL-6']
    index_data['Precursor m/z-NL-8'] = first_precursor_mz - index_data['NL-8']
    ms2i_values = input_data['MS2i']
    for index, row in index_data.iterrows():
        ms2mz_NLdb = row['Precursor m/z-NL-db-1']
        ms2mz_NL2 = row['Precursor m/z-NL-2']
        ms2mz_NL4 = row['Precursor m/z-NL-4']
        ms2mz_NL6 = row['Precursor m/z-NL-6']
        ms2mz_NL8 = row['Precursor m/z-NL-8']
        matched_input_data_NLdb = input_data[(input_data['MS2mz'] - ms2mz_NLdb).abs() < 0.05]
        matched_input_data_NL2 = input_data[(input_data['MS2mz'] - ms2mz_NL2).abs() < 0.05]
        matched_input_data_NL4 = input_data[(input_data['MS2mz'] - ms2mz_NL4).abs() < 0.05]
        matched_input_data_NL6 = input_data[(input_data['MS2mz'] - ms2mz_NL6).abs() < 0.05]
        matched_input_data_NL8 = input_data[(input_data['MS2mz'] - ms2mz_NL8).abs() < 0.05]
        if not matched_input_data_NL2.empty:
            if matched_input_data_NLdb.empty:
                nl_db_intensity = 0
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                max_nl2_index = nl2_intensity.idxmax()
                nl2_intensity = nl2_intensity[max_nl2_index]
            else:
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                nl_db_intensity = ms2i_values[matched_input_data_NLdb.index]
                max_nl2_index = nl2_intensity.idxmax()
                min_nl_db_index = (ms2mz_NLdb - input_data.loc[matched_input_data_NLdb.index, 'MS2mz']).abs().idxmin()
                nl2_intensity = nl2_intensity[max_nl2_index]
                nl_db_intensity = nl_db_intensity[min_nl_db_index]
            if (nl2_intensity > nl_db_intensity).all():
                input_data.loc[matched_input_data_NL2.index, 'double bond-NL-2'] = row['double bond position']
                input_data.loc[matched_input_data_NL2.index, 'NL-2 Matched'] = row['Precursor m/z-NL-2']
                input_data.loc[matched_input_data_NL4.index, 'NL-4 Matched'] = row['Precursor m/z-NL-4']
                input_data.loc[matched_input_data_NL6.index, 'NL-6 Matched'] = row['Precursor m/z-NL-6']
                input_data.loc[matched_input_data_NL8.index, 'NL-8 Matched'] = row['Precursor m/z-NL-8']
    columns_to_check = ['double bond-NL-2', 'NL-2 Matched', 'NL-4 Matched', 'NL-6 Matched', 'NL-8 Matched']
    for column in columns_to_check:
        if column in input_data.columns:
            input_data['double bond-NL-2'] = input_data['double bond-NL-2'].mask(input_data['double bond-NL-2'].duplicated(), '')
            input_data['NL-2 Matched'] = input_data['NL-2 Matched'].mask(input_data['NL-2 Matched'].duplicated(), '')
            input_data['NL-4 Matched'] = input_data['NL-4 Matched'].mask(input_data['NL-4 Matched'].duplicated(), '')
            input_data['NL-6 Matched'] = input_data['NL-6 Matched'].mask(input_data['NL-6 Matched'].duplicated(), '')
            input_data['NL-8 Matched'] = input_data['NL-8 Matched'].mask(input_data['NL-8 Matched'].duplicated(), '')
    return input_data
def process_excel_DU5(input_data, index_data):
    first_precursor_mz = input_data['Precursor m/z'].iloc[0]
    index_data['Precursor m/z-NL-db-1'] = first_precursor_mz - index_data['NL-db-1']
    index_data['Precursor m/z-NL-2'] = first_precursor_mz - index_data['NL-2']
    index_data['Precursor m/z-NL-4'] = first_precursor_mz - index_data['NL-4']
    index_data['Precursor m/z-NL-6'] = first_precursor_mz - index_data['NL-6']
    index_data['Precursor m/z-NL-8'] = first_precursor_mz - index_data['NL-8']
    index_data['Precursor m/z-NL-10'] = first_precursor_mz - index_data['NL-10']
    ms2i_values = input_data['MS2i']
    for index, row in index_data.iterrows():
        ms2mz_NLdb = row['Precursor m/z-NL-db-1']
        ms2mz_NL2 = row['Precursor m/z-NL-2']
        ms2mz_NL4 = row['Precursor m/z-NL-4']
        ms2mz_NL6 = row['Precursor m/z-NL-6']
        ms2mz_NL8 = row['Precursor m/z-NL-8']
        ms2mz_NL10 = row['Precursor m/z-NL-10']
        matched_input_data_NLdb = input_data[(input_data['MS2mz'] - ms2mz_NLdb).abs() < 0.05]
        matched_input_data_NL2 = input_data[(input_data['MS2mz'] - ms2mz_NL2).abs() < 0.05]
        matched_input_data_NL4 = input_data[(input_data['MS2mz'] - ms2mz_NL4).abs() < 0.05]
        matched_input_data_NL6 = input_data[(input_data['MS2mz'] - ms2mz_NL6).abs() < 0.05]
        matched_input_data_NL8 = input_data[(input_data['MS2mz'] - ms2mz_NL8).abs() < 0.05]
        matched_input_data_NL10 = input_data[(input_data['MS2mz'] - ms2mz_NL10).abs() < 0.05]
        if not matched_input_data_NL2.empty:
            if matched_input_data_NLdb.empty:
                nl_db_intensity = 0
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                max_nl2_index = nl2_intensity.idxmax()
                nl2_intensity = nl2_intensity[max_nl2_index]
            else:
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                nl_db_intensity = ms2i_values[matched_input_data_NLdb.index]
                max_nl2_index = nl2_intensity.idxmax()
                min_nl_db_index = (ms2mz_NLdb - input_data.loc[matched_input_data_NLdb.index, 'MS2mz']).abs().idxmin()
                nl2_intensity = nl2_intensity[max_nl2_index]
                nl_db_intensity = nl_db_intensity[min_nl_db_index]
            if (nl2_intensity > nl_db_intensity).all():
                input_data.loc[matched_input_data_NL2.index, 'double bond-NL-2'] = row['double bond position']
                input_data.loc[matched_input_data_NL2.index, 'NL-2 Matched'] = row['Precursor m/z-NL-2']
                input_data.loc[matched_input_data_NL4.index, 'NL-4 Matched'] = row['Precursor m/z-NL-4']
                input_data.loc[matched_input_data_NL6.index, 'NL-6 Matched'] = row['Precursor m/z-NL-6']
                input_data.loc[matched_input_data_NL8.index, 'NL-8 Matched'] = row['Precursor m/z-NL-8']
                input_data.loc[matched_input_data_NL10.index, 'NL-10 Matched'] = row['Precursor m/z-NL-10']
    columns_to_check = ['double bond-NL-2', 'NL-2 Matched', 'NL-4 Matched', 'NL-6 Matched', 'NL-8 Matched', 'NL-10 Matched']
    for column in columns_to_check:
        if column in input_data.columns:
            input_data['double bond-NL-2'] = input_data['double bond-NL-2'].mask(input_data['double bond-NL-2'].duplicated(), '')
            input_data['NL-2 Matched'] = input_data['NL-2 Matched'].mask(input_data['NL-2 Matched'].duplicated(), '')
            input_data['NL-4 Matched'] = input_data['NL-4 Matched'].mask(input_data['NL-4 Matched'].duplicated(), '')
            input_data['NL-6 Matched'] = input_data['NL-6 Matched'].mask(input_data['NL-6 Matched'].duplicated(), '')
            input_data['NL-8 Matched'] = input_data['NL-8 Matched'].mask(input_data['NL-8 Matched'].duplicated(), '')
            input_data['NL-10 Matched'] = input_data['NL-10 Matched'].mask(input_data['NL-10 Matched'].duplicated(), '')
    return input_data
def process_excel_DU6(input_data, index_data):
    first_precursor_mz = input_data['Precursor m/z'].iloc[0]
    index_data['Precursor m/z-NL-2'] = first_precursor_mz - index_data['NL-2']
    index_data['Precursor m/z-NL-4'] = first_precursor_mz - index_data['NL-4']
    index_data['Precursor m/z-NL-6'] = first_precursor_mz - index_data['NL-6']
    index_data['Precursor m/z-NL-8'] = first_precursor_mz - index_data['NL-8']
    index_data['Precursor m/z-NL-10'] = first_precursor_mz - index_data['NL-10']
    index_data['Precursor m/z-NL-12'] = first_precursor_mz - index_data['NL-12']
    index_data['Precursor m/z-NL-db-1'] = first_precursor_mz - index_data['NL-db-1']
    ms2i_values = input_data['MS2i']
    for index, row in index_data.iterrows():
        ms2mz_NL2 = row['Precursor m/z-NL-2']
        ms2mz_NL4 = row['Precursor m/z-NL-4']
        ms2mz_NL6 = row['Precursor m/z-NL-6']
        ms2mz_NL8 = row['Precursor m/z-NL-8']
        ms2mz_NL10 = row['Precursor m/z-NL-10']
        ms2mz_NL12 = row['Precursor m/z-NL-12']
        ms2mz_NLdb = row['Precursor m/z-NL-db-1']
        matched_input_data_NL2 = input_data[(input_data['MS2mz'] - ms2mz_NL2).abs() < 0.05]
        matched_input_data_NL4 = input_data[(input_data['MS2mz'] - ms2mz_NL4).abs() < 0.05]
        matched_input_data_NL6 = input_data[(input_data['MS2mz'] - ms2mz_NL6).abs() < 0.05]
        matched_input_data_NL8 = input_data[(input_data['MS2mz'] - ms2mz_NL8).abs() < 0.05]
        matched_input_data_NL10 = input_data[(input_data['MS2mz'] - ms2mz_NL10).abs() < 0.05]
        matched_input_data_NL12 = input_data[(input_data['MS2mz'] - ms2mz_NL12).abs() < 0.05]
        matched_input_data_NLdb = input_data[(input_data['MS2mz'] - ms2mz_NLdb).abs() < 0.05]
        if not matched_input_data_NL2.empty:
            if matched_input_data_NLdb.empty:
                nl_db_intensity = 0
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                max_nl2_index = nl2_intensity.idxmax()
                nl2_intensity = nl2_intensity[max_nl2_index]
            else:
                nl2_intensity = ms2i_values[matched_input_data_NL2.index]
                nl_db_intensity = ms2i_values[matched_input_data_NLdb.index]
                max_nl2_index = nl2_intensity.idxmax()
                min_nl_db_index = (ms2mz_NLdb - input_data.loc[matched_input_data_NLdb.index, 'MS2mz']).abs().idxmin()
                nl2_intensity = nl2_intensity[max_nl2_index]
                nl_db_intensity = nl_db_intensity[min_nl_db_index]
            if (nl2_intensity > nl_db_intensity).all():
                input_data.loc[matched_input_data_NL2.index, 'double bond-NL-2'] = row['double bond position']
                input_data.loc[matched_input_data_NL2.index, 'NL-2 Matched'] = row['Precursor m/z-NL-2']
                input_data.loc[matched_input_data_NL4.index, 'NL-4 Matched'] = row['Precursor m/z-NL-4']
                input_data.loc[matched_input_data_NL6.index, 'NL-6 Matched'] = row['Precursor m/z-NL-6']
                input_data.loc[matched_input_data_NL8.index, 'NL-8 Matched'] = row['Precursor m/z-NL-8']
                input_data.loc[matched_input_data_NL10.index, 'NL-10 Matched'] = row['Precursor m/z-NL-10']
                input_data.loc[matched_input_data_NL12.index, 'NL-12 Matched'] = row['Precursor m/z-NL-12']
    columns_to_check = ['double bond-NL-2', 'NL-2 Matched', 'NL-4 Matched', 'NL-6 Matched', 'NL-8 Matched', 'NL-10 Matched', 'NL-12 Matched']
    for column in columns_to_check:
        if column in input_data.columns:
            input_data['double bond-NL-2'] = input_data['double bond-NL-2'].mask(input_data['double bond-NL-2'].duplicated(), '')
            input_data['NL-2 Matched'] = input_data['NL-2 Matched'].mask(input_data['NL-2 Matched'].duplicated(), '')
            input_data['NL-4 Matched'] = input_data['NL-4 Matched'].mask(input_data['NL-4 Matched'].duplicated(), '')
            input_data['NL-6 Matched'] = input_data['NL-6 Matched'].mask(input_data['NL-6 Matched'].duplicated(), '')
            input_data['NL-8 Matched'] = input_data['NL-8 Matched'].mask(input_data['NL-8 Matched'].duplicated(), '')
            input_data['NL-10 Matched'] = input_data['NL-10 Matched'].mask(input_data['NL-10 Matched'].duplicated(), '')
            input_data['NL-12 Matched'] = input_data['NL-12 Matched'].mask(input_data['NL-12 Matched'].duplicated(), '')
    return input_data
def process_excel(input_folder, index_file, output_folder):
    index_data = {}
    for i in range(1, 7):
        sheet_name = f'DU-{i}'
        index_data[sheet_name] = pd.read_excel(index_file, sheet_name=sheet_name)
    for filename in os.listdir(input_folder):
        if filename.endswith('.xlsx'):
            input_file = os.path.join(input_folder, filename)
            output_file = os.path.join(output_folder, filename)
            with pd.ExcelFile(input_file) as xls:
                with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
                    for sheet_name in xls.sheet_names:
                        input_data = pd.read_excel(input_file, sheet_name=sheet_name)
                        _3_column = 'sn_3_chain_f' in input_data.columns
                        _2_column = 'sn_2_chain_b' in input_data.columns
                        if 'sn_1_chain_d' not in input_data.columns:
                            input_data.to_excel(writer, index=False, sheet_name=sheet_name)
                            continue
                        processed_data = pd.DataFrame()
                        for i in range(1, 7):
                            if (i in input_data['sn_1_chain_d'].values) or (_2_column and i in input_data['sn_2_chain_b'].values) or (_3_column and i in input_data['sn_3_chain_f'].values):
                                if f'process_excel_DU{i}' in globals():
                                    process_func = globals()[f'process_excel_DU{i}']
                                    processed_data = pd.concat([processed_data, process_func(input_data.copy(), index_data[f'DU-{i}'])], ignore_index=True)
                        processed_data.to_excel(writer, index=False, sheet_name=sheet_name)
process_excel('output-step4', 'index-step5.xlsx', 'output-step5')
