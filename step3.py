import os
import pandas as pd
import numpy as np


def step3(ms2_dataset: dict, index_file='index-step3.xlsx'):
    index_data_uncertain = pd.read_excel(index_file, sheet_name='Uncertain')
    index_data_other = pd.read_excel(index_file, sheet_name='Others')

    for ms2_list in ms2_dataset.values():
        for ms2_spectrum in ms2_list:
            mz = ms2_spectrum.precursor_mz
            if not ms2_spectrum.lipid_class:
                # Uncertain
                theo_mz_array = index_data_uncertain['TheoMz'].to_numpy()
                diff_array = np.abs(theo_mz_array - mz)
                min_diff = diff_array.min()

                if min_diff <= 0.01:
                    matched_row = index_data_uncertain.iloc[np.where(diff_array == min_diff)].iloc[0]
                    ms2_spectrum.lipid_class.append(matched_row['MAIN_CLASS'])
                    ms2_spectrum.sub_class.append(matched_row['Subclass'])
                    ms2_spectrum.lipid_id.append(matched_row['ID'])
                    ms2_spectrum.lipid_formula.append(matched_row['Formula'])
                    ms2_spectrum.lipid_mass.append(matched_row['Mass'])
                    ms2_spectrum.adduct.append(matched_row['ADDUCT_TYPE'])
                    ms2_spectrum.c_number.append(matched_row['c_number'])
                    ms2_spectrum.db_number.append(matched_row['double_bond'])
            else:
                # Other classes
                cand_classes = ms2_spectrum.lipid_class.copy()
                ms2_spectrum.lipid_class = []
                for target_class in cand_classes:
                    filtered_data = index_data_other[index_data_other['MAIN_CLASS'] == target_class]

                    theo_mz_array = filtered_data['TheoMz'].to_numpy()
                    diff_array = np.abs(theo_mz_array - mz)
                    min_diff = diff_array.min()

                    if min_diff <= 0.01:
                        matched_rows = filtered_data.iloc[np.where(diff_array == min_diff)]
                        for _, row in matched_rows.iterrows():
                            ms2_spectrum.lipid_class.append(row['MAIN_CLASS'])
                            ms2_spectrum.sub_class.append(row['Subclass'])
                            ms2_spectrum.lipid_id.append(row['ID'])
                            ms2_spectrum.lipid_formula.append(row['Formula'])
                            ms2_spectrum.lipid_mass.append(row['Mass'])
                            ms2_spectrum.adduct.append(row['ADDUCT_TYPE'])
                            ms2_spectrum.c_number.append(row['c_number'])
                            ms2_spectrum.db_number.append(row['double_bond'])
