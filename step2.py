import numpy as np


def step2(ms2_dataset: dict):
    for ms2_list in ms2_dataset.values():
        for ms2_spectrum in ms2_list:
            MS2mz_values = ms2_spectrum.mz_array  # np.array

            has_184 = np.any(np.abs(MS2mz_values - 184.073) <= 0.5)
            has_224_226 = np.any(np.abs(MS2mz_values - 224.107) <= 0.5) or np.any(np.abs(MS2mz_values - 226.085) <= 0.5)
            has_142 = np.any(np.abs(MS2mz_values - 142.026) <= 0.5)
            has_182 = np.any(np.abs(MS2mz_values - 182.058) <= 0.5)
            has_225 = np.any(np.abs(MS2mz_values - 225.100) <= 0.5)
            has_253 = np.any(np.abs(MS2mz_values - 253.085) <= 0.5)

            if has_184 and has_224_226 and not has_142:
                ms2_spectrum.lipid_class.append('PC')
            elif has_184 and has_224_226 and has_142 and has_182:
                ms2_spectrum.lipid_class.append('PC')
                ms2_spectrum.lipid_class.append('PE')
            elif has_184 and (has_225 or has_253):
                ms2_spectrum.lipid_class.append('SM')
            elif has_142 and has_182:
                ms2_spectrum.lipid_class.append('PE')
                # 没有被判定为任何类别的就是Others吗？


if __name__ == '__main__':
    pass
