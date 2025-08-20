import os
from pyopenms import MzMLFile, MSExperiment
import numpy as np
from dataclasses import dataclass


@dataclass
class MS2Spectrum:
    def __init__(self, precursor_mz, precursor_rt, mz_array, intensity_array):
        self.precursor_mz = precursor_mz
        self.precursor_rt = precursor_rt
        self.mz_array = mz_array
        self.intensity_array = intensity_array
        self.lipid_class = []
        self.sub_class = []
        self.lipid_id = []
        self.lipid_formula = []
        self.lipid_mass = []
        self.adduct = []
        self.c_number = []
        self.db_number = []

    def __repr__(self):
        return f"MS2Spectrum(mz={self.precursor_mz}, rt={self.precursor_rt}, peaks={len(self.mz_array)})"


def step1(input_folder: str):
    dataset = {}  # key: filename, value: list of MS2Spectrum

    for file in os.listdir(input_folder):
        if file.endswith(".mzML"):
            file_path = os.path.join(input_folder, file)
            experiment = MSExperiment()
            MzMLFile().load(file_path, experiment)

            ms2_spectra = extract_ms2_spectra(experiment)
            dataset[file] = ms2_spectra

            print(f"Loaded {len(ms2_spectra)} MS2 spectra from {file}")
    return dataset


def extract_ms2_spectra(experiment: MSExperiment) -> list[MS2Spectrum]:
    ms2_list = []
    for scan in experiment:
        if scan.getMSLevel() == 2:
            precursor = scan.getPrecursors()[0] if scan.getPrecursors() else None
            if precursor:
                precursor_mz = np.float32(precursor.getMZ())
                precursor_rt = np.float32(scan.getRT())
                mzs, intensities = scan.get_peaks()
                # Filter and convert to float32 arrays
                mzs = np.asarray(mzs, dtype=np.float32)
                intensities = np.asarray(intensities, dtype=np.float32)

                mask = intensities > 0
                mzs_filtered = mzs[mask]
                intensities_filtered = intensities[mask]

                ms2 = MS2Spectrum(precursor_mz, precursor_rt, mzs_filtered, intensities_filtered)
                ms2_list.append(ms2)
    return ms2_list



if __name__ == "__main__":
    input_folder = "input-mzml"
    dataset = step1(input_folder)
