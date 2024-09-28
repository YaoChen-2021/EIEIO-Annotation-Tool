# DICP
# CY
# 2024/5/13 20:11

import os
import subprocess
programs = [{"program": "step1.py", "input_folder": "input-mzml", "output_folder": "output-step1"},
            {"program": "step2.py", "input_folder": "output-step1", "output_folder": "output-step2"},
            {"program": "step3.py", "input_folder": "output-step2", "output_folder": "output-step3"},
            {"program": "step4.py", "input_folder": "output-step3", "output_folder": "output-step4"},
            {"program": "step5.py", "input_folder": "output-step4", "output_folder": "output-step5"},
            {"program": "step6.py", "input_folder": "output-step5", "output_folder": "output-step6"}]
for prog in programs:
    if not os.path.exists(prog["output_folder"]):
        os.makedirs(prog["output_folder"])
    command = ["python", prog["program"], prog["input_folder"], prog["output_folder"]]
    subprocess.run(command)
print("Annotation Over")
