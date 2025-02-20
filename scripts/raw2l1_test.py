import os
import subprocess

# Define paths as variables
python_executable = r"C:/Users/getho/Documents/VisualStudioCodeProjects/github_raw2l1/.conda/python.exe"
raw2l1_script = r"C:/Users/getho/Documents/VisualStudioCodeProjects/github_raw2l1/raw2l1/raw2l1.py"
conf_file = r"C:/Users/getho/Documents/VisualStudioCodeProjects/github_raw2l1/raw2l1/conf/conf_lufft_chm15k_eprofile_TUCC.ini"
data_file = r"/home/thomasfgklima/Dokumente/github_raw2l1/data/20220804_99001_CHM160145_000.nc"  
output_file = r"C:/Users/getho/Documents/VisualStudioCodeProjects/github_raw2l1/output/test_lufft_tucc_yesterday.nc"

# Build the command
command = [
    python_executable,
    raw2l1_script,
    "20220804", 
    conf_file,
    data_file,
    output_file,
]

# Run the command
try:
    subprocess.run(command, check=True)
    print("raw2l1 script executed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error while running raw2l1 script: {e}")
