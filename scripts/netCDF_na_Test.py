from netCDF4 import Dataset
import numpy as np

# Define the path to the original NetCDF file
file_path = "/home/thomasfgklima/Dokumente/urbisphere/processing/ACTRIS_raw2l1/data/ceilometer/20220522_99001_CHM160145_000.nc"  # Replace with the actual file path

# Open the NetCDF file
with Dataset(file_path, "r") as nc_file:
    print(f"Inspecting file: {file_path}\n")
    for var_name, variable in nc_file.variables.items():
        print(f"Variable: {var_name}")
        
        # Check for _FillValue
        fill_value = variable._FillValue if hasattr(variable, "_FillValue") else None
        if fill_value is not None:
            print(f"  _FillValue: {fill_value}")
        else:
            print("  No _FillValue attribute found.")
        
        # Check for other missing value attributes
        missing_value = variable.missing_value if hasattr(variable, "missing_value") else None
        if missing_value is not None:
            print(f"  missing_value: {missing_value}")
        
        # Check the actual data for missing values
        try:
            data = variable[:]
            if hasattr(data, "mask"):  # For masked arrays
                print(f"  Data is masked with {np.sum(data.mask)} missing values.")
                # Print first few missing data points if available
                masked_indices = np.where(data.mask)
                print(f"  Example missing indices: {masked_indices[:5]}")
            elif (data == fill_value).any():
                print(f"  Data contains {np.sum(data == fill_value)} instances of _FillValue.")
            elif (data != data).any():  # Check for NaNs
                print(f"  Data contains {np.sum(data != data)} NaN values.")
            else:
                print("  No missing data detected in the variable.")
        except Exception as e:
            print(f"  Unable to inspect data: {e}")
        
        print("-" * 40)