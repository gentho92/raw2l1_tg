import os
import glob
import numpy as np
from netCDF4 import Dataset
from datetime import datetime, timedelta

# Define the input and output directories
input_dir = "/home/thomasfgklima/Dokumente/urbisphere/processing/ACTRIS_raw2l1/data/ceilometer"
output_dir = "/home/thomasfgklima/Dokumente/urbisphere/processing/ACTRIS_raw2l1/data/ceilometer"

# Define the location, instrument type, and the date range to process
location = "99001"
instrument = "CHM160145"
start_date = datetime(2022, 5, 24)
end_date = datetime(2022, 5, 31)

# Global attributes (template)
global_attrs_template = {
    "title": "CHM15k Nimbus",
    "source": "CHM160145",
    "device_name": "CHM160145",
    "serlom": "TUB160044",
    "day": None,  # Placeholder, set dynamically per file
    "month": None,  # Placeholder, set dynamically per file
    "year": None,  # Placeholder, set dynamically per file
    "location": "99001",
    "institution": "Technische Universität Berlin",
    "wmo_id": 0,
    "software_version": "15.12.1 2.13 1.040 0",
    "comment": "",
    "overlap_file": "TUB160044 (2016-07-22 08:52:58)",
}

# Create a list of dates to process
dates_to_process = [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range((end_date - start_date).days + 1)]

# Process each date
for date_str in dates_to_process:
    year = int(date_str[:4])
    month = int(date_str[4:6])
    day = int(date_str[6:])
    day_of_year = (datetime(year, month, day) - datetime(year, 1, 1)).days + 1

    # Find the folder corresponding to the day of the year
    day_folder = os.path.join(input_dir, f"{day_of_year:03}")

    if not os.path.exists(day_folder):
        print(f"Folder for day {day_of_year:03} does not exist, skipping.")
        continue

    # Find all NetCDF files for this day
    nc_files = sorted(glob.glob(os.path.join(day_folder, f"{location}_A{date_str}*.nc")))

    if not nc_files:
        print(f"No files found for date {date_str}, skipping.")
        continue

    # Open the first file to create the structure of the output file
    with Dataset(nc_files[0], "r") as first_file:
        # Create the output file
        output_file = os.path.join(output_dir, f"{date_str}_{location}_{instrument}_000.nc")
        with Dataset(output_file, "w", format="NETCDF4") as output_nc:
            # Copy dimensions
            for name, dimension in first_file.dimensions.items():
                output_nc.createDimension(name, (len(dimension) if not dimension.isunlimited() else None))

            # Copy variables
            for name, variable in first_file.variables.items():
                out_var = output_nc.createVariable(
                    name,
                    variable.datatype,
                    variable.dimensions,
                    fill_value=None,  # No explicit fill value for now
                )
                # Copy attributes, excluding `_ChunkSizes`
                out_var.setncatts({k: variable.getncattr(k) for k in variable.ncattrs() if k != "_ChunkSizes"})
                if name != "time":  # Don't copy data for 'time' yet
                    out_var[:] = variable[:]

            # Prepare to concatenate 'time' and other time-dependent variables
            time_data = []
            variable_data = {var: [] for var in first_file.variables if "time" in first_file.variables[var].dimensions}

            # Loop through files to concatenate
            for file in nc_files:
                with Dataset(file, "r") as nc:
                    time_data.extend(nc.variables["time"][:])
                    for var in variable_data:
                        variable_data[var].append(nc.variables[var][:])

            # Write concatenated data to the output file
            output_nc.variables["time"][:] = time_data
            for var, data in variable_data.items():
                # Flatten and concatenate variable data
                concatenated_data = np.concatenate(data)
                output_nc.variables[var][:] = concatenated_data

            # Set global attributes
            global_attrs = global_attrs_template.copy()
            global_attrs["day"] = day
            global_attrs["month"] = month
            global_attrs["year"] = year
            for attr_name, attr_value in global_attrs.items():
                if isinstance(attr_value, int):  # Ensure int values are stored as int
                    output_nc.setncattr(attr_name, np.int32(attr_value))
                else:
                    output_nc.setncattr(attr_name, attr_value)

            print(f"Saved combined file for {date_str} to {output_file}")

print("Processing complete.")
