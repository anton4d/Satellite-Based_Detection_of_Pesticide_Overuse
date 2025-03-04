import os
import rasterio
from datetime import datetime, timedelta
from rasterio.merge import merge

# Define the root folder containing the images
root_folder = "../Pictures"
output_folder = "../PicturesMerged/"

# Ensure the output directory exists
os.makedirs(output_folder, exist_ok=True)

# Specify the start and end date for processing
start_date = datetime.strptime("2024-08-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-08-10", "%Y-%m-%d")

# Step 1: Collect all available TIFF dates for each region
region_files = {}  # {region: [sorted list of available dates]}

for region_folder in os.listdir(root_folder):
    region_path = os.path.join(root_folder, region_folder)
    if os.path.isdir(region_path):
        available_dates = []
        for tiff_file in os.listdir(region_path):
            if tiff_file.endswith(".tiff"):
                try:
                    date_str = tiff_file[:-5]  # Extract date from filename
                    available_dates.append(datetime.strptime(date_str, "%Y-%m-%d"))
                except ValueError:
                    print(f"Skipping invalid file: {tiff_file}")

        # Store sorted list of available dates for this region
        if available_dates:
            region_files[region_folder] = sorted(available_dates)

# Step 2: Function to find the closest available date for a given region
def get_closest_date(region, target_date):
    """ Returns the closest available date for a given region. """
    available_dates = region_files.get(region, [])
    if not available_dates:
        return None  # No available dates for this region

    # Find the closest date by absolute difference
    closest_date = min(available_dates, key=lambda d: abs((d - target_date).days))
    return closest_date.strftime("%Y-%m-%d")

# Step 3: Iterate through each date in the specified range and generate merged TIFFs
current_date = start_date

while current_date <= end_date:
    current_date_str = current_date.strftime("%Y-%m-%d")
    print(f"\nProcessing merged TIFF for: {current_date_str}")

    tiff_files = []

    # Find the closest available file for each region
    for region in region_files.keys():
        closest_date_str = get_closest_date(region, current_date)
        if closest_date_str:
            file_path = os.path.join(root_folder, region, f"{closest_date_str}.tiff")
            if os.path.exists(file_path):
                tiff_files.append(file_path)

    if not tiff_files:
        print(f"❌ No valid TIFF files found for {current_date_str}, skipping...")
        current_date += timedelta(days=1)
        continue

    # Open and merge the TIFF files
    src_files_to_mosaic = [rasterio.open(fp) for fp in tiff_files]
    mosaic, out_transform = merge(src_files_to_mosaic)

    # Save the merged TIFF using a correct naming format
    output_file = os.path.join(output_folder, f"Merge_{current_date_str}.tiff")
    with rasterio.open(
        output_file,
        "w",
        driver="GTiff",
        height=mosaic.shape[1],
        width=mosaic.shape[2],
        count=src_files_to_mosaic[0].count,  # Number of bands
        dtype=src_files_to_mosaic[0].dtypes[0],
        crs=src_files_to_mosaic[0].crs,  # Preserve CRS
        transform=out_transform,
    ) as dest:
        dest.write(mosaic)

    # Close all open files
    for src in src_files_to_mosaic:
        src.close()

    print(f"✅ Saved merged TIFF: {output_file}")

    # Move to the next date
    current_date += timedelta(days=1)

print("\nAll TIFFs merged successfully!")