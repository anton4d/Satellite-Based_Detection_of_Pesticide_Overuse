import os
import rasterio
from datetime import datetime, timedelta
from rasterio.merge import merge


root_folder = "../Pictures"
output_folder = "../PicturesMerged/"


os.makedirs(output_folder, exist_ok=True)

start_date = datetime.strptime("2024-08-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-08-10", "%Y-%m-%d")

region_files = {}  
for region_folder in os.listdir(root_folder):
    region_path = os.path.join(root_folder, region_folder)
    if os.path.isdir(region_path):
        available_dates = []
        for tiff_file in os.listdir(region_path):
            if tiff_file.endswith(".tiff"):
                try:
                    date_str = tiff_file[:-5]  
                    available_dates.append(datetime.strptime(date_str, "%Y-%m-%d"))
                except ValueError:
                    print(f"Skipping invalid file: {tiff_file}")

        if available_dates:
            region_files[region_folder] = sorted(available_dates)

def get_closest_date(region, target_date):
    """ Returns the closest available date for a given region. """
    available_dates = region_files.get(region, [])
    if not available_dates:
        return None  

    closest_date = min(available_dates, key=lambda d: abs((d - target_date).days))
    return closest_date.strftime("%Y-%m-%d")


current_date = start_date

while current_date <= end_date:
    current_date_str = current_date.strftime("%Y-%m-%d")
    print(f"\nProcessing merged TIFF for: {current_date_str}")

    tiff_files = []


    for region in region_files.keys():
        closest_date_str = get_closest_date(region, current_date)
        if closest_date_str:
            file_path = os.path.join(root_folder, region, f"{closest_date_str}.tiff")
            if os.path.exists(file_path):
                tiff_files.append(file_path)

    if not tiff_files:
        print(f"No valid TIFF files found for {current_date_str}, skipping...")
        current_date += timedelta(days=1)
        continue


    src_files_to_mosaic = [rasterio.open(fp) for fp in tiff_files]
    mosaic, out_transform = merge(src_files_to_mosaic)


    output_file = os.path.join(output_folder, f"Merge_{current_date_str}.tiff")
    with rasterio.open(
        output_file,
        "w",
        driver="GTiff",
        height=mosaic.shape[1],
        width=mosaic.shape[2],
        count=src_files_to_mosaic[0].count, 
        dtype=src_files_to_mosaic[0].dtypes[0],
        crs=src_files_to_mosaic[0].crs, 
        transform=out_transform,
    ) as dest:
        dest.write(mosaic)


    for src in src_files_to_mosaic:
        src.close()

    print(f"Saved merged TIFF: {output_file}")


    current_date += timedelta(days=1)

print("\nAll TIFFs merged successfully!")