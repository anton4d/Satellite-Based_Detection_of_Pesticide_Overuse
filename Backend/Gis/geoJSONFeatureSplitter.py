import json
import os

# Function to load the original GeoJSON file
def load_geojson(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

# Function to save each feature as a new GeoJSON file
def save_split_files(features, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_paths = []
    for i, feature in enumerate(features):
        # Create a new GeoJSON structure for each feature
        new_geojson = {
            "type": "FeatureCollection",
            "features": [feature]
        }
        # Define the file path for the new file
        file_path = os.path.join(output_dir, f"feature_{i+1}.geojson")
        with open(file_path, 'w') as f:
            json.dump(new_geojson, f)
        file_paths.append(file_path)
    
    return file_paths

# Main function to execute the splitting process
def split_geojson(input_file, output_dir):

    geojson_data = load_geojson(input_file)
    features = geojson_data.get('features', [])
    split_files = save_split_files(features, output_dir)

    return split_files


if __name__ == "__main__":
    input_file = '../../Shapefiles/Marker_2020.geojson'
    output_dir = '../../Shapefiles/individualgeoJSON'
    
    split_files = split_geojson(input_file, output_dir)
    
    logging.info(f"Split files saved to: {split_files}")
