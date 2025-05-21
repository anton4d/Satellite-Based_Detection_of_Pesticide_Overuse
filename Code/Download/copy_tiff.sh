#!/bin/bash

# Define base folder
basefolder="Picturesres1024date20240801"
destination="$basefolder/all"

# Create destination folder if it doesn't exist
mkdir -p "$destination"

# Loop through subdirectories in basefolder
for dir in "$basefolder"/*/; do
    # Get folder name without path
    foldername=$(basename "$dir")

    # Find TIFF files in the folder
    for file in "$dir"/*.tiff; do
        # Check if file exists
        if [ -f "$file" ]; then
            # Get filename without path
            filename=$(basename "$file")
            
            # Copy with new name to destination
            cp "$file" "$destination/${foldername}_${filename}"
        fi
    done
done

echo "All TIFF files have been copied to $destination"
