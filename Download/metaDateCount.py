import pandas as pd

# Load your CSV file
df = pd.read_csv("DateMetaData.csv")

# Keep only relevant columns
df = df[['ID', 'DateOnly', 'Platform',"ClouCover"]]

# Check for duplicate (Id, DateOnly) with different Platforms
duplicate_checkpl = df.groupby(['ID', 'DateOnly'])['Platform'].nunique()

# Filter cases where more than one unique platform exists
duplicates = duplicate_checkpl[duplicate_checkpl > 1]

# Display results
if not duplicates.empty:
    print("Multiple platforms recorded data for the same Id and Date:")
    print(duplicates)
else:
    print("No conflicts found.")

duplicate_checkCl = df.groupby(["ID","DateOnly"])["ClouCover"].nunique()
# Filter cases where more than one unique platform exists
duplicates = duplicate_checkCl[duplicate_checkCl > 1]

# Display results
if not duplicates.empty:
    print("Multiple CloudCover for the same Id and Date:")
    print(duplicates)
else:
    print("No conflicts found.")
