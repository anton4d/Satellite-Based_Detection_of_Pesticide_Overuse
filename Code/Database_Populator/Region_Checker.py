import mysql.connector
from shapely.geometry import shape, Point, Polygon
from shapely import wkb
import os, dotenv


Fyn_Polygon_Geojson = {
    "type": "Polygon",
    "coordinates": [
        [
            [9.692, 54.756],
            [10.972000000000005, 54.756],
            [10.972000000000005, 55.78],
            [9.692, 55.78],
            [9.692, 54.756]
        ]
    ]
}
fyn_polygon = shape(Fyn_Polygon_Geojson)

dotenv_path = dotenv.find_dotenv()
if dotenv_path:
    dotenv.load_dotenv(dotenv_path)
    print(f"Loaded environment variables from {dotenv_path}")
else:
    print("No .env file found.")
    

connection = mysql.connector.connect(
    host=os.getenv("SQLHOST"),
    user=os.getenv("DBUSER"),
    password=os.getenv("DBPASSWORD"),
    database=os.getenv("DBDB")
)

cursor = connection.cursor()


cursor.execute("SELECT FieldId, ST_AsBinary(Polygon) FROM Field")
rows = cursor.fetchall()

for field_id, polygon_wkb in rows:
    try:
        field_polygon = wkb.loads(polygon_wkb)
    except Exception as e:
        print(f"Error decoding WKB for FieldId {field_id}: {e}")
        continue

    
    found_inside = any(Point(coord).within(fyn_polygon) for coord in field_polygon.exterior.coords)

    if found_inside:
        
        cursor.execute("UPDATE Field SET Region = %s WHERE FieldId = %s", ('Fyn', field_id))
        print(f"Updated FieldId {field_id} with Region = 'Fyn'")


connection.commit()
cursor.close()
connection.close()
