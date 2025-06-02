import mysql.connector
from dotenv import load_dotenv
import os




def update_random_fields_region_to_test(crop_id, year, db_config):
    """
    Finds 10 random fields with given crop_id and year,
    and updates their region to 'Test'.
    """
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        
        select_query = """
            SELECT f.FieldId
            FROM Field f
            JOIN FieldCropYear fcy ON f.FieldId = fcy.FieldId
            WHERE fcy.CropId = %s AND fcy.Year = %s
            ORDER BY RAND()
            LIMIT 10;
        """
        cursor.execute(select_query, (crop_id, year))
        results = cursor.fetchall()
        field_ids = [row['FieldId'] for row in results]

        if not field_ids:
            print("No fields found with the given crop_id and year.")
            return

        
        format_strings = ','.join(['%s'] * len(field_ids))
        update_query = f"""
            UPDATE Field
            SET Region = 'Test'
            WHERE FieldId IN ({format_strings});
        """
        cursor.execute(update_query, field_ids)
        conn.commit()

        print(f"Updated {cursor.rowcount} fields to region 'Test'.")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
   
    crop_id = int(input("Enter Crop ID: "))
    year = input("Enter Year (e.g., 2024): ")

    load_dotenv()
    db_config = {
        'host': os.getenv('SQLHOST'),
        'user': os.getenv('DBUSER'),
        'password': os.getenv('DBPASSWORD'),
        'database': os.getenv('DBDB'),
    }

    update_random_fields_region_to_test(crop_id, year, db_config)
