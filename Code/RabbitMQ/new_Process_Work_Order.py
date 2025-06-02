import pika, sys, argparse
from datetime import datetime, timedelta

def Split_Date_Range(Start_Date, End_Date, Workers, Max_Days=10):
    """Split total range into chunks for workers and further into max 10-day segments."""
    Total_Days = (End_Date - Start_Date).days
    Worker_Chunk_Size = Total_Days // Workers
    Worker_Chunks = []

    Current_Date = Start_Date
    for i in range(Workers):
        Next_Date = Current_Date + timedelta(days=Worker_Chunk_Size)
        if i == Workers - 1:
            Next_Date = End_Date
        Worker_Chunks.append((Current_Date, Next_Date))
        Current_Date = Next_Date + timedelta(days=1)

    Final_Chunks = []
    for worker_start, worker_end in Worker_Chunks:
        current = worker_start
        while current < worker_end:
            next_chunk = min(current + timedelta(days=Max_Days), worker_end)
            Final_Chunks.append((current, next_chunk))
            current = next_chunk + timedelta(days=1)

    return Final_Chunks

def Distribute_Tasks(Work_Order_ID,Start_Date, End_Date, Crop_ID,Save_TIFF, Workers,Data,Field_ID,Data_Mode,Plot_Type,region,TIFF_mode):
    Chunks = Split_Date_Range(Start_Date, End_Date, Workers)
    Messages = [f"{Work_Order_ID}|{Chunk[0].strftime('%Y-%m-%d')}|{Chunk[1].strftime('%Y-%m-%d')}|{Save_TIFF}|{Data_Mode}|{Data}|{Plot_Type}|{Crop_ID}|{Field_ID}|{region}|{TIFF_mode}" for Chunk in Chunks]
    return Messages

def Send_Messages(messages):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='Process_task_queue', durable=True)

    for message in messages:
        channel.basic_publish(
            exchange='',
            routing_key='Process_task_queue',
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
        )
        print(f"[x] Sent {message}")

    connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distribute TIFF processing tasks.")
    parser.add_argument("Work_Order_ID",type=str,help="The name for the work order")
    parser.add_argument("Start_Date",type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("End_Date",type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("Workers", type=int, help="Number of workers")
    parser.add_argument("TIFF_Mode",choices=["BB","Field"])
    parser.add_argument("Save_TIFF",choices=["True","False"], help="If you want save the tiff")
    parser.add_argument("Data_Mode",choices=["Plot","ToDb"],help="The dataMode of the proce")
    parser.add_argument("Data",choices=["NDVI","EVI","GNDVI","CVI","NDRE","ALL"],help="The data to be processed")
    parser.add_argument("Plot_Type",choices=["Kdeplot","Histogram","HeatMap","None"])
    parser.add_argument("Crop_ID",type=int, help="The cropid you want to process")
    parser.add_argument("Field_ID",default="All", help="A field id default is All")
    parser.add_argument("Region", type=str,choices=["all", "Fyn","Test"], help="Region name")
    

    args = parser.parse_args()

    try:
        Work_Order_ID = args.Work_Order_ID
        Start_Date = datetime.strptime(args.Start_Date, "%Y-%m-%d")
        End_Date = datetime.strptime(args.End_Date, "%Y-%m-%d")
        Crop_ID = args.Crop_ID
        Save_TIFF = args.Save_TIFF
        Workers = args.Workers
        Data = args.Data
        Field_ID = args.Field_ID
        Data_Mode= args.Data_Mode
        Plot_Type= args.Plot_Type
        region = args.Region
        TIFF_Mode = args.TIFF_Mode
        
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    if Workers < 1:
        print("Error: Workers must be at least 1.")
        sys.exit(1)

    Messages = Distribute_Tasks(Work_Order_ID,Start_Date, End_Date, Crop_ID,Save_TIFF, Workers,Data,Field_ID,Data_Mode,Plot_Type,region,TIFF_Mode)
    Send_Messages(Messages)
