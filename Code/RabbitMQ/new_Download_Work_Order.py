import pika,sys,argparse
import sys
from datetime import datetime, timedelta

def Split_Date_Range(Start_Date, End_Date, Workers, Max_days=10):
    """First splits the date range into worker-sized chunks, then further splits each into max 10-day chunks."""
    Total_Days = (End_Date - Start_Date).days
    Worker_Chunk_Size = Total_Days // Workers
    Worker_Chunks = []
    
    Current_Date = Start_Date
    for i in range(Workers):
        Next_Date = Current_Date + timedelta(days=Worker_Chunk_Size)
        if i == Workers - 1:
            Next_Date = End_Date
        Worker_Chunks.append((Current_Date, Next_Date))
        Current_Date = Next_Date
    
    Final_Chunks = []
    for worker_start, worker_end in Worker_Chunks:
        current = worker_start
        while current < worker_end:
            next_chunk = min(current + timedelta(days=Max_days), worker_end)
            Final_Chunks.append((current, next_chunk))
            current = next_chunk
    
    return Final_Chunks

def Distribute_Tasks(Work_Order_ID,Start_Date, End_Date, Workers, Mode, Region, Resolution,Crop_ID,Field_ID):
    """Distributes the tasks among workers while ensuring max 10-day chunks."""
    Date_Chunks = Split_Date_Range(Start_Date, End_Date, Workers)
    Messages = [f"{Work_Order_ID}|{Chunk[0].isoformat()}Z|{Chunk[1].isoformat()}Z|{Mode}|{Region}|{Resolution}|{Crop_ID}|{Field_ID}" for Chunk in Date_Chunks]
    
    return Messages

def Send_Messages(messages):
    """Sends messages to the RabbitMQ queue."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='Download_task_queue', durable=True)
    
    for message in messages:
        channel.basic_publish(
            exchange='',
            routing_key='Download_task_queue',
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
        )
        print(f"[x] Sent {message}")
    
    connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RabbitMQ worker for processing downloads.")
    parser.add_argument("Work_Order_ID",type=str,help="The name for the work order")
    parser.add_argument("Start_Date", type=str, help="Start date in YYYY-MM-DD format")
    parser.add_argument("End_Date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument("Workers", type=int, help="Number of workers")
    parser.add_argument("Mode", type=str,choices=["BB","Field"], help="Processing mode")
    parser.add_argument("Region", type=str,choices=["all", "Fyn","Test"], help="Region name")
    parser.add_argument("Resolution", type=int, help="Resolution value")
    parser.add_argument("Crop_Type_ID", help="either give a crop ID or all")
    parser.add_argument("Field_ID", help="either give a Field ID or all")
    
    args = parser.parse_args()
    
    try:
        Start_Date = datetime.strptime(args.Start_Date, "%Y-%m-%d")
        End_Date = datetime.strptime(args.End_Date, "%Y-%m-%d")
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)
    
    if args.Workers < 1:
        print("Error: Number of workers must be at least 1.")
        sys.exit(1)
    
    messages = Distribute_Tasks(args.Work_Order_ID,Start_Date, End_Date, args.Workers, args.Mode, args.Region, args.Resolution,args.Crop_Type_ID,args.Field_ID)
    Send_Messages(messages)
