import pika, sys, argparse
from datetime import datetime, timedelta

def split_date_range(start_date, end_date, workers, max_days=10):
    """Split total range into chunks for workers and further into max 10-day segments."""
    total_days = (end_date - start_date).days
    worker_chunk_size = total_days // workers
    worker_chunks = []

    current_date = start_date
    for i in range(workers):
        next_date = current_date + timedelta(days=worker_chunk_size)
        if i == workers - 1:
            next_date = end_date
        worker_chunks.append((current_date, next_date))
        current_date = next_date + timedelta(days=1)

    final_chunks = []
    for worker_start, worker_end in worker_chunks:
        current = worker_start
        while current < worker_end:
            next_chunk = min(current + timedelta(days=max_days), worker_end)
            final_chunks.append((current, next_chunk))
            current = next_chunk + timedelta(days=1)

    return final_chunks

def distribute_tasks(start_date, end_date,CropId,SaveTiff, workers):
    chunks = split_date_range(start_date, end_date, workers)
    messages = [f"{chunk[0].strftime('%Y-%m-%d')}|{chunk[1].strftime('%Y-%m-%d')}|{CropId}|{SaveTiff}" for chunk in chunks]
    return messages

def send_messages(messages):
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
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("workers", type=int, help="Number of workers")
    parser.add_argument("CropId",type=int, help="The cropid you want to process")
    parser.add_argument("SaveTiff",choices=["True","False"], help="If you want save the tiff")

    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        cropid = args.CropId
        saveTiff = args.SaveTiff
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    if args.workers < 1:
        print("Error: Workers must be at least 1.")
        sys.exit(1)

    messages = distribute_tasks(start_date, end_date, cropid,saveTiff, args.workers)
    send_messages(messages)
