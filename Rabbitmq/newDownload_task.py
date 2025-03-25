import pika,sys,argparse
import sys
from datetime import datetime, timedelta

def split_date_range(start_date, end_date, workers, max_days=10):
    """First splits the date range into worker-sized chunks, then further splits each into max 10-day chunks."""
    total_days = (end_date - start_date).days
    worker_chunk_size = total_days // workers
    worker_chunks = []
    
    current_date = start_date
    for i in range(workers):
        next_date = current_date + timedelta(days=worker_chunk_size)
        if i == workers - 1:
            next_date = end_date
        worker_chunks.append((current_date, next_date))
        current_date = next_date
    
    final_chunks = []
    for worker_start, worker_end in worker_chunks:
        current = worker_start
        while current < worker_end:
            next_chunk = min(current + timedelta(days=max_days), worker_end)
            final_chunks.append((current, next_chunk))
            current = next_chunk
    
    return final_chunks

def distribute_tasks(start_date, end_date, workers, mode, region, resolution):
    """Distributes the tasks among workers while ensuring max 10-day chunks."""
    date_chunks = split_date_range(start_date, end_date, workers)
    messages = [f"{chunk[0].isoformat()}Z|{chunk[1].isoformat()}Z|{mode}|{region}|{resolution}" for chunk in date_chunks]
    
    return messages

def send_messages(messages):
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
    parser.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format")
    parser.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument("workers", type=int, help="Number of workers")
    parser.add_argument("mode", type=str, help="Processing mode")
    parser.add_argument("region", type=str, help="Region name")
    parser.add_argument("resolution", type=int, help="Resolution value")
    
    args = parser.parse_args()
    
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1)  # Include last day
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)
    
    if args.workers < 1:
        print("Error: Number of workers must be at least 1.")
        sys.exit(1)
    
    messages = distribute_tasks(start_date, end_date, args.workers, args.mode, args.region, args.resolution)
    send_messages(messages)
