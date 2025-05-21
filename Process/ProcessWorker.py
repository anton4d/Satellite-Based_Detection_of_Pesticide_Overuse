import time
import pika
import subprocess
import re
import sys
import logging
import argparse
import concurrent.futures
import os

DATEPATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}\|(True|False)\|(Plot|ToDb)\|(NDVI|EVI|GNDVI|CVI|NDRE|ALL)\|(Kdeplot|Histogram|HeatMap|None)\|\d{1,2}\|(\d{1,}|All)\|(BB|Field)$")
log_filename = "tiff_process.log"
folderName = "log/"
def setup_logging(log_filename):
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info(f"TIFF Worker started. Logging to {log_filename}")

def validate_message(message):
    return bool(DATEPATTERN.match(message))

def process_tiff(message, log_filename):
    logging.info(f"Starting TIFF process with message: {message}")
    process = subprocess.Popen(
        ["python3", "TiffProcessor.py", message, f"ProcessFor{log_filename}", folderName]
    )
    return process.wait()

def cracefullstop(channel, connection, executor):
    channel.stop_consuming()
    connection.close()
    executor.shutdown(wait=False)
    logging.info("Graceful shutdown.")

def commandsCallback(ch, method, properties, body, executor,connection):
    message = body.decode().strip()
    logging.info(f"Received message: {message}")
    if message == "Stop":
        cracefullstop(ch,connection,executor)


def callback(ch, method, properties, body, executor):
    message = body.decode().strip()
    logging.info(f"Received message: {message}")

    if not validate_message(message):
        logging.error(f"Invalid message format: {message}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    startTime = time.time()
    future = executor.submit(process_tiff, message, log_filename)

    def on_done(f):
        try:
            endtime = time.time()
            fullTime = endtime - startTime
            exit_code = f.result()
            if exit_code == 0:
                logging.info(f"TIFF process completed, ACK sent. it took {fullTime} seconds")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            elif exit_code ==1:
                logging.error(f"TIFF process failed (exit {exit_code}), not requeued.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                logging.error(f"TIFF process failed (exit {exit_code}), requeued.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            logging.error(f"Error in TIFF processing: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    future.add_done_callback(lambda f: ch.connection.add_callback_threadsafe(lambda: on_done(f)))

def consume():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=30))
    channel = connection.channel()

    channel.exchange_declare(exchange="Commands", exchange_type="fanout")

    result = channel.queue_declare(queue="", exclusive=True)

    queue_name = result.method.queue
    

    channel.queue_bind(exchange="Commands", queue=queue_name)

    channel.queue_declare(queue='Process_task_queue', durable=True)
    channel.basic_qos(prefetch_count=1)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    channel.basic_consume(queue='Process_task_queue', on_message_callback=lambda ch, method, properties, body: 
                          callback(ch, method, properties, body, executor))
    channel.basic_consume(queue=queue_name, on_message_callback=lambda ch, method, properties, body: 
                          commandsCallback(ch, method, properties, body, executor,connection))

    logging.info("TIFF RabbitMQ consumer started.")
    print(" [*] Waiting for messages. To exit press CTRL+C")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        cracefullstop(channel,connection,executor)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("LogFile", nargs="?", default="tiff_process.log", help="Log file name")
    parser.add_argument("LogFolder", nargs="?",default="Logs",help="The folder the log files should be saved to (default: Logs")
    args = parser.parse_args()
    global log_filename
    global folderName
    log_filename = f"{args.LogFile}.log"
    folderName = f"{args.LogFolder}/"
    logfile = folderName+log_filename
    os.makedirs(folderName, exist_ok=True)
    setup_logging(logfile)
    consume()

if __name__ == "__main__":
    main()
