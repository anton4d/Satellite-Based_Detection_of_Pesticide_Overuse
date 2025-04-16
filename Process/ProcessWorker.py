import pika
import subprocess
import re
import sys
import logging
import argparse
import concurrent.futures

DATEPATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}\|\d{1,2}\|\w*$")
log_filename = "tiff_process.log"

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
        ["python3", "TiffProcessor.py", message, f"ProcessFor{log_filename}"]
    )
    return process.wait()

def callback(ch, method, properties, body, executor):
    message = body.decode().strip()
    logging.info(f"Received message: {message}")

    if not validate_message(message):
        logging.error(f"Invalid message format: {message}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    future = executor.submit(process_tiff, message, log_filename)

    def on_done(f):
        try:
            exit_code = f.result()
            if exit_code == 0:
                logging.info(f"TIFF process completed, ACK sent.")
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
    channel.queue_declare(queue='Process_task_queue', durable=True)
    channel.basic_qos(prefetch_count=1)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    channel.basic_consume(queue='Process_task_queue', on_message_callback=lambda ch, method, properties, body: callback(ch, method, properties, body, executor))

    logging.info("TIFF RabbitMQ consumer started.")
    print(" [*] Waiting for messages. To exit press CTRL+C")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()
        executor.shutdown(wait=False)
        logging.info("Graceful shutdown.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("LogFile", nargs="?", default="tiff_process.log", help="Log file name")
    args = parser.parse_args()
    global log_filename
    log_filename = f"{args.LogFile}.log"
    setup_logging(log_filename)
    consume()

if __name__ == "__main__":
    main()
