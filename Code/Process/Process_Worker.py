import time
import pika
import subprocess
import re
import sys
import logging
import argparse
import concurrent.futures
import os

class TiffWorker:
    DATEPATTERN = re.compile(
        r"^\w{1,}\|\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}\|(True|False)\|(Plot|ToDb)\|"
        r"(NDVI|EVI|GNDVI|CVI|NDRE|ALL)\|(Kdeplot|Histogram|HeatMap|None)\|\d{1,2}\|(\d{1,}|All)\|(BB|Field)$"
    )

    def __init__(self, log_filename="tiff_process.log", folder_name="Workerlogs"):
        self.log_filename = f"{log_filename}.log"
        self.folder_name = f"Logs/{folder_name}/"
        self.Work_Order_ID = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        os.makedirs(self.folder_name, exist_ok=True)
        self.setup_logging()

    def setup_logging(self):
        logfile = self.folder_name + self.log_filename
        logging.basicConfig(
            filename=logfile,
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        logging.info(f"TIFF Worker started. Logging to {logfile}")

    def validate_message(self, message):
        return bool(self.DATEPATTERN.match(message))

    def Extract_Work_Order_ID(self, message):
        self.Work_Order_ID = message.split('|', 1)[0]

    def process_tiff(self, message):
        logging.info(f"Starting TIFF process with message: {message}")
        self.Extract_Work_Order_ID(message)
        process = subprocess.Popen(
            ["python3", "TIFF_Processor.py", message, f"{self.log_filename}_{self.Work_Order_ID}", self.folder_name]
        )
        return process.wait()

    def graceful_shutdown(self, channel, connection):
        channel.stop_consuming()
        connection.close()
        self.executor.shutdown(wait=False)
        logging.info("Graceful shutdown.")

    def handle_command(self, ch, method, properties, body, connection):
        message = body.decode().strip()
        logging.info(f"Received command message: {message}")
        if message == "Stop":
            self.graceful_shutdown(ch, connection)

    def handle_message(self, ch, method, properties, body):
        message = body.decode().strip()
        logging.info(f"Received task message: {message}")

        if not self.validate_message(message):
            logging.error(f"Invalid message format: {message}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        start_time = time.time()
        future = self.executor.submit(self.process_tiff, message)

        def on_done(f):
            try:
                full_time = time.time() - start_time
                exit_code = f.result()
                if exit_code == 0:
                    logging.info(f"TIFF process completed in {full_time:.2f} seconds, ACK sent.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                elif exit_code == 1:
                    logging.error(f"TIFF process failed (exit {exit_code}), not requeued.")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                else:
                    logging.error(f"TIFF process failed (exit {exit_code}), requeued.")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception as e:
                logging.error(f"Error in TIFF processing: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        future.add_done_callback(lambda f: ch.connection.add_callback_threadsafe(lambda: on_done(f)))

    def consume(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=30))
        channel = connection.channel()


        channel.exchange_declare(exchange="Commands", exchange_type="fanout")
        result = channel.queue_declare(queue="", exclusive=True)
        command_queue = result.method.queue
        channel.queue_bind(exchange="Commands", queue=command_queue)

        channel.queue_declare(queue='Process_task_queue', durable=True)
        channel.basic_qos(prefetch_count=1)


        channel.basic_consume(
            queue='Process_task_queue',
            on_message_callback=lambda ch, method, props, body:
                self.handle_message(ch, method, props, body)
        )
        channel.basic_consume(
            queue=command_queue,
            on_message_callback=lambda ch, method, props, body:
                self.handle_command(ch, method, props, body, connection)
        )

        logging.info("TIFF RabbitMQ consumer started.")
        print(" [*] Waiting for messages. To exit press CTRL+C")

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.graceful_shutdown(channel, connection)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("LogFile", nargs="?", default="tiff_process", help="Log file name (without extension)")
    parser.add_argument("LogFolder", nargs="?", default="Logs", help="Folder where log files will be saved")
    args = parser.parse_args()

    worker = TiffWorker(log_filename=args.LogFile, folder_name=args.LogFolder)
    worker.consume()

if __name__ == "__main__":
    main()
