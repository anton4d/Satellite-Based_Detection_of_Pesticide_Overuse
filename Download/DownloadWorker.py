import time,os,pika,subprocess,re,sys,logging,argparse,concurrent.futures


DATEPATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|(BB|Field)\|(Fyn| )\|\d{3,4}\|(\d{1,}|all)\|(\d{1,}|all)$"
)

log_filename = "download_process.log"
folderName= "Logs/"

def setup_logging(log_filename):
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info(f"Download Worker started. Logging to {log_filename}")

def validate_message(message):
    return bool(DATEPATTERN.match(message))

def process_download(message, log_filename,LogFolder):
    """Runs the long-running download process."""
    logging.info(f"Starting download process with message: {message}")
    process = subprocess.Popen(
        ["python3", "DownloadProcess.py", message, f"ProcessFor{log_filename}", LogFolder]
    )
    exit_code = process.wait()
    return exit_code

def callback(ch, method, properties, body, executor):
    """Callback that offloads the download task to a thread."""
    message = body.decode().strip()
    logging.info(f"Received message: {message}")

    if not validate_message(message):
        logging.error(f"Invalid message format: {message} -> Sending NACK (not requeued)")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    startTime = time.time()
    future = executor.submit(process_download, message, log_filename, folderName)

    def on_done(f):
        try:
            endtime = time.time()
            fullTime = endtime - startTime
            exit_code = f.result()
            if exit_code == 0:
                logging.info(f"Download process completed successfully, ACK sent for: {message}, it took {fullTime} seconds")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            elif exit_code == 4:
                logging.error(f"Download process gotten a message that is not supportet-> Sending NACK (not requeued)")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            elif exit_code == 7:
                logging.error(
                        "No more users with available tokens. Worker is shutting down.\n"
                        "The task will be requeued and resumed when the worker restarts and tokens are available again."
                        )                
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                sys.exit(1)
            else:
                logging.error(f"Download process failed (exit code {exit_code}), message will be requeued")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            logging.error(f"Error during download process: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


    future.add_done_callback(lambda f: ch.connection.add_callback_threadsafe(lambda: on_done(f)))

def shutdown_handler(channel, connection, executor):
    logging.info("Download worker stopping...")
    executor.shutdown(wait=False)
    logging.info("Closing RabbitMQ connection...")
    channel.stop_consuming()
    connection.close()
    logging.info("RabbitMQ connection closed. Exiting gracefully.")
    sys.exit(0)

def consume():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=30))
    channel = connection.channel()
    channel.queue_declare(queue='Download_task_queue', durable=True)
    channel.basic_qos(prefetch_count=1)


    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


    on_message_callback = lambda ch, method, properties, body: callback(ch, method, properties, body, executor)
    channel.basic_consume(queue='Download_task_queue', on_message_callback=on_message_callback)


    logging.info("Starting RabbitMQ consumer...")
    print(" [*] Waiting for messages. To exit press CTRL+C")


    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        shutdown_handler(channel, connection, executor)

def main():
    parser = argparse.ArgumentParser(description="RabbitMQ worker for processing downloads.")
    parser.add_argument("LogFile", nargs="?", default="download_process.log", help="Log file name (default: download_process.log)")
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
