import time,os,pika,subprocess,re,sys,logging,argparse,concurrent.futures


class Download_Worker:
    def __init__(self,Log_File_Name,Log_Folder_Name):
        """
        Initialize the Download Worker.
        :param Log File Name: The name of the workers logfile 
        :param Log Folder Namer: The name of the folder the log should be in. Root folder is Logs/
        """
        self.Log_File_Name = f"{Log_File_Name}"
        self.Log_Folder_Name = f"Logs/{Log_Folder_Name}/"
        self.Work_Order_Pattern = re.compile(
            r"^\w{1,}\|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|(BB|Field)\|(Fyn|all|Test)\|(\d{3,4}|-1)\|(\d{1,}|all)\|(\d{1,}|all)$"
            )
        self.Work_Order_ID = None
        self.Setup_Logging()
        
    def Setup_Logging(self):
        os.makedirs(self.Log_Folder_Name, exist_ok=True)
        logging.basicConfig(
            filename=F"{self.Log_Folder_Name}{self.Log_File_Name}.log",
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        logging.info(f"Download Worker started. Logging to {self.Log_File_Name}")

    def Validate_Message(self,message):
        return bool(self.Work_Order_Pattern.match(message))
    
    def Extract_Work_Order_ID(self, message):
        self.Work_Order_ID = message.split('|', 1)[0]

    def Process_Download(self,message):
        """Runs the long-running download process."""
        logging.info(f"Starting download process with message: {message}")
        self.Extract_Work_Order_ID(message)
        process = subprocess.Popen(
            [sys.executable, "Download_Process.py", message, f"{self.Log_File_Name}_{self.Work_Order_ID}.log", self.Log_Folder_Name]
        )
        exit_code = process.wait()
        return exit_code
    
    def Commands_Callback(self,ch, method, properties, body, executor,connection):
        message = body.decode().strip()
        logging.info(f"Received message: {message}")
        if message == "Stop":
            self.Shutdown_Handler(ch,connection,executor)

    def Shutdown_Handler(self,channel, connection, executor):
        logging.info("Download worker stopping...")
        executor.shutdown(wait=False)
        logging.info("Closing RabbitMQ connection...")
        channel.stop_consuming()
        connection.close()
        logging.info("RabbitMQ connection closed. Exiting gracefully.")
        sys.exit(0)

    def callback(self,ch, method, properties, body, executor):
        """Callback that offloads the download task to a thread."""
        message = body.decode().strip()
        logging.info(f"Received message: {message}")

        if not self.Validate_Message(message):
            logging.error(f"Invalid message format: {message} -> Sending NACK (not requeued)")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        startTime = time.time()
        future = executor.submit(self.Process_Download, message)

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

    def consume(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=30))
        channel = connection.channel()

        channel.exchange_declare(exchange="Commands", exchange_type="fanout")
        result = channel.queue_declare(queue="", exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange="Commands", queue=queue_name)


        channel.queue_declare(queue='Download_task_queue', durable=True)
        channel.basic_qos(prefetch_count=1)


        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


        on_message_callback = lambda ch, method, properties, body: self.callback(ch, method, properties, body, executor)
        channel.basic_consume(queue=queue_name, on_message_callback=lambda ch, method, properties, body: 
                            self.Commands_Callback(ch, method, properties, body, executor,connection))
        channel.basic_consume(queue='Download_task_queue', on_message_callback=on_message_callback)


        logging.info("Starting RabbitMQ consumer...")


        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.Shutdown_Handler(channel, connection, executor)


def main():
    parser = argparse.ArgumentParser(description="RabbitMQ worker for processing downloads.")
    parser.add_argument("LogFile", nargs="?", default="download_process.log", help="Log file name (default: download_process.log)")
    parser.add_argument("LogFolder", nargs="?",default="Logs",help="The folder the log files should be saved to (default: Logs")
    args = parser.parse_args()

    log_filename = args.LogFile
    folderName = args.LogFolder

    Worker =   Download_Worker(log_filename,folderName)
    Worker.consume()


if __name__ == "__main__":
    main()
