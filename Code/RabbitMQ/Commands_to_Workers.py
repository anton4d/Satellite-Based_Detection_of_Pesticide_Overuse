import pika
import sys

Connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
Channel = Connection.channel()

Channel.exchange_declare(exchange='Commands', exchange_type='fanout')

message = ' '.join(sys.argv[1:]) or "info: Hello World!"
Channel.basic_publish(exchange='Commands', routing_key='', body=message)
print(f" [x] Sent {message}")
Connection.close()