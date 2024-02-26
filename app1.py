import pika
import pandas as pd
import duckdb
import multiprocessing
import time

# RabbitMQ connection details
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_QUEUE_NAME = "your_queue_name"

# Number of desired processes (adjust as needed)
NUM_PROCESSES = 10000

# Function to process the data and write to DuckDB
def process_data(data, process_id):
    try:
        # Load data from bytes (assuming Parquet format)
        df = pd.read_parquet(data, engine="pyarrow")

        # Perform operations on the data (replace with your actual logic)
        # ...

        # Connect to DuckDB
        con = duckdb.connect(f"duckdb://memory?process_id={process_id}")

        # Save data to DuckDB table
        df.to_parquet(f"data_{process_id}.parquet", engine="duckdb", con=con)

        # Close connection
        con.close()

        print(f"Data processed and saved successfully for process {process_id}.", flush=True)

    except Exception as e:
        print(f"Error processing data for process {process_id}: {e}", flush=True)

def worker(process_id, queue):
    while True:
        if not queue.empty():
            data = queue.get()
            process_data(data, process_id)
        else:
            print(f"No messages in queue for process {process_id}. Sleeping for 5 seconds...", flush=True)
            time.sleep(5)

def main():
    # Set up RabbitMQ connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )
    channel = connection.channel()

    # Set up multiprocessing queue
    multiprocessing_queue = multiprocessing.Queue()

    # Create and start worker processes
    processes = []
    for i in range(NUM_PROCESSES):
        process = multiprocessing.Process(target=worker, args=(i, multiprocessing_queue))
        process.start()
        processes.append(process)

    def on_message(channel, method, properties, body):
        if body:
            multiprocessing_queue.put(body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print("No messages in queue. Sleeping for 5 seconds...", flush=True)
            connection.sleep(5)  # Sleep instead of abrupt shutdown

    # Set up RabbitMQ consumer
    channel.basic_consume(queue=RABBITMQ_QUEUE_NAME, on_message_callback=on_message)

    print("Waiting for messages...", flush=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()
