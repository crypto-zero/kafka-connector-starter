import os
import argparse
import signal
import time

import httpx

parser = argparse.ArgumentParser(description="Kafka Connector Starter")
parser.add_argument(
    "--kafka-connect-url",
    default=os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083"),
    help="Kafka Connect URL",
)


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True


def main():
    args = parser.parse_args()
    if not args.kafka_connect_url:
        print("Kafka Connect URL is required")
        return
    killer = GracefulKiller()
    while not killer.kill_now:
        try:
            response = httpx.get(f"{args.kafka_connect_url}/connectors")
            response.raise_for_status()
            connectors = response.json()
            for connector in connectors:
                response = httpx.get(
                    f"{args.kafka_connect_url}/connectors/{connector}/status"
                )
                response.raise_for_status()
                tasks = response.json()["tasks"]
                for task in tasks:
                    task_id = task["id"]
                    response = httpx.get(
                        f"{args.kafka_connect_url}/connectors/{connector}/tasks/{task_id}/status"
                    )
                    response.raise_for_status()
                    task_status = response.json()["state"]
                    if task_status == "RUNNING":
                        print(f"Connector {connector} is already running")
                        continue
                    response = httpx.post(
                        f"{args.kafka_connect_url}/connectors/{connector}/tasks/{task_id}/restart"
                    )
                    response.raise_for_status()
                    task_trace = task.get("trace", "")
                    print(
                        f"Restarted task {task_id} for connector {connector}: {task_trace}"
                    )
        except httpx.HTTPStatusError as e:
            print(f"Failed to get connectors: {e}")
        except Exception as e:
            print(f"Failed to get connectors: {e}")

        # Sleep for 5 seconds
        time.sleep(5)


if __name__ == "__main__":
    main()
