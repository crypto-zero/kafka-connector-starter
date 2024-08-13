import os
import argparse
import signal
import time
import logging

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
    logging.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("Starting Kafka Connector Starter")

    args = parser.parse_args()
    if not args.kafka_connect_url:
        logger.error("Kafka Connect URL is required")
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
                connector_status = response.json()
                if connector_status["connector"]["state"] != "RUNNING":
                    response = httpx.post(
                        f"{args.kafka_connect_url}/connectors/{connector}/restart"
                    )
                    response.raise_for_status()
                    connector_trace = connector_status.get("connector", {}).get(
                        "trace", ""
                    )
                    logger.info(f"Restarted connector {connector}: {connector_trace}")

                tasks = connector_status["tasks"]
                for task in tasks:
                    task_id = task["id"]
                    response = httpx.get(
                        f"{args.kafka_connect_url}/connectors/{connector}/tasks/{task_id}/status"
                    )
                    response.raise_for_status()
                    task_status = response.json()
                    if task_status["state"] == "RUNNING":
                        logger.info(f"Connector {connector} is already running")
                        continue
                    response = httpx.post(
                        f"{args.kafka_connect_url}/connectors/{connector}/tasks/{task_id}/restart"
                    )
                    response.raise_for_status()
                    task_trace = task.get("trace", "")
                    logger.info(
                        f"Restarted task {task_id} for connector {connector}: {task_trace}"
                    )
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to get connectors: {e}")
        except Exception as e:
            logger.error(f"Failed to restart connectors: {e}")

        # Sleep for 5 seconds
        time.sleep(5)


if __name__ == "__main__":
    main()
