import argparse
import base64

from google.cloud import pubsub_v1


def pub(project_id: str, topic_id: str, message: str) -> None:
    """Publishes a message to a Pub/Sub topic."""
    # Initialize a Publisher client.
    client = pubsub_v1.PublisherClient()
    # Create identifier `projects/{project_id}/topics/{topic_id}`
    topic_path = client.topic_path(project_id, topic_id)

    # Data sent to Cloud Pub/Sub
    data = base64.b64encode(message.encode())

    # Returns a future.
    api_future = client.publish(topic_path, data)
    message_id = api_future.result()

    print(f"Published {data} to {topic_path}: {message_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Google Cloud project ID")
    parser.add_argument("topic_id", help="Pub/Sub topic ID")

    args = parser.parse_args()

    message = str(input("Write a message to pub/sub: "))

    pub(args.project_id, args.topic_id, message)
