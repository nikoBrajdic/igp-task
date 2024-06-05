# igp-task
iGamingPlatform's recruitment task solution

This project demonstrates communication between the user, a Python Falcon REST API and a Python background service 
over Kafka, utilising the Redis cache server to sync parallel messaging operations,
ensuring communication idempotence.

<hr>

## Setup

- To run the project, Docker and Docker Compose must be available and running on the system.
- Before creating the services, create a `.env` file with the required variables. An example file `.env.sample` is provided.
- To create and enable services, run this command from the root of the project:

```bash
docker compose up --build
```

- To remove all containers created by the previous command, run:

```bash
docker compose down
```

<hr>

## Usage

To send messages to the Falcon service, send a POST request including the API Key via the
`x-api-key` header. e.g.:
```bash
curl -X POST -H "X-Api-Key: <your key>" -d '{"number": [1, 2, 3, 4]}' localhost:8000/numbers
```

To retrieve the processed message, send a GET request including the API Key via the 
`x-api-key` header. e.g.:
```bash
curl -X GET  -H "X-Api-Key: <your key>" -H "Accept: application/json" -H "Content-Type: application/json" localhost:8000/final_numbers
```

### Limitation
When the Falcon app is scaled to multiple instances, when adding a consumer to the group handling the messages from the `final-numbers` topic
the group goes into infinite rebalance. This occurs when an additional Falcon replica attempts to lazily 
initialize the consumer and attach it to the group, essentially allowing for only one consumer instance to join the group. 
On the other hand, this doesn't happen with the group handling the initial `numbers` topic, as they immediately start polling the topic upon startup. 
Further investigation is required.
