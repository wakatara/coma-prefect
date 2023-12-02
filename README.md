# COMA Prefect with Docker Compose

This repo contains a dev repo for running ingstion for COMA using the
goma-apis for the UHawaii's COMA project.

This docker constrains the overall Prefect environment to _just_ the features needed for
running deploys (atm) to demonstrate the

# Limitations

Running Prefect Agent in Docker, means we can't run `DockerContainer`  
deployments unless you share the host's Docker socket with the
agent container because Docker-in-Docker is not supported.

# Getting Started

Clone this repo.

While other services like `minio`, `agent`, and `database` (postgres) are
used in this template for a production deploy, they're commented out.
This leaves the following services and sections:

- `server` - Prefect server API and UI
- `cli` - a container that mounts this repos `flows` directory for building and
  applying deployment and running flows esp for development purposes

There are also `/data` and `/home` placeholder directories to temporarily fake/mirror
the COMA filesystem if you do not have access to a `sci-backend` docker which
requires filesystem access to modify files and return results.

## Prefect Server

To run Prefect Server, `cd` to the cloned repo, and run in the terminal:

```
docker-compose --profile server up
```

This will start the Prefect server with a sqlite database.

If you have uncommented the postgres and database sections this
will also spin up a PostgreSQL Server. When the server is ready,
you will see a line that looks like:

```
server_1     | INFO:     Uvicorn running on http://0.0.0.0:4200 (Press CTRL+C to quit)
```

The Prefect Server container shares port 4200 with your host machine.
You can open a browser and go to `http://localhost:4200` to see the
Prefect UI.

## Prefect CLI

Next, open another terminal in the same directory and run:

```
docker-compose run cli
```

This runs an interactive bash session in a container that shares a
Docker network with the server you just started.

It shares the `flows` subdirectory of the repository on the host machine.
If you `ls` you'll see:

```
your_worflow_python_file.py
root@fb032110b1c1:~/flows#
```

Run `your_workflow_python_file.py` to submit the workflow.

Open `http://localhost:4200/runs` in your brwowser and you'll see your workflow(s)
running in your CLI container.

# Disabled Services

## Prefect Agent

You can run a Prefect Agent by updating `docker-compose.yml` and changing
`YOUR_WORK_QUEUE_NAME` to match the name of the Prefect work queue you would
like to connect to, and then running the following command:

`docker-compose --profile agent up`

This will run a Prefect agent and connect to the work queue you provided.

As with the CLI, you can also use Docker Compose to run an agent that connects
to Prefect Cloud by updating the agent's `PREFECT_API_URL` and `PREFECT_API_KEY`
settings in `docker-compose.yml`.

## MinIO Storage

MinIO is an S3-compatible object store that works perfectly as remote storage
for Prefect deployments. You can run it inside your corporate network and use it
as a private, secure object store, or just run it locally in Docker Compose and
use it for testing and experimenting with Prefect deployments.

If you'd like to use MinIO with Prefect in Docker compose, start them both at
once by running:

```
docker compose --profile server --profile minio up
```

Although Prefect Server won't need to talk to MinIO, Prefect agents and the
Prefect CLI will need to talk to both MinIO _and_ Prefect Server to create and
run depoyments, so it's best to start them simultaneously.

After the MinIO container starts, you can load the MinIO UI in your web browser
by navigating to `http://localhost:9000`. Sign in by entering `minioadmin` as
both the username and password.

Create a bucket named `prefect-flows` to store your Prefect flows, and then
click **Identity->Service Accounts** to create a service account. This will give
you an access key and a secret you can enter in a Prefect block to let the
Prefect CLI and agents write to and read from your MinIO storage bucket.

After you create a MinIO service account, open the Prefect UI at
`http://localhost:4200`. Click **Blocks**, then add a **Remote File System**
block. Give the block any name you'd like, but remember what name you choose
because you will need it when creating a deployment.

In the _Basepath_ field, enter `s3://prefect-flows`.

Finally, the _Settings_ JSON field should look like this:

```
{
  "key": "YOUR_MINIO_KEY",
  "secret": "YOUR_MINIO_SECRET",
  "client_kwargs": {
    "endpoint_url": "http://minio:9000"
  }
}
```

Replace the placeholders with the key and secret MinIO generated when you
created the service account. You are now ready to deploy a flow to a MinIO
storage bucket! If you want to try it, open a new terminal and run:

```
docker compose run cli
```

Then, when the CLI container starts and gives you a Bash prompt, run:

```
prefect deployment build -sb "remote-file-system/your-storage-block-name" -n "Awesome MinIO deployment" -q "awesome" "flow.py:greetings"
```

Now, if you open `http://localhost:9001/buckets/prefect-flows/browse` in a web
browser, you will see that flow.py has been copied into your MinIO bucket.

## Next Steps

You can run as many profiles as once as you'd like. For example, if you have
created a deployment and want to start and agent for it, but don't want to open
two separate terminals to run Prefect Server, an agent, _and_ MinIO you can
start them all at once by running:

```
docker compose --profile server --profile minio --profile agent up
```

And if you want to start two separate agents that pull from different work
queues? No problem! Just duplicate the agent service, give it a different name,
and set its work queue name. For example:

```
agent_two:
    image: prefecthq/prefect:2.3.0-python3.10
    restart: always
    entrypoint: ["prefect", "agent", "start", "-q", "YOUR_OTHER_WORK_QUEUE_NAME"]
    environment:
      - PREFECT_API_URL=http://server:4200/api
    profiles: ["agent"]
```

Now, when you run `docker-compose --profile agent up`, both agents will start,
connect to the Prefect Server API, and begin polling their work queues.
