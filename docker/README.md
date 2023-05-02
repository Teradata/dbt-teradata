# Docker for dbt-teradata

This docker file is suitable for building dbt-teradata Docker images locally or using with CI/CD to automate populating a container registry.

## Building an image:
In order to build a new image, run the following docker command.
```
docker build --tag <your_image_name> <path/to/dockerfile>
```

## Running an image in a container:
The `ENTRYPOINT` for this Dockerfile is the command `dbt` so you can bind-mount your project to `/usr/app` and use dbt as normal:
```
docker run \
--network=host
--mount type=bind,source=path/to/project,target=/usr/app \
--mount type=bind,source=path/to/profiles.yml,target=/root/.dbt/profiles.yml \
my-dbt \
ls
```

### Examples:

To build an image named "my-dbt"
```
cd dbt-core/docker
docker build --tag my-dbt
```

To run a dbt job with above created image "my-dbt"
```
docker run --mount type=bind,source=/root/jaffle_shop,target=/usr/app --mount type=bind,source=/root/jaffle_shop/profiles.yml,target=/root/.dbt/profiles.yml my-dbt:latest ls
```