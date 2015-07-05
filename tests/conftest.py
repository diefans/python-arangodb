"""
global settings
"""

import pytest


@pytest.fixture(scope="session")
def docker_client():
    from docker import Client

    client = Client()

    return client


@pytest.fixture(scope="session")
def docker_arangodb_container(request, docker_client):
    """Create a intermediary docker container a arangodb instance"""

    import socket

    container = docker_client.create_container(
        image="arangodb/arangodb:2.6.1",
        command="standalone --disable-authentication --disable-initialize",
    )

    docker_client.start(container=container['Id'])

    container = docker_client.inspect_container(container['Id'])

    def tear_down():
        docker_client.remove_container(container=container['Id'], v=True, force=True)

    request.addfinalizer(tear_down)

    # wait for connection
    while True:
        sock = socket.socket()
        try:
            sock.connect((container['NetworkSettings']['IPAddress'], 8529))
            break

        except socket.error:
            pass

    return container


@pytest.fixture(scope="session")
def docker_arangodb(request, docker_arangodb_container):
    """set the client factory for the docker container."""

    from arangodb import api, meta

    endpoint = "http://{NetworkSettings[IPAddress]}:8529".format(**docker_arangodb_container)

    client = api.Client(
        database='pytest',
        endpoint=endpoint
    )

    system_client = api.SystemClient(
        endpoint=endpoint
    )

    system_client.create_database("pytest")

    def factory(cls):
        return client

    meta.MetaBase.__client_factory__ = factory.__get__

    def tear_down():
        meta.MetaBase.__client_factory__ = None

    request.addfinalizer(tear_down)

    return client
