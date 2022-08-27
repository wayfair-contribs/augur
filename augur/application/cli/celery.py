#SPDX-License-Identifier: MIT

"""
Augur library commands for controlling the backend components
"""

import os
import click
import subprocess
from redis.exceptions import ConnectionError as RedisConnectionError

from augur import instance_id
from augur.application.logs import AugurLogger
from augur.tasks.init.redis_connection import redis_connection
from augur.application.cli import test_connection, test_db_connection 

logger = AugurLogger("augur", reset_logfiles=True).get_logger()

@click.group('celery', short_help='Commands for controlling the backend API server & data collection workers')
def cli():
    """Placeholder docstring."""

@cli.command("start")
@test_connection
@test_db_connection
def start():
    """Start Augur's celery process."""
    celery_process = None

    celery_command = f"celery -A augur.tasks.init.celery_app.celery_app worker --loglevel=debug --concurrency=20 -n {instance_id}@%h"
    celery_process = subprocess.Popen(celery_command.split(" "))

    try:
        celery_process.wait()
    except KeyboardInterrupt:

        if celery_process:
            logger.info("Shutting down celery process")
            celery_process.terminate()

        try:
            logger.info("Flusing redis cache")
            redis_connection.flushdb()
            
        except RedisConnectionError:
            pass

@cli.command("clear-tasks")
@test_connection
@test_db_connection
def clear():


    while True:

        user_input = str(input("Warning this will remove all the tasks from all instances on this server!\nWould you like to proceed? [y/N]"))

        if not user_input:
            logger.info("Exiting")
            return
        
        if user_input in ("y", "Y", "Yes", "yes"):
            logger.info("Removing all tasks")
            celery_purge_command = "celery -A augur.tasks.init.celery_app.celery_app purge -f"
            subprocess.call(celery_purge_command.split(" "))
            return

        elif user_input in ("n", "N", "no", "NO"):
            logger.info("Exiting")
            return
        else:
            logger.error("Invalid input")

    