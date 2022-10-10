#SPDX-License-Identifier: MIT
import subprocess
from datetime import datetime
import logging
import json
import sqlalchemy as s

from augur.application.db.session import DatabaseSession
from augur.tasks.init.celery_app import celery_app as celery, engine
from augur.application.db.models import RepoLabor, WorkerHistory, WorkerJob, Repo
from augur.application.db.engine import create_database_engine

logger = logging.getLogger(__name__)
worker_type = "value_worker"
tool_source = 'Value Worker'
tool_version = '1.0.0'
data_source = 'SCC'


@celery.task
def value_model(repo_git: str):
    """ Data collection and storage method."""

    with DatabaseSession(logger) as session:

        config = session.config
        repo_id = session.query(Repo).filter(Repo.repo_git == repo_git).one().repo_id
        

        repo_directory = config.get_value('Workers', 'facade_worker')['repo_directory']
        scc_bin = config.get_value('Value_Task', 'scc_bin')

        logger.info(repo_id)

        repo_path_sql = s.sql.text("""
            SELECT repo_id, CONCAT(repo_group_id || chr(47) || repo_path || repo_name) AS path
            FROM repo
            WHERE repo_id = :repo_id
        """)

        relative_repo_path = engine.execute(
            repo_path_sql, {'repo_id': repo_id}).fetchone()[1]
        absolute_repo_path = repo_directory + relative_repo_path

        try:
            generate_value_data(repo_id, absolute_repo_path, scc_bin, session)
        except Exception as e:
            logger.error(e)


def generate_value_data(repo_id, path, scc_bin, session):
    """Runs scc on repo and stores data in database

    :param repo_id: Repository ID
    :param path: Absolute path of the Repostiory
    """
    logger.info('Running `scc`....')
    logger.info(f'Repo ID: {repo_id}, Path: {path}')

    output = subprocess.check_output(
        [scc_bin, '-f', 'json', path])
    records = json.loads(output.decode('utf8'))

    value_data = []
    for record in records:
        for file in record['Files']:
            repo_labor = {
                'repo_id': repo_id,
                'rl_analysis_date': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'programming_language': file['Language'],
                'file_path': file['Location'],
                'file_name': file['Filename'],
                'total_lines': file['Lines'],
                'code_lines': file['Code'],
                'comment_lines': file['Comment'],
                'blank_lines': file['Blank'],
                'code_complexity': file['Complexity'],
                'tool_source': tool_source,
                'tool_version': tool_version,
                'data_source': data_source,
                'data_collection_date': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            value_data.append(repo_labor)

    repo_labor_natural_keys = ["repo_id", "rl_analysis_date", "file_path", "file_name"]
    return_columns = ["repo_labor_id"]
    result = session.insert_data(value_data, RepoLabor,repo_labor_natural_keys, return_columns)
    
    logger.info(f"Added {len(result)} rows to Repo Labor")
