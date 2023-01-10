"""
This DAG loads the data into neo4j graph database, and starts the neo4j container on the host.
"""
import os
import uuid
from datetime import timedelta
from typing import Optional, Union, List

from airflow import DAG, AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator, stringify
from airflow.utils.dates import days_ago
from docker.types import Mount

neo4j_conn_id = 'neo4j'


# Lib


class DockerOperatorWithExposedPorts(DockerOperator):
    """Extends DockerOperator to support port publishing.

    Solution is from https://stackoverflow.com/questions/65157416/expose-port-using-dockeroperator.
    """

    def __init__(self, *args, **kwargs):
        self.port_bindings = kwargs.pop("port_bindings", {})
        if self.port_bindings and kwargs.get("network_mode") == "host":
            self.log.warning("`port_bindings` is not supported in `host` network mode.")
            self.port_bindings = {}
        super().__init__(*args, **kwargs)

    def _run_image_with_mounts(
            self, target_mounts, add_tmp_variable: bool
    ) -> Optional[Union[List[str], str]]:
        """

        NOTE: This method was copied entirely from the base class `DockerOperator`, for the capability
        of performing port publishing.
        """
        if add_tmp_variable:
            self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
        else:
            self.environment.pop('AIRFLOW_TMP_DIR', None)
        if not self.cli:
            raise Exception("The 'cli' should be initialized before!")
        self.container = self.cli.create_container(
            command=self.format_command(self.command),
            name=self.container_name,
            environment={**self.environment, **self._private_environment},
            ports=list(self.port_bindings.keys()) if self.port_bindings else None,
            host_config=self.cli.create_host_config(
                auto_remove=False,
                mounts=target_mounts,
                network_mode=self.network_mode,
                shm_size=self.shm_size,
                dns=self.dns,
                dns_search=self.dns_search,
                cpu_shares=int(round(self.cpus * 1024)),
                port_bindings=self.port_bindings if self.port_bindings else None,
                mem_limit=self.mem_limit,
                cap_add=self.cap_add,
                extra_hosts=self.extra_hosts,
                privileged=self.privileged,
                device_requests=self.device_requests,
            ),
            image=self.image,
            user=self.user,
            entrypoint=self.format_command(self.entrypoint),
            working_dir=self.working_dir,
            tty=self.tty,
        )
        logstream = self.cli.attach(container=self.container['Id'], stdout=True, stderr=True, stream=True)
        try:
            self.cli.start(self.container['Id'])

            log_lines = []
            for log_chunk in logstream:
                log_chunk = stringify(log_chunk).strip()
                log_lines.append(log_chunk)
                self.log.info("%s", log_chunk)

            result = self.cli.wait(self.container['Id'])
            if result['StatusCode'] != 0:
                joined_log_lines = "\n".join(log_lines)
                raise AirflowException(f'Docker container failed: {repr(result)} lines {joined_log_lines}')

            if self.retrieve_output:
                return self._attempt_to_retrieve_result()
            elif self.do_xcom_push:
                if len(log_lines) == 0:
                    return None
                try:
                    if self.xcom_all:
                        return log_lines
                    else:
                        return log_lines[-1]
                except StopIteration:
                    # handle the case when there is not a single line to iterate on
                    return None
            return None
        finally:
            if self.auto_remove == "success":
                self.cli.remove_container(self.container['Id'])
            elif self.auto_remove == "force":
                self.cli.remove_container(self.container['Id'], force=True)


# DAG


default_args = {
    'owner': 'Ihar',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='load_graph_db',
    default_args=default_args,
    description='Load the data into neo4j graph database',
    schedule_interval=None,
    start_date=days_ago(2),
)

# Flow

start = EmptyOperator(task_id='start')
end = EmptyOperator(task_id='end')

host_import_dir = os.environ.get('NEO4J_HOST_IMPORT_DIR')
host_data_dir = os.environ.get('NEO4J_HOST_DATA_DIR')
print(f'host_import_dir: {host_import_dir}, host_data_dir: {host_data_dir}')

load_container = DockerOperatorWithExposedPorts(
    dag=dag,
    task_id='load_container',
    image='neo4j-script',
    container_name=f'neo4j-db-{uuid.uuid4()}',
    docker_url='tcp://docker-proxy:2375',
    mounts=[
        Mount(
            source=host_import_dir,
            target='/import',
            type='bind',
        ),
        Mount(
            source=host_data_dir,
            target='/data',
            type='bind',
        ),
    ],
    auto_remove=True,
    port_bindings={7687: 7687, 7474: 7474},
)

start >> load_container >> end
