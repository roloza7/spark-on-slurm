
import os
import sys
import subprocess
import tempfile
import time
import socket
import asyncio
import atexit
import sys

from .slurm import SlurmContext, get_slurm_context, get_worker_list
from .spark import SparkContext, get_spark_context, is_port_open

class SLURMCluster(object):
    """
    Represents a slurm cluster in pyspark.
    Takes care of spinning up the cluster and shutting it down.
    """
    slurm_context : SlurmContext 
    spark_context : SparkContext
    
    def __init__(self,
                 port : int = None,
                 scratch_dir : str = None,
                 log_dir : str = None):    
            
        self.slurm_context = get_slurm_context(
            port=port,
            scratch=scratch_dir
        )

        self.spark_context = asyncio.get_event_loop().run_until_complete(get_spark_context())

        if log_dir is None:
            # Use local path
            self.log_dir = os.path.join(
                ".",
                os.environ.get("SLURM_JOB_ID", "slurm-jobqueue-logs"),
            )
        else:
            # Expand the log directory
            self.log_dir = os.path.expanduser(log_dir)
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir, exist_ok=True)
        
        self.handle = None
        self.__initialize_cluster()
        # From here on only the master node is working because the workers blocked
        atexit.register(self.shutdown)

    def __initialize_cluster(self):
        """
        Initialize the cluster.
        """
        os.environ["SPARK_HOME"] = self.spark_context.home
        os.environ["SPARK_LOG_DIR"] = self.log_dir
        # Use current python executable
        os.environ["PYSPARK_PYTHON"] = os.environ.get("PYSPARK_PYTHON", sys.executable)

        os.environ["SPARK_LOCAL_DIRS"] = self.slurm_context.scratch
        os.environ["SPARK_WORKER_MEMORY"] = os.environ.get("SLURM_MEM_PER_NODE", "4G")
    
        hostname, port = self.slurm_context.hostname, self.slurm_context.port
        if self.slurm_context.rank == 0:
            # Master node
            os.environ["SPARK_MASTER_HOST"] = hostname
            
            master_executable_path = os.path.join(self.spark_context.home, "sbin", "start-master.sh")
            print(f"Starting master with executable path: {master_executable_path}")


            args = [
                master_executable_path,
                f"--port {port}",
            ]

            self.handle = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # Wait on master to start
            self.handle.wait()
            # Check if the master started successfully
            if self.handle.returncode != 0:
                raise RuntimeError(f"Failed to start Spark master: {self.handle.stderr.read().decode('utf-8')}")
            
        else:

            # Wait for the master to start
            timeout = 60 * 5 # 5 minutes
            start_time = time.time()
            while time.time() - start_time < timeout:
                if is_port_open(hostname, port):
                    break
                print(f"[{self.slurm_context.rank}] Waiting for Spark master to start on {hostname}:{port}...")
                time.sleep(1)

            worker_executable_path = os.path.join(os.environ["SPARK_HOME"], "sbin", "start-worker.sh")

            args = [
                worker_executable_path,
                f"--cores {self.slurm_context.ncpus}",
                f"spark://{hostname}:{port}",
            ]

            # Execute the command blockingly
            # This script will exit quickly after starting the worker
            self.handle = subprocess.Popen(
                args,
                preexec_fn=os.setsid,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True)
            # Wait for the worker to start
            self.handle.wait()

            # Sleep forever to prevent slurm from killing
            subprocess.run(
                ["sleep", "infinity"],
            )

    def master(self):
        """
        Get the master node.
        """
        return self.slurm_context.hostname
    
    def port(self):
        """
        Get the port of the master node.
        """
        return self.slurm_context.port
    
    def shutdown(self):
        print("Spinning down cluster...")

        workers_list = get_worker_list()

        # SSH into each worker and send sigterm
        for worker in workers_list:
            print(f"Stopping worker on {worker}")
            args = [
                "ssh",
                worker,
                os.path.join(os.environ["SPARK_HOME"], "sbin", "stop-worker.sh"),
            ]
            subprocess.run(args, check=True)

        # Stop the master node
        if self.slurm_context.rank == 0:
            print(f"Stopping master on {self.slurm_context.hostname}")
            args = [
                os.path.join(os.environ["SPARK_HOME"], "sbin", "stop-master.sh"),
            ]
            subprocess.run(args, check=True)