import os
import time
import connexion
import six
import threading
import subprocess

from swagger_server.models.inline_response200 import InlineResponse200  # noqa: E501
from swagger_server.models.inline_response2001 import InlineResponse2001  # noqa: E501
from swagger_server.models.inline_response202 import InlineResponse202  # noqa: E501
from swagger_server import util

# STATO GLOBALE E VARIABILI DI AMBIENTE
tracked_jobs = {}
job_counter = 0

HDFS_PATHS = {
    "3.6GB (full)": "datasetCompleto",
    "1.5GB": "datasetMedium",
    "0.5GB": "datasetSmall"
}

def esegui_job_background(job_id, framework, input_file_key):
    """
    Esegue fisicamente il comando sul cluster in background.
    """
    output_dir = f"output_job_{job_id}"
    input_path = HDFS_PATHS.get(input_file_key, "datasetCompleto")

    try:
        my_env = os.environ.copy()

        start_time = time.time()

        if framework == "MapReduce":
            comando = [
                "hadoop", "jar",
                "/home/hadoop/ProgettoCloud/cloudmr-1.0-SNAPSHOT.jar",
                "it.unipi.MainDriver",
                input_path, output_dir
            ]
            print(f"[Thread-{job_id}] Start {framework} on {input_path}...")
            # Per MapReduce passiamo l'ambiente pulito standard
            subprocess.run(comando, check=True)

        elif framework == "Spark":
            comando = [
                "spark-submit", "--master", "yarn",
                "--conf", "spark.eventLog.enabled=false",
                "/home/hadoop/ProgettoCloud/JobSpark.py",
                input_path, output_dir
            ]
            
            # Driver nel container: usa il comando base di sistema
            my_env["PYSPARK_DRIVER_PYTHON"] = "python3" 
            # Executors sulle VM: usa il percorso che YARN si aspetta
            my_env["PYSPARK_PYTHON"] = "/usr/bin/python3.10" 

            print(f"[Thread-{job_id}] Start {framework} on {input_path}...")
            # Per Spark passiamo a subprocess l'ambiente modificato (env=my_env)
            subprocess.run(comando, env=my_env, check=True)
            
        else:
            tracked_jobs[str(job_id)]["status"] = "FAILED"
            return

        end_time = time.time()
        execution_time = round(end_time - start_time, 2)

        # Aggiorna lo stato al termine dell'esecuzione
        tracked_jobs[str(job_id)]["status"] = "COMPLETED"
        tracked_jobs[str(job_id)]["executionTime"] = execution_time
        print(f"[Thread-{job_id}] Job completed successfully!")

    except subprocess.CalledProcessError as e:
        tracked_jobs[str(job_id)]["status"] = "FAILED"
        print(f"[Thread-{job_id}] Error during execution: {e}")

# ENDPOINTS REST
def job_post(framework=None, input_file=None):  # noqa: E501
    """Launch a job"""
    global job_counter
    
    # Incremento manuale dell'ID
    job_counter += 1
    job_id = str(job_counter)
    
    # Registrazione iniziale in RAM
    tracked_jobs[job_id] = {
        "jobId": job_id,
        "status": "RUNNING",
        "framework": framework,
        "executionTime": None
    }
    
    # Avvio del calcolo asincrono
    thread = threading.Thread(target=esegui_job_background, args=(job_id, framework, input_file))
    thread.start()
    
    # Ritorna immediatamente la conferma al client
    return {"message": f"Job {framework} accepted and started in background", "jobId": job_id}, 202


def jobs_get():  # noqa: E501
    """Get the status of all recent jobs"""
    
    # Trasforma i valori del dizionario in una lista per rispettare l'array JSON dello YAML
    lista_jobs = [{"jobId": job["jobId"], "status": job["status"]} for job in tracked_jobs.values()]
    return lista_jobs, 200


def jobs_job_id_get(job_id):  # noqa: E501
    """Get the status and results of a specific job"""
    
    # Verifica esistenza
    if job_id not in tracked_jobs:
        return "Job not found", 404
        
    # Crea una copia per non modificare il dizionario originale
    job_info = {
        "jobId": tracked_jobs[job_id]["jobId"], 
        "status": tracked_jobs[job_id]["status"],
        "framework": tracked_jobs[job_id].get("framework", "Unknown") 
    }

    # Se il job ha calcolato il tempo, lo aggiungiamo alla risposta
    if tracked_jobs[job_id].get("executionTime") is not None:
        job_info["executionTime"] = tracked_jobs[job_id]["executionTime"]
    
    # Se ha finito, vai a leggere da HDFS
    if job_info["status"] == "COMPLETED":
        output_dir = f"output_job_{job_id}"
        try:
            # Esegue cat del file di output e ne cattura il testo
            hdfs_output = subprocess.check_output([
                "hadoop", "fs", "-cat", f"{output_dir}/part-*"
            ])
            
            raw_lines = hdfs_output.decode('utf-8').strip().split('\n')[:100]
            
            formatted_results = []
            for line in raw_lines:
                if not line:
                    continue
                
                parts = line.split('\t')

                # Controllo di sicurezza
                if len(parts) >= 2:
                    try:
                        rating = float(parts[0])
                        pair = parts[1]
                        formatted_line = f"{rating:.2f} - {pair}"
                        formatted_results.append(formatted_line)
                    except ValueError:
                        # Se la conversione in float fallisce, stampa la riga con un trattino
                        formatted_results.append(line.replace('\t', ' - '))
                else:
                    # Se non c'è tab, stampa la riga così com'è
                    formatted_results.append(line)

            job_info["results"] = formatted_results
            
        except subprocess.CalledProcessError:
            job_info["status"] = "FAILED"
            job_info["results"] = ["Error: Can't read output file from HDFS"]
            
    return job_info, 200
