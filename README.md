# 🎬 Best Director–Actor Partnerships on IMDb — Hadoop MapReduce vs Apache Spark

![Java](https://img.shields.io/badge/Java-8-ED8B00?logo=openjdk&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-3.1.3-66CCFF?logo=apachehadoop&logoColor=black)
![Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-container-2496ED?logo=docker&logoColor=white)

A distributed analytics platform that finds the **most successful director–actor and director–actress partnerships
in film history**, using the official IMDb datasets (3.6 GB). The same pipeline is implemented in
**Hadoop MapReduce (Java)** and **Apache Spark (Python)**, runs on a 3-node **HDFS + YARN** cluster,
and is exposed through a containerised **REST API**. The two frameworks are then benchmarked against
each other and against a single-machine Python script.

Final project for the **Cloud Computing** course — MSc in Artificial Intelligence and Data Engineering,
University of Pisa, academic year 2025/26.

---

## ✨ Highlights

- **Same algorithm, three implementations**: a cascade of 3 MapReduce jobs, a single Spark DAG of RDD transformations,
  and a local Python baseline.
- **MapReduce optimisations**: a combiner and a custom `Writable` to cut shuffle traffic, multiple reducers for the
  heavy jobs, and a custom descending comparator with a single reducer for a globally sorted output.
- **Cluster deployment** on three virtual machines: one master (NameNode, ResourceManager) and two workers (DataNode, NodeManager).
- **REST API** (OpenAPI/Swagger, Flask + Connexion) to launch jobs asynchronously, monitor them and read the ranking
  straight from HDFS — packaged as a lightweight Docker container running on the master node.
- **Experimental evaluation** on three dataset sizes (0.5, 1.5, 3.6 GB): execution time, YARN overhead,
  CPU (vcore-seconds) and memory (MB-seconds).

## 🧠 The problem

For every pair (director, actor/actress) the system computes the **average rating of the titles they made together**.
To keep the ranking statistically meaningful it only considers:

- titles with **at least 100,000 votes**;
- pairs with **at least 3 collaborations**.

Input: a denormalised TSV built by joining three IMDb tables, one record per person per title:

```
MovieID      PersonName          Role       MovieRating   NumVotes
tt0111161    Frank Darabont      director   9.3           2900000
```

## ⚙️ Pipeline

| Phase | MapReduce | Spark |
|------|-----------|-------|
| **1. Pair generation** — clean records, group by title, cartesian product directors × actors | Job 1 (4 reducers) | `map` → `filter` → `groupByKey` → `flatMap` |
| **2. Aggregation** — sum and count per pair, keep pairs with ≥ 3 collaborations, compute average | Job 2 (combiner + custom `Writable`, 4 reducers) | `mapValues` → `reduceByKey` → `filter` |
| **3. Global ranking** — sort by average rating, descending | Job 3 (custom comparator, 1 reducer) | `sortBy(ascending=False)` |

Titles with an abnormal number of credits (more than 50 directors or 500 actors) are skipped to avoid overloading a single reducer.

## 🏗️ Architecture

```
            ┌──────────────┐   HTTP    ┌───────────────────────────── VM1 (master) ─────────┐
 Client ───▶│  Swagger UI  │──────────▶│  Docker: Flask REST API ──▶ hadoop jar / spark-submit │
            └──────────────┘           │  NameNode · ResourceManager                          │
                                       └──────────────────────┬───────────────────────────────┘
                                                YARN / HDFS   │
                                   ┌──────────────────────────┴──────────────────────────┐
                                   ▼                                                     ▼
                         VM2 (worker): DataNode · NodeManager          VM3 (worker): DataNode · NodeManager
```

### REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/job?framework={MapReduce\|Spark}&inputFile={0.5GB\|1.5GB\|3.6GB (full)}` | Starts a job in the background and returns its `jobId` (HTTP 202) |
| `GET`  | `/jobs` | Status of all jobs submitted in the current session |
| `GET`  | `/jobs/{jobId}` | Status, execution time and — when completed — the top 100 pairs read from HDFS |

## 📊 Results

Real execution time measured by YARN (for MapReduce, the sum of the three jobs):

| Approach | 0.5 GB | 1.5 GB | 3.6 GB |
|----------|-------:|-------:|-------:|
| Hadoop MapReduce | 86 s | 106 s | 141 s |
| Apache Spark | 49 s | 84 s | 170 s |
| Python (local, single machine) | **16 s** | **37 s** | **85 s** |

Resource usage on the full dataset:

| Approach | CPU (vcore-seconds) | Memory (MB-seconds) |
|----------|--------------------:|--------------------:|
| Hadoop MapReduce | 1,292 | 375,889 |
| Apache Spark | 490 | 440,511 |

**Key findings**

- Spark is faster on small and medium inputs, while MapReduce scales better up to the full dataset in this setup.
- The two frameworks **trade CPU for memory**: Spark uses less than half the CPU of MapReduce but holds more RAM for
  longer, since it keeps data in memory between stages.
- YARN adds a fixed overhead of about **15–25 s** per job (resource negotiation, container allocation, ApplicationMaster start-up).
- At 3.6 GB the **local script beats both clusters**: a distributed system pays off only when the data no longer fits
  on a single machine.

Full analysis in [`Report-MontalbanoGroup.pdf`](Report-MontalbanoGroup.pdf).

## 🗂️ Repository structure

```
├── cloudmr/                 # Hadoop MapReduce implementation (Java, Maven)
│   └── src/main/java/it/unipi/
│       ├── MainDriver.java  # chains the 3 jobs, custom descending comparator
│       ├── Job1.java        # pair generation
│       ├── Job2.java        # aggregation: combiner + RatingCountWritable
│       └── Job3.java        # global ranking
├── JobSpark.py              # Apache Spark implementation (PySpark, RDD API)
├── Local_python.py          # single-machine baseline
├── python-flask-server/     # REST API (Swagger-generated Flask server) + Dockerfile, docker-compose
└── Report-MontalbanoGroup.pdf
```

## 🚀 Running it

The dataset is not included: it can be rebuilt from the [IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/)
by joining titles, ratings and principals into the 5-column TSV shown above.

**Local baseline**

```bash
python3 Local_python.py dataset.tsv results.tsv
```

**Hadoop MapReduce** (requires a Hadoop 3 cluster and Maven)

```bash
cd cloudmr && mvn clean package
hadoop jar target/cloudmr-1.0-SNAPSHOT.jar it.unipi.MainDriver <hdfs_input> <hdfs_output>
```

**Apache Spark on YARN**

```bash
spark-submit --master yarn JobSpark.py <hdfs_input> <hdfs_output>
```

**REST API** (on the master node; Hadoop and Spark are mounted from the host, not bundled in the image)

```bash
cd python-flask-server
docker compose up -d --build
# Swagger UI: http://<master-node>:8080/ui/
```

The server expects the jar and `JobSpark.py` in `/home/hadoop/ProgettoCloud/` and the input files on HDFS as
`datasetSmall`, `datasetMedium` and `datasetCompleto`. Paths are set in
`python-flask-server/swagger_server/controllers/default_controller.py`.

## ⚠️ Limitations

- Job state is kept in memory: restarting the REST server loses the job history.
- Cluster paths and dataset names are hard-coded for the course environment.
- The ranking key concatenates names with `_`, so homonyms are not distinguished.

## 👥 Authors — Montalbano Group

- **Andrea Doni** — [@Andryd22](https://github.com/Andryd22)
- **Dario Falaschi** — [@Dariofala](https://github.com/Dariofala)
- **Leonardo Lentini** — [@LeonardoLentini](https://github.com/LeonardoLentini)
