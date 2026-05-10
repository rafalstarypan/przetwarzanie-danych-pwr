# Komendy projektowe — Citi Bike NYC

Skrótowy katalog komend używanych w projekcie. Sekcje uporządkowane od najczęściej używanych do diagnostycznych. Wszystkie komendy zakładają, że jesteś w katalogu repozytorium projektu (`przetwarzanie-danych-pwr/`) w terminalu Ubuntu (WSL2), chyba że zaznaczono inaczej.

---

## Spis treści

1. [Krytyczne ostrzeżenia](#1-krytyczne-ostrzeżenia)
2. [Ścieżki referencyjne](#2-ścieżki-referencyjne)
3. [Cykl życia klastra](#3-cykl-życia-klastra)
4. [HDFS](#4-hdfs)
5. [YARN](#5-yarn)
6. [Pipeline MapReduce — etap 7](#6-pipeline-mapreduce--etap-7)
7. [Spark / Jupyter — etap 8](#7-spark--jupyter--etap-8)
8. [Diagnostyka](#8-diagnostyka)
9. [Referencje i archiwum](#9-referencje-i-archiwum)

---

## 1. Krytyczne ostrzeżenia

| ❌ NIGDY nie rób | ✅ Zamiast tego |
|---|---|
| `docker compose down` | `docker compose stop` |
| `./hadoop-start.sh start` (bez `restart`) | `./hadoop-start.sh restart` |
| `./compose-up.sh ...` (po pierwszym uruchomieniu) | `docker start master slave1 slave2 slave3 spark jupyter-lab` |
| `docker volume prune` | (nie potrzebne nigdy w trakcie projektu) |

**Powód:** `compose-up.sh` automatycznie wywołuje `hadoop-start.sh start`, które wykonuje `rm -rf /data/hadoop/*` + `namenode -format` — to formatuje NameNode i niszczy całą zawartość HDFS (`/raw`, `/processed` znikają).

> Pierwsza utrata danych po niewłaściwym `compose down` kosztowała kilka godzin ponownego pozyskiwania.

---

## 2. Ścieżki referencyjne

| Ścieżka | Opis |
|---|---|
| `/mnt/c/Users/Admin/Desktop/magisterka_pwr/semestry/III/przetwarzanie_danych/przetwarzanie-danych-pwr` | Korzeń repozytorium (z hosta WSL Ubuntu) |
| `/usr/src/docker/docker-server/spark/docker-script/hadoop` | Katalog skryptów klastra hjben (compose-up, hadoop-start/stop, jupyter-start) |
| `/raw/{citibike,noaa,events,station_to_borough}/<run_date>/` | Dane źródłowe na HDFS (etap 6) |
| `/processed/{T1,T2_rides,T2_events,T2,T3a,T3b,T3c,W}/` | Wyniki pipeline'u MR (etap 7) |
| `/processed/T1_spark/` | Wynik pipeline'u Spark (etap 8) |
| `logs/mapreduce/` | Dzienniki etapu 7 (na hoście) |
| `logs/spark/` | Dzienniki etapu 8 (na hoście) |
| `outputs/etap7/` | Wyeksportowane TSV z etapu 7 |
| `C:\Users\Admin\.wslconfig` | Konfiguracja pamięci WSL2 |

---

## 3. Cykl życia klastra

### 3.1. Codzienne — wybudzenie

Po `docker compose stop` lub restarcie hosta:

```bash
docker start master slave1 slave2 slave3 spark jupyter-lab
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Wszystkie 6 kontenerów powinno być `Up`. Następnie weryfikacja demonów Hadoop:

```bash
docker exec master jps
```

Spodziewane: `NameNode`, `SecondaryNameNode`, `DataNode`, `ResourceManager`, `NodeManager`, `JobHistoryServer`. Jeśli któregoś brakuje — restart demonów (sekcja 3.3).

### 3.2. Codzienne — zatrzymanie

```bash
docker exec master /opt/scripts/hadoop-stop.sh
docker compose stop
```

(Skrypt `hadoop-stop.sh` zatrzymuje demonów Hadoop grzecznie — flushuje editlogs NameNode'a; `docker compose stop` zatrzymuje kontenery zachowując ich wolumeny.)

### 3.3. Restart demonów Hadoop (bez utraty danych)

Gdy demon padł (np. RM zginął na OOM, NodeManager zniknął z `yarn node -list`):

```bash
docker exec master /opt/scripts/hadoop-stop.sh
docker exec master /opt/scripts/hadoop-start.sh restart
```

**`restart`** — bezpieczne, zachowuje dane. **`start`** — formatuje NameNode (NIE używać).

Po restarcie weryfikacja:

```bash
docker exec master jps
docker exec master yarn node -list -all
```

Powinno być widać 6 demonów na master + 4 nody RUNNING w YARN.

### 3.4. Pierwsza inicjalizacja (jednorazowo, w przeszłości)

```bash
cd /usr/src/docker/docker-server/spark/docker-script/hadoop
./compose-up.sh 3.3.5 3.4.0 3 /Users/Shared/workspace/docker-ws/spark-notebook /tmp/hadoop /tmp/hadoop_logs /tmp/spark_logs
```

⚠️ **Po pierwszym uruchomieniu nigdy więcej nie używać `compose-up.sh`** — formatuje NameNode.

### 3.5. Po `wsl --shutdown` lub restarcie Windows

WSL ginie ze stanem kontenerów. Wolumeny Dockera (HDFS data) leżą w `ext4.vhdx` i przeżywają. Po restarcie:

1. Otwórz Docker Desktop ręcznie (jeśli nie wstał sam)
2. `docker ps -a` → kontenery powinny mieć status `Exited (137)` (= SIGKILL)
3. `docker start master slave1 slave2 slave3 spark jupyter-lab`
4. Weryfikacja jak w 3.1

---

## 4. HDFS

### 4.1. Listowanie i rozmiary

```bash
docker exec master hdfs dfs -ls /raw                              # top-level listing
docker exec master hdfs dfs -ls -R /processed | head -40          # rekursywnie, pierwsze 40 linii
docker exec master hdfs dfs -du -s -h /raw /processed             # sumaryczny rozmiar (replikacja zlicza ×3)
docker exec master hdfs dfs -du -h /processed                     # rozmiar per podkatalog
```

### 4.2. Wyświetlanie zawartości

```bash
docker exec master hdfs dfs -cat /processed/W/part-* | head -10   # head pierwszych 10 linii
docker exec master hdfs dfs -cat /processed/W/part-* | wc -l      # liczba wierszy
```

### 4.3. Wgrywanie i pobieranie

```bash
docker exec master hdfs dfs -mkdir -p /movies/
docker exec master hdfs dfs -put -f /tmp/file.csv /movies/                       # upload pojedynczego pliku
docker exec master hdfs dfs -setrep -w 3 /movies/file.csv                        # ustawienie replikacji
docker exec master hdfs dfs -get /processed/W /tmp/W                             # pobranie katalogu
docker exec master hdfs dfs -getmerge /processed/W /tmp/W.tsv                    # scalenie part-* w jeden plik
```

### 4.4. Sprawdzenie integralności (replikacja)

```bash
docker exec master hdfs fsck /processed -files | grep "Average block replication"
```

Spodziewane: `Average block replication: 3.0`.

### 4.5. Czyszczenie

```bash
docker exec master hdfs dfs -rm -r -f /processed/T1                              # rekursywnie, bez kosza
docker exec master hdfs dfs -rm -r -f /processed/T1 /processed/T2 ...            # wiele ścieżek na raz
```

`-r` rekursywnie, `-f` bez potwierdzenia, `-skipTrash` można dołączyć żeby ominąć kosz Hadoopa.

---

## 5. YARN

### 5.1. Stan klastra

```bash
docker exec master yarn node -list -all                                          # lista wszystkich nodów
docker exec master hdfs dfsadmin -report 2>&1 | head -10                         # raport NameNode
```

Lista nodów powinna pokazać 4 RUNNING (master + 3 slaves).

### 5.2. Aplikacje

```bash
docker exec master yarn application -list                                        # aktywne aplikacje
docker exec master yarn application -list -appStates FINISHED 2>&1 | head -20    # ostatnie zakończone
docker exec master yarn application -status <APP_ID>                             # szczegóły aplikacji
docker exec master yarn application -kill <APP_ID>                               # zabicie aplikacji
```

### 5.3. UI w przeglądarce

| URL | Co |
|---|---|
| `http://localhost:8088` | YARN ResourceManager — lista aplikacji, counters |
| `http://localhost:9870` | HDFS NameNode UI — Browse Directory, replikacja |
| `http://localhost:19888` | JobHistoryServer (szczegóły zakończonych zadań MR) |
| `http://localhost:4040` | Spark Application UI (tylko podczas aktywnej sesji Sparka) |
| `http://localhost:8088/proxy/<APP_ID>/` | Tracking URL aplikacji (mappery, reducery, counters) |

---

## 6. Pipeline MapReduce — etap 7

### 6.1. Tryby uruchomienia

```bash
# Tryb DEMO (1M rekordów, ~8 min)
bash scripts/mr/run_all.sh --month 2025-06 --sample

# Tryb PEŁNY (cały miesiąc czerwca, ~10 min)
bash scripts/mr/run_all.sh --month 2025-06

# Wszystkie miesiące (mar-lis 2025) — uwaga: ~1-2 h
bash scripts/mr/run_all.sh

# Pipeline po przygotowanych danych (skip prepare + skip build)
bash scripts/mr/run_all.sh --skip-prepare --skip-build

# Pipeline po przygotowanych danych, ale z rebuildem JAR-a
bash scripts/mr/run_all.sh --skip-prepare
```

### 6.2. Tylko prepare (rozpakowanie ZIP-ów Citi Bike)

```bash
bash scripts/mr/prepare_inputs.sh --month 2025-06 --sample            # 1 part = ~1M wierszy
bash scripts/mr/prepare_inputs.sh --month 2025-06                     # wszystkie 5 partycji = ~4,76M
bash scripts/mr/prepare_inputs.sh                                     # wszystkie miesiące, wszystkie partycje
```

### 6.3. Cleanup outputów (przed re-runem)

```bash
# Pełny cleanup pipeline'u (zachowuje /raw)
docker exec master hdfs dfs -rm -r -f \
  /processed/citibike \
  /processed/T1 /processed/T2 /processed/T2_rides /processed/T2_events \
  /processed/T3a /processed/T3b /processed/T3c \
  /processed/W

# Cleanup tylko od stage'u 3 (zachowuje T1, T2)
docker exec master hdfs dfs -rm -r -f /processed/T3a /processed/T3b /processed/T3c /processed/W
```

### 6.4. Pojedyncze etapy

```bash
bash scripts/mr/run_stage1.sh        # E1 — clean & normalize
bash scripts/mr/run_stage2.sh        # E2a + E2b + E2c
bash scripts/mr/run_stage3.sh        # E3a + E3b + E3c
bash scripts/mr/run_stage4.sh        # E4 — enrich
```

### 6.5. Eksport wyników na host

```bash
bash scripts/mr/export_results.sh
ls -lh outputs/etap7/                # W.tsv, T3a.tsv, T3b.tsv, T3c.tsv (z nagłówkami)
```

### 6.6. Weryfikacja

```bash
bash scripts/mr/verify.sh 2025-06-15 Manhattan
bash scripts/mr/verify.sh                                         # default: 2025-06-15 Manhattan

wc -l outputs/etap7/W.tsv                                         # spodziewane ~150 (30 dni × 5 boroughs + header)
```

### 6.7. Monitor pipeline'u (drugi terminal Ubuntu)

```bash
watch -n 5 'echo "=== WSL ==="; wsl.exe -d Ubuntu-20.04 -- free -h | head -2; echo "=== RM alive? ==="; docker exec master jps 2>/dev/null | grep ResourceManager || echo "RM DEAD"; echo "=== YARN apps ==="; docker exec master yarn application -list 2>/dev/null | tail -5'
```

### 6.8. Logi etapu 7

```bash
ls -lt logs/mapreduce/ | head -20                                # najnowsze logi
cat logs/mapreduce/run_all_<TS>.log                              # sumaryczny log z timingami per stage
cat logs/mapreduce/<TS>_E1.log                                   # log konkretnego sub-joba
grep "duration=" logs/mapreduce/run_all_*.log                    # czasy wszystkich runów
```

---

## 7. Spark / Jupyter — etap 8

### 7.1. Start Jupytera (z hosta)

```bash
cd /usr/src/docker/docker-server/spark/docker-script/hadoop
./jupyter-start.sh
```

Skrypt wypisze URL z tokenem typu `http://127.0.0.1:8888/?token=abc...`. Skopiuj go do przeglądarki.

### 7.2. Pobranie tokenu Jupytera (jeśli zgubiony)

```bash
docker exec jupyter-lab jupyter server list
```

### 7.3. Restart Jupytera w razie problemów

```bash
docker exec -d jupyter-lab jupyter lab --ip=0.0.0.0 --port=8888 --allow-root --no-browser
sleep 3
docker exec jupyter-lab jupyter server list
```

### 7.4. Sanity check środowiska Spark (3 komórki w nowym notebooku)

```python
# Komórka 1 — inicjalizacja
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("yarn")
         .appName("etap8-sanity-check")
         .config("spark.executor.memory", "512m")
         .config("spark.executor.cores", "1")
         .config("spark.executor.instances", "2")
         .getOrCreate())

print("Spark version:", spark.version)
print("Master:", spark.sparkContext.master)

# Komórka 2 — test rozproszonego obliczenia
result = spark.range(1000000).filter("id % 7 == 0").count()
print("Records divisible by 7:", result)        # spodziewane: 142858

# Komórka 3 — test integracji z HDFS
df = spark.read.option("header", True).csv("/processed/citibike/202506-citibike-tripdata_1.csv")
print("Columns:", df.columns)
print("Row count:", df.count())                  # spodziewane: 1000000
df.show(3)

# Sprzątanie
spark.stop()
```

### 7.5. Wersje i porty

```bash
docker exec spark spark-submit --version 2>&1 | grep -E "version|Spark"
docker port jupyter-lab                          # porty 8888 (Jupyter) + 4040-4044 (Spark UI)
docker port spark                                # 8080 (master UI), 8081 (worker UI), 8998 (Livy)
```

### 7.6. Notebook etapu 8 (E1 w PySpark)

W Jupyterze: upload `src/spark/etap8_e1_demo.ipynb` lub przez docker cp:

```bash
docker exec jupyter-lab pwd                      # ustal working dir Jupytera
docker cp src/spark/etap8_e1_demo.ipynb jupyter-lab:/<workdir>/
```

### 7.7. Eksport notebooka do HTML

```bash
docker exec jupyter-lab jupyter nbconvert --to html /<workdir>/etap8_e1_demo.ipynb
docker cp jupyter-lab:/<workdir>/etap8_e1_demo.html internal_docs/8/
```

### 7.8. Skopiowanie logu Sparka na host

```bash
docker cp jupyter-lab:/tmp/<TS>_E1_spark.log logs/spark/
```

(`<TS>` widoczne w komórkach 1 i 7 notebooka.)

---

## 8. Diagnostyka

### 8.1. Pamięć WSL2

```bash
wsl.exe -d Ubuntu-20.04 -- free -h               # z PowerShell lub WSL Ubuntu
cat /proc/meminfo | head -5                      # tylko z WSL
```

Spodziewane: `total ~8.7 GiB` (cap z `.wslconfig` = 9 GB minus narzut WSL kernela).

### 8.2. OOM-killer (jeśli ResourceManager znika)

```bash
wsl.exe -d Ubuntu-20.04 -- dmesg | grep -iE "killed process|out of memory" | tail -20
```

### 8.3. Konfiguracja `.wslconfig`

`C:\Users\Admin\.wslconfig`:

```ini
[wsl2]
memory=9GB
swap=4GB
processors=8
localhostForwarding=true
```

Po zmianie:

```powershell
wsl --shutdown
```

(z PowerShell jako Admin), potem otwarcie Docker Desktop.

### 8.4. Stan demonów Hadoop

```bash
docker exec master jps                                                 # spodziewane: NN, SNN, DN, RM, NM, JHS
docker exec slave1 jps                                                 # tylko DN, NM (na slave'ach jps może nie pokazać NM w obrazie hjben)
docker exec master hdfs dfsadmin -report 2>&1 | head -20               # status NameNode + lista DataNodes
```

### 8.5. Logi Hadoop wewnątrz kontenera

```bash
docker exec master bash -c 'ls $HADOOP_HOME/logs/'                                              # listing
docker exec master bash -c 'tail -100 $HADOOP_HOME/logs/hadoop-root-resourcemanager-master.log' # RM log
docker exec master bash -c 'cat $HADOOP_HOME/logs/hadoop-root-resourcemanager-master.out'       # RM stdout/stderr (np. crash dump)
docker exec master bash -c 'grep -E "FATAL|OutOfMemory|Exception|ERROR" $HADOOP_HOME/logs/hadoop-root-resourcemanager-master.log | tail -40'
```

> Uwaga: `$HADOOP_HOME` rozwija się tylko wewnątrz `bash -c '...'` w kontenerze, nie na hoście.

### 8.6. Statystyki Dockera

```bash
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"
```

### 8.7. Stan kontenerów

```bash
docker ps -a --format 'table {{.Names}}\t{{.Status}}'                  # all (włącznie z exited)
docker logs <container_name> --tail 50                                  # ostatnie 50 linii loga konkretnego kontenera
```

---

## 9. Referencje i archiwum

### 9.1. Wbudowany przykład Hadoopa — obliczenie π

```bash
docker exec -it master bash
yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-$HADOOP_VERSION.jar pi 2 5
```

### 9.2. Przykład MovieLens (z dokumentacji hjben)

```bash
docker exec -it master bash
wget https://files.grouplens.org/datasets/movielens/ml-20m.zip -O /tmp/ml-20m.zip
unzip /tmp/ml-20m.zip -d /tmp/
cd /tmp/ml-20m
hdfs dfs -mkdir -p /movies/
hdfs dfs -put movies.csv /movies/
hdfs dfs -setrep -w 2 /movies/movies.csv
```

### 9.3. Wersje technologii

| Komponent | Wersja |
|---|---|
| Hadoop | 3.3.5 |
| Spark | 3.4.0 |
| Scala | 2.12.17 |
| Java | OpenJDK 1.8.0_372 |
| Python (Jupyter) | 3.11 |
| Replikacja HDFS | 3 |

### 9.4. Argumenty `compose-up.sh` (do referencji)

```
./compose-up.sh <hadoop_ver> <spark_ver> <num_slaves> <notebook_dir> <hadoop_data> <hadoop_logs> <spark_logs>
./compose-up.sh 3.3.5 3.4.0 3 /Users/Shared/workspace/docker-ws/spark-notebook /tmp/hadoop /tmp/hadoop_logs /tmp/spark_logs
```

### 9.5. Lista kontenerów i ich rola

| Kontener | Rola |
|---|---|
| `master` | NameNode, SecondaryNameNode, ResourceManager, JobHistoryServer, DataNode, NodeManager |
| `slave1`, `slave2`, `slave3` | DataNode, NodeManager |
| `spark` | Spark master + worker (tryb standalone, dla nas mniej istotne — używamy YARN) |
| `jupyter-lab` | Jupyter Lab z PySpark, dostęp przez `localhost:8888` |
| `mariadb` | (nieużywane w projekcie) |

### 9.6. Schemat tabeli wynikowej W

```
date  borough  total_rides  member_rides  avg_duration_min  temp  precip  snow  event_types  expected_rides  event_intensity  anomaly  demand_level
```

13 kolumn, format TSV bez nagłówka (nagłówek dodawany przy eksporcie przez `export_results.sh`).
