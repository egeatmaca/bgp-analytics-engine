# BGP Analytics Engine

## Getting Started
1. Starting containers: <br>
    <code>sudo docker compose up</code>
2. Running data collection:
    1. Open a terminal inside the <code>bgp-spark-client</code> container: <br>
        <code>sudo docker compose exec -it bgp-spark-client /bin/bash</code>
    2. Run data collection: <br>
        <code>python src/main.py</code>
3. Accessing endpoints:
    1. Jupyter Notebook: <code>http://localhost:8888</code>
    2. Spark UI: <code>http://localhost:8080</code>
    3. Spark History Server: <code>http://localhost:18080</code>
