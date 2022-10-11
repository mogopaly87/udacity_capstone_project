<h2>Udacity Data Engineer Nanodegree - Capstone Project</h2>

---

<h3>Project Summary</h3>
<p>
In this project, I design and build a data pipeline that provides historical weather measurement data for all available weather stations as provided by  Meteostat endpoints. This project provides a data warehouse that will serve as a sing-source-of-truth for potential data analysts and to allow dashboards to integrate location-based weather data.
</p>

<p>Documentation Structure:</p>

<ul>
<li>Scope project and source data</li>
<li>Tools and architecture</li>
<li>Data Dictionary</li>
<li>Data Model</li>
<li>Setup Steps</li>
<li>Run Application</li>
<li>Future Design Considerations</li>
</ul>

---

<h3>Scope project and source data</h3>
<p>This project will integrate weather station properties and weather measurement.</p>
<p>Data Sets:</p>
<ul>
<li><a href="https://dev.meteostat.net/bulk/stations.html">Weather Station Data</a></li>
<li><a href="https://dev.meteostat.net/bulk/monthly.html">Monthly Measurements</a></li>
</ul>

---

<h3>Tools and Architecture</h3>
<ul>
<li>Meteostat API</li>
<li>AWS S3</li>
<li>AWS Redshift</li>
<li>AWS EMR</li>
<li>Apache Airflow</li>
<li>PySpark</li>
<li>Python</li>
</ul>

<img src="Udacity_capstone.png">

---

<h3>Data Dictionary</h3>
<p><strong>Monthly Data Measurements</strong></p>

<img src="monthly.png">

<p><strong>Weather Station Properties</strong></p>

<img src="station.png">

---

<h3>Data Model</h3>>
<p>The data warehouse was designed as a Star schema with only one dimension, the weather station properties. However,
other dimensions can be derived by aggregating data from the fact table (staging_readings)</p>
<img src="udacity_capstone_project.drawio.png">
---

<h3>Setup Steps:</h3>

<ol>
    <li>Set Environment Variables<br><br>
    For your <strong>config.ini</strong> file (template test_config.ini file with project):<br>
        <ul>
            <li>S3_RAW_STATION_INPUT = s3://_your_s3_bucket_/raw_station_data</li>
            <li>S3_CLEAN_STATION_OUTPUT = s3://_your_s3_bucket_/clean_station_data</li>
            <li>S3_RAW_READINGS_INPUT = s3://_your_s3_bucket_/raw_readings_data</li>
            <li>S3_CLEAN_READINGS_OUTPUT = s3://_your_s3_bucket_/clean_readings_data</li>
            <li>BUCKET = _your_s3_bucket_</li>
            <li>PREFIX = raw_readings_data</li>
            <li>N/B: Create each subfolder in your S3 bucket (raw_station_data, clean_station_data, raw_readings_data, clean_readings_data)</li>
        </ul>
    </li><br>
    <li>Airflow Variables
        <ul>
            <li>access_key_id : _your_aws_access_key_id</li>
            <li>secret_access_key  : _your_aws_secret_key</li>
            <li>emr_id : _your_EMR_CLUSTER_ID</li>
            <li>raw_station_data_key : raw_station_data</li>
            <li>clean_readings_data_key : clean_readings_data</li>
            <li>clean_station_data_key : clean_station_data</li>
            <li>my_s3_bucket : _your_s3_bucket</li>
        </ul>
    </li><br>
    <li>Create a AWS Redshift cluster and EMR cluster with Apache Spark and Hadoop installed.</li><br>
    <li>Copy ALL Spark-related files <strong>(util.py, transform_data.py, spark_app.py, read_data.py, and download_readings_by_station_id.py)</strong> to the home directory of EMR Master node using this command:<br><br>-> <strong>scp -i ~/_path_to_/_your_key_pair.pem util.py, transform_data.py, spark_app.py, read_data.py, and download_readings_by_station_id.py hadoop@ec2-X-XX-XX-XXX.compute-1.amazonaws.com:~/.</strong></li><br>    
    <li>Remotely log into EMR cluster to confirm files are copied successfully using this command:<br><br>-> <strong>ssh -i ~/_path_to/your_key_pair.pem hadoop@ec2-X-XX-XX-XXX.compute-1.amazonaws.com</strong></li><br>
    <li>While remotely logged into EMR Master node, run this command:<br>-> <strong>pip install boto3 requests</strong></li>
</ol>

---

<h3>Run Application</h3>
<p>
To Run the application, start Airflow webserver and scheduler. Then run the DAG.
</p>

---

<h3>Future Design Considerations</h3>
<p>Since the weather measurements are continuous, an additional feature would include adding daily/monthly queries to the Meteostat API to update
data warehouse.</p>

---

<h3>Write Up</h3>
<ol>
    <li>
    The data was increased by 100x.
    <p>
        In a scenario where the data increases by 100x, my AWS EMR configuration is set to Auto Scaling if required. This would take care of any requirement
        for additional computation.
    </p>
    </li>
</ol>
