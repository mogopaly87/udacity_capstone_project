<h2>Udacity Data Engineer Nanodegree - Capstone Project</h2>

---

<h3>Project Summary</h3>
<p>
In this project, I design and build a data pipeline that provides historical weather measurement data for all available weather stations as provided by  Meteostat endpoints. This project provides a data warehouse that will serve as a sing-source-of-truth for potential data analysts.
</p>

<p>Documentation Structure:</p>

<ul>
<li>Scope project and source data</li>
<li>Tools and architecture</li>
<li>Data Dictionary</li>
<li>Define the data model</li>
<li>Setup Steps</li>
</ul>

---

<h3>Scope project and source data</h3>
<p>This project will integrate weather station data and weather measurement data with fact and dimension tables.</p>
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

<img scr="monthly.png">

<p><strong>Weather Station Properties</strong></p>

<img scr="station.png">
