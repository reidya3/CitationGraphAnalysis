<h1 align="center">
  <img alt="collaborations", height=300px, src="images/final-report-visuals/page-rank-graph-2.png"     />
  <br/>
  Citation Graph Analysis 
</h1>
<h3 align="center">
  Anthony Reidy, 18369643. Kian Sweeney, 18306226.
  <br/><br/><br/>
</h3>


## Table of Contents
- [Preamble](#preamble)
- [Report](#report)
- [Technologies Used](#technologies-used)
- [Graph Analytics](#graph-analytics)
- [Video Link](#video-link)

## Preamble
An initial study of researcher relationships within DCU’s school of computing revealed little collaboration. In this investigation, we  explored this observation further.  First, we scraped faculty member’s Google Scholar and DORAS profiles  to  extract    nuggets  of  information  using  the BeatuifulSoup and Selenium libraries on pySpark.  Next, we explored our inital observation further by employing graph algorithms using GraphX.  Overall, we believe that the results generated from this investigation are crucial for understanding research partnerships within DCU. We hope that our work may provide useful information which will enable cooperation, especially for early researchers who may not know the expertise and influence of certain faculty members.

## Report
Our midway and final report can be found in the [reports](reports) folder. 

## Technologies Used
![Tech_used](images/final-report-visuals/updated_graphx.png)

## Graph Analytics
We utilise GraphX (scala) to implement our graph algrorithims. `Sbt assembly` is used to create a scala aplication  where a endpoint exists for every graph algorithim. The bash script used to start the master and worker node can be found [here](graph-anayltics/run-graph-algos.sh). The scala app can be found [here](graph-analytics/app)

## Video Link
The link to the video showcasing us running the described technologies can be found [here](https://drive.google.com/drive/u/0/folders/1v81OPN7zrqRXe1KVx2XWd4sqBn140gYz).
