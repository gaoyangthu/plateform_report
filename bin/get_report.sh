#!/bin/bash
#######################################
## A script send report to all involved
#######################################

HADOOP=/usr/bin/hadoop
HDFS=/usr/bin/hdfs

dir_name=$(dirname $0)
curr_path=$(cd $dir_name;pwd)

Configured_Capacity=0
Present_Capacity=0
DFS_Remaining=0
DFS_Used=0
DFS_Used_Percent=0
Under_Replicated=0
Missing_Blocks=0

curr=$(date +"%Y-%m-%d")

## Get report data from HDFS which will be saved in cluster_info
function getReport(){
	$HDFS dfsadmin -report  > $curr_path/../cache/cluster_info 2>/dev/null
	$HADOOP fs -du -s -h /home/datamining/flume/DPI >> $curr_path/../cache/cluster_info 2>&1
}

## A common function to get TeraByte formated data from report info.
function getTBInfo(){
	info=$(grep "$1" "$curr_path/../cache/cluster_info" |awk -F\( '{print $2}'|awk -F 'TB' '{print $1}')

	echo "$info"
}

## Get normal form infomation in the formate "{INFO} : {VALUE}".
function getNormalInfo(){
	info=$(grep "$1" "$curr_path/../cache/cluster_info"|awk -F: '{print $2}')

	echo "$info"
}

## Get province usage.
function getProvinceUsage(){
	info=$(grep "$1" "$curr_path/../cache/cluster_info" |awk '$1 > 0 {print $1$2}')

	echo "$info"
}



## Collect all information from stored report info.
function collectInfo(){
	Configured_Capacity=$(getTBInfo "Configured Capacity")
	Present_Capacity=$(getTBInfo "Present Capacity")
	DFS_Remaining=$(getTBInfo "DFS Remaining")
	DFS_Used=$(getTBInfo "DFS Used:")

	DFS_Used_Percent=$(getNormalInfo "DFS Used%")
	Under_Replicated=$(getNormalInfo "Under replicated blocks")
	Missing_Blocks=$(getNormalInfo "Missing blocks")
}

## Format the generated data.
function formatInfo(){

cat > $curr_path/../cache/formated_info <<EOF
<html>
<!-- CSS goes in the document HEAD or added to your external stylesheet -->
<style type="text/css">
table.gridtable {
	font-family: verdana,arial,sans-serif;
	font-size:14px;
	color:#333333;
	border-width: 1px;
	border-color: #666666;
	border-collapse: collapse;
}
table.gridtable th {
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #dedede;
}
table.gridtable td {
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #ffffff;
	text-align:center;
}
</style>

<title>Hadoop平台状态报告</title>

<body>
<center><h2>Hadoop平台状态报告($curr)</h2></center>
<center><h3>hadoop集群状态</h3></center>
<center><table class="gridtable">
  <tr>
    <th>日期</th>
    <th>总容量(TB)</th>
    <th>HDFS使用量(TB)</th>
    <th>HDFS使用百分比(%)</th>
    <th>HDFS剩余量(TB)</th>
    <th>复制中块数</th>
    <th>丢失的块数</th>
  </tr>

  <tr>
    <td>$curr</td>
    <td>$Present_Capacity</td>
    <td>$DFS_Used</td>
    <td>$DFS_Used_Percent</td>
    <td>$DFS_Remaining</td>
    <td>$Under_Replicated</td>
    <td>$Missing_Blocks</td>
  </tr>
</table>
</center>
</html>
EOF
}

## Send mail to indeviduals that are listed as prameters.
function sendMail(){
	#mail  XXX@gmail.com < ./formated_info
	/usr/bin/php $curr_path/../lib/sendmail.php "Hadoop平台状态报告" $curr_path/../cache/formated_info 2>&1 >> $curr_path/../logs/mail.log
	echo "[$(date +"%Y-%m-%d %H:%M:%S")] mail sended." 2>&1 >> $curr_path/../logs/report.log
}

getReport
collectInfo
formatInfo
sendMail
