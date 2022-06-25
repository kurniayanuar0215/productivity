import mysql.connector
import pandas as pd
import os

# DB CONFIG
conn = mysql.connector.connect(
    host="10.47.150.144",
    user="sqaviewjbr",
    password="5qAv13wJbr@2019#",
    database="kpi"
)
# QUERY
q_siteid = '''SELECT '2G',a.`jam`,a.`siteid`,
SUM(`tch_traffic_erl`+`tch_traffic_erl`) traffic,
SUM(`gprs_payload_mbit`+`edge_payload_ul_mbit`+`edge_payload_dl_mbit`)/8192 payload_GB
FROM kpi.`kpi_hourly_2g` a
WHERE `jam` BETWEEN "'''+date_1+''' 00:00:00" AND "'''+date_2+''' 23:00:00" AND LEFT(siteid,3) IN (
    'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB', 
    'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL', 
    'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX'
  )GROUP BY a.`jam`,a.`siteid`
  
UNION
  
SELECT '3G',a.`jam`,a.`siteid`,
SUM(`traffic_voice_erlang`+`traffic_video_erlang`) traffic,
SUM(`ps_payload_dl_mbit`+`ps_payload_ul_mbit`+`hsdpa_payload_mbit`+`hsupa_payload_mbit`)/8192 payload_GB
FROM kpi.`kpi_hourly_3g` a
WHERE `jam` BETWEEN "'''+date_1+''' 00:00:00" AND "'''+date_2+''' 23:00:00" AND LEFT(siteid,3) IN (
    'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB', 
    'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL', 
    'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX'
)GROUP BY a.`jam`,a.`siteid`

UNION
  
SELECT '4G',a.`jam`,a.`siteid`,0,
SUM(`traffic_dl_volume_mbit`+`traffic_ul_volume_mbit`)/8192 payload_GB
FROM kpi.`kpi_hourly_4g` a
WHERE `jam` BETWEEN "'''+date_1+''' 00:00:00" AND "'''+date_2+''' 23:00:00" AND LEFT(siteid,3) IN (
    'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB', 
    'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL', 
    'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX'
 )GROUP BY a.`jam`,a.`siteid`;'''

name_file = 'prod_kpi_hourly_'+date_1+'_'+date_2+'.xlsx'

prod_siteid = pd.read_sql(q_siteid, conn)

with pd.ExcelWriter('''F:/KY/prod/download/'''+name_file) as writer:
    prod_siteid.to_excel(writer, index=False, sheet_name='PROD_KPI_HOURLY_SITEID')

update.message.bot.sendDocument(update.message.chat.id, open(
    'F:/KY/prod/download/'+name_file, 'rb'))
