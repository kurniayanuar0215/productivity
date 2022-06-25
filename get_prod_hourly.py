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
q_siteid = '''SELECT a.`jam`,a.`siteid`,a.`branch`,a.`cluster`,a.`kabupaten`,
ROUND(SUM(traffic_2g_total_erlang),2) traffic_2g,
ROUND(SUM(traffic_3g_total_erlang),2) traffic_3g,
ROUND(SUM(payload_2g_total_mbit)/8192,2) payload_2g_GB,
ROUND(SUM(payload_3g_total_mbit)/8192,2) payload_3g_GB,
ROUND(SUM(payload_4g_total_mbit)/8192,2) payload_4g_GB,
ROUND(SUM(a.`traffic_total_erlang`),2) traffic_total,
ROUND(SUM(a.`payload_total_mbit`)/8192,2) payload_total_GB
FROM `kpi`.`productivity_hourly_siteid` a
WHERE a.`jam` BETWEEN "'''+date_1+''' 00:00:00" AND "'''+date_2+''' 23:00:00" AND a.`region` = 'JABAR' '''+query_siteid+'''
GROUP BY a.`jam`,a.`siteid`;
'''

name_file = 'prod_hourly_'+date_1+'_'+date_2+'.xlsx'

prod_siteid = pd.read_sql(q_siteid, conn)

with pd.ExcelWriter('''F:/KY/prod/download/'''+name_file) as writer:
    prod_siteid.to_excel(writer, index=False, sheet_name='PROD_HOURLY_SITEID')

update.message.bot.sendDocument(update.message.chat.id, open(
    'F:/KY/prod/download/'+name_file, 'rb'))
