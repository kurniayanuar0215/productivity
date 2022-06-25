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
q_siteid = '''SELECT a.`tanggal`,a.`siteid`,a.`branch`,a.`cluster`,a.`kabupaten`,
ROUND(SUM(traffic_2g_total_erlang),2) traffic_2g,
ROUND(SUM(traffic_3g_total_erlang),2) traffic_3g,
ROUND(SUM(payload_2g_total_mbit)/8192,2) payload_2g_GB,
ROUND(SUM(payload_3g_total_mbit)/8192,2) payload_3g_GB,
ROUND(SUM(payload_4g_total_mbit)/8192,2) payload_4g_GB,
ROUND(SUM(a.`traffic_total_erlang`),2) traffic_total,
ROUND(SUM(a.`payload_total_mbit`)/8192,2) payload_total_GB
FROM `kpi`.`productivity_daily_siteid` a
WHERE a.tanggal BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" AND a.`region` = 'JABAR' '''+query_siteid+'''
GROUP BY a.`tanggal`,a.`siteid`;
'''

q_kab = '''SELECT `tanggal`, `tahun`, `bulan`, DAY(`tanggal`) DAY, WEEK(`tanggal`+2) WEEK,
`region`, `branch`, `cluster`, `kabupaten`,
SUM(`traffic_2g_total_erlang`) traffic_2g,
SUM(`traffic_3g_total_erlang`) traffic_3g,
SUM(`payload_2g_total_mbit`)/8192 payload_2g_GB,
SUM(`payload_3g_total_mbit`)/8192 payload_3g_GB,
SUM(`payload_4g_total_mbit`)/8192 payload_4g_GB,
SUM(`traffic_total_erlang`) traffic_total,
SUM(`payload_total_mbit`)/8192 payload_total_GB
FROM kpi.`productivity_daily_kabkec`
WHERE `tanggal` BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" AND `region` = 'JABAR'
GROUP BY `tanggal`, `tahun`, `bulan`, DAY, WEEK, `region`, `branch`, `cluster`, `kabupaten`;
'''

q_rtp = '''SELECT a.`tahun`, a.`tanggal`, a.`bulan`, DAY(a.`tanggal`) DAY, WEEK(a.`tanggal`+2) WEEK, b.rtp,
SUM(`traffic_2g_total_erlang`) traffic_2g,
SUM(`traffic_3g_total_erlang`) traffic_3g,
SUM(`payload_2g_total_mbit`)/8192 payload_2g_GB,
SUM(`payload_3g_total_mbit`)/8192 payload_3g_GB,
SUM(`payload_4g_total_mbit`)/8192 payload_4g_GB,
SUM(`traffic_total_erlang`) traffic_total,
SUM(`payload_total_mbit`)/8192 payload_total_GB
FROM kpi.`productivity_daily_kabkec` a
JOIN test.`dapot_rtp` b
ON CONCAT(a.`kabupaten`,"-",a.`kecamatan`) = b.kabkec
WHERE a.tanggal BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" AND b.`region` = 'JABAR'
GROUP BY a.`tahun`, a.`tanggal`, a.`bulan`, DAY, WEEK, b.rtp;
'''

q_region = '''SELECT `tanggal`, DAYOFWEEK(DATE_SUB(tanggal, INTERVAL 1 DAY)) DAY, minggu, AREA, `payload_total_mbit`/(8*1024*1024) PY_TB
FROM kpi.`productivity_daily_area`
WHERE `tanggal` BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" AND `AREA` = 'JABAR';
'''

q_user4g = '''SELECT a.`tanggal`, a.`siteid`, ROUND(SUM(a.`user_number_max`)) User_4G
FROM kpi.`kpi_daily_4g` a
WHERE a.tanggal BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" '''+query_siteid+'''
GROUP BY a.`tanggal`,a.`siteid`;
'''

name_file = 'prod_'+date_1+'_'+date_2+'.xlsx'

if count_param == 1 or count_param == 3:
    prod_siteid = pd.read_sql(q_siteid, conn)
    prod_kab = pd.read_sql(q_kab, conn)
    prod_rtp = pd.read_sql(q_rtp, conn)
    prod_region = pd.read_sql(q_region, conn)
    # kpi_user4g = pd.read_sql(q_user4g, conn)

    with pd.ExcelWriter('''F:/KY/prod/download/'''+name_file) as writer:
        prod_siteid.to_excel(
            writer, index=False, sheet_name='PROD_DAILY_SITEID')
        prod_kab.to_excel(
            writer, index=False, sheet_name='PROD_DAILY_KAB')
        prod_rtp.to_excel(
            writer, index=False, sheet_name='PROD_DAILY_RTP')
        prod_region.to_excel(
            writer, index=False, sheet_name='PROD_DAILY_REGION')
        # kpi_user4g.to_excel(
        #     writer, index=False, sheet_name='KPI_USER4G')

elif count_param == 4:
    prod_siteid = pd.read_sql(q_siteid, conn)

    with pd.ExcelWriter('''F:/KY/prod/download/'''+name_file) as writer:
        prod_siteid.to_excel(
            writer, index=False, sheet_name='PROD_DAILY_SITEID')

update.message.bot.sendDocument(update.message.chat.id, open(
    'F:/KY/prod/download/'+name_file, 'rb'))
