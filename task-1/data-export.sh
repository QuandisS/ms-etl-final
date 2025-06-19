yc ydb  \
--endpoint grpcs://ydb.serverless.yandexcloud.net:2135 \
--database /ru-central1/b1g5u0ua309ujjunpg99/etnonf89ebklrhn40b2v \
--sa-key-file authorized_key.json \
import file csv \
--path transactions_v2 \
--delimiter "," \
--skip-rows 1 \
--null-value "" \
--verbose \
Fraud.csv