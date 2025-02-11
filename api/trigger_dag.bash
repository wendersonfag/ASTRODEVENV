curl -X POST "http://localhost:8080/api/v1/dags/cls_bitcoin/dagRuns" \
-u "admin:admin" \
-H "Content-Type: application/json" \
-d '{
    "dag_run_id": "manual_run_cls_bitcoin_'"$(date +%Y%m%dT%H%M%S)"'"
}'