sudo docker compose -f docker-compose.yaml -f docker-compose.test.yaml up -d --build
EXIT_CODE=$(sudo docker compose -f docker-compose.yaml -f docker-compose.test.yaml wait "test-runner")
EXIT_CODE=$(echo $EXIT_CODE | grep -oE '[0-9]+' | tail -1)
sudo docker compose -f docker-compose.yaml -f docker-compose.test.yaml logs test-runner
sudo docker compose -f docker-compose.yaml -f docker-compose.test.yaml down -v
exit $EXIT_CODE
