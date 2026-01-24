.PHONY: run up down clean logs ps topics events bronze bronze-tree silver silver-tree quarantine-tree gold psql kpi-sample detector alerts-sample down-all


run: up topics
# 	docker compose --profile events up -d --build generator
	docker compose --profile pipeline up -d --build bronze-writer
	docker compose --profile pipeline up -d --build silver-writer
	docker compose --profile pipeline up -d --build gold-writer
	docker compose --profile detector up -d --build anomaly-detector


down-all:
	docker compose --profile pipeline --profile detector down

up:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f

ps:
	docker compose ps

topics: up
	docker exec -i redpanda-0 rpk topic create ecom.events.raw.v1 --brokers localhost:9092 -p 3 -r 1 || true

events: topics
	docker compose --profile events up -d --build generator

bronze: topics
	docker compose --profile pipeline up -d --build bronze-writer

bronze-tree:
	docker compose --profile debug run --rm mc tree local/lake/bronze/ecom_events/v1/

silver: topics
	docker compose --profile pipeline up -d --build silver-writer

silver-tree:
	docker compose --profile debug run --rm mc tree local/lake/silver/ecom_events/v1/

quarantine-tree:
	docker compose --profile debug run --rm mc tree local/lake/quarantine/ecom_events/v1/

gold: topics
	docker compose --profile pipeline up -d --build gold-writer

psql:
	docker exec -it postgres psql -U app -d analytics

kpi-sample:
	docker exec -i postgres psql -U app -d analytics -c "SELECT window_start, metric, value FROM serving.kpi_minute ORDER BY window_start DESC, metric LIMIT 30;"

detector: topics
	docker compose --profile detector up -d --build anomaly-detector

alerts-sample:
	docker exec -i postgres psql -U app -d analytics -c "SELECT window_start, metric, severity, value, baseline, score FROM serving.alerts ORDER BY window_start DESC LIMIT 20;"

