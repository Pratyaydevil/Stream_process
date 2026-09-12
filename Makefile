up:
	docker compose up -d --build

logs:
	docker compose logs -f

down:
	docker compose down

clean:
	docker compose down -v
