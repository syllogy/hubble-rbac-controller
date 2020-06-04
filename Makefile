.PHONY: test

up:
	docker-compose up -d

down:
	docker-compose down

test:
	go test  -v ./... --tags 'integration'
