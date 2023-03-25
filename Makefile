REGISTRY=mysql2athena
TAG=latest

all: build

build:
	docker build -t ${REGISTRY}:$(TAG) .

