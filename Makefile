IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)
COMMIT_SHA := $(git rev-parse HEAD)

docker_build:
	docker build -t $(IMAGE) .

docker_push:
	docker push $(IMAGE)

build:
	go build -ldflags="-w -s" -o main
