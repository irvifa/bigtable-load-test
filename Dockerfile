FROM golang:alpine AS builder

# Git is required for fetching the dependencies.
RUN apk update && apk add --no-cache git bash

COPY . .

RUN go get -d -v

RUN go build -ldflags="-w -s" -o /go/bin/main

CMD ["/bin/bash", "-c", "./run.sh"]
