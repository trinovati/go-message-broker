FROM golang:1.24.0-alpine


RUN apk add git


WORKDIR /app


COPY go.mod ./ 
COPY go.sum ./


RUN go mod download
RUN go mod verify


COPY . ./