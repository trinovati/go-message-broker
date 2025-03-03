FROM golang:1.24.0-alpine

RUN apk add git

ARG CI_PERSONAL_USER
ARG CI_PERSONAL_ACCESS_TOKEN


WORKDIR /app
ADD . /app

RUN go env -w GOPRIVATE="gitlab.com/aplicacao/*"
RUN touch ~/.netrc
RUN echo -e "machine gitlab.com\n login $CI_PERSONAL_USER\n password $CI_PERSONAL_ACCESS_TOKEN" > ~/.netrc


COPY go.mod ./
COPY go.sum ./

RUN go mod download
RUN go mod verify

COPY . ./