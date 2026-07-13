ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S socket_service_server_group && \
    adduser -S socket_service_server_user -G socket_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/socket_service
COPY ./socket_service .

RUN rm -f go.mod && go mod init socket_service
RUN go mod edit -go=1.26.4
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go mod edit -require github.com/go-playground/validator/v10@v10.28.0
RUN go mod edit -require github.com/gorilla/websocket@v1.4.1
RUN go mod edit -require github.com/redis/rueidis@v1.0.76
RUN go get -d -v

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s -X 'main.Version=${BUILD_VERSION_REQUIRED}'" -o /go/bin/socket_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/socket_service /go/bin/socket_service
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/socket-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl && rm -rf /var/cache/apk/*

USER socket_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/socket-service-entrypoint.sh"]
