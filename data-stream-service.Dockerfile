ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} rust:1-alpine AS query_engine_builder

RUN apk add --no-cache build-base cmake openssl-dev perl pkgconf

WORKDIR /query_engine
COPY ./data_stream_service/internal/infra/queryengine/datafusion_query_engine .
RUN cargo build --release

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S data_stream_service_server_group && \
    adduser -S data_stream_service_server_user -G data_stream_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/data_stream_service
COPY ./data_stream_service .

RUN rm -f go.mod && go mod init data_stream_service
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go get -d -v

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s" -o /go/bin/data_stream_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/data_stream_service /go/bin/data_stream_service
COPY --from=query_engine_builder /query_engine/target/release/datafusion_query_engine /usr/local/bin/datafusion_query_engine
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/data-stream-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl && rm -rf /var/cache/apk/*

USER data_stream_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/data-stream-service-entrypoint.sh"]
