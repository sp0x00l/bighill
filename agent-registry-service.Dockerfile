ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S agent_registry_service_server_group && \
    adduser -S agent_registry_service_server_user -G agent_registry_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/data_contracts/build/protobufs
COPY ./data_contracts/build/protobufs .

WORKDIR $GOPATH/src/agent_registry_service
COPY ./agent_registry_service .

RUN rm -f go.mod go.sum && go mod init agent_registry_service
RUN go mod edit -go=1.26.4
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs
RUN go mod tidy
RUN go mod download

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s -X 'main.Version=${BUILD_VERSION_REQUIRED}'" -o /go/bin/agent_registry_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/agent_registry_service /go/bin/agent_registry_service
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/agent-registry-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl && rm -rf /var/cache/apk/*

USER agent_registry_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/agent-registry-service-entrypoint.sh"]
