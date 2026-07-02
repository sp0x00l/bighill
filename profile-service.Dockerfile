ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S profile_service_server_group && \
    adduser -S profile_service_server_user -G profile_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/data_contracts/build/protobufs
COPY ./data_contracts/build/protobufs .

WORKDIR $GOPATH/src/profile_service
COPY ./profile_service .

RUN rm -f go.mod && go mod init profile_service
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs
RUN go get -d -v

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags "kafka musl" -ldflags="-w -s -X 'main.Version=${BUILD_VERSION_REQUIRED}'" -o /go/bin/profile_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/profile_service /go/bin/profile_service
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/profile-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl postgresql-client && rm -rf /var/cache/apk/*

USER profile_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/profile-service-entrypoint.sh"]
