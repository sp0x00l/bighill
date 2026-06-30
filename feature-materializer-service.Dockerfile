ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S feature_materializer_service_server_group && \
    adduser -S feature_materializer_service_server_user -G feature_materializer_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/data_contracts/build/protobufs
COPY ./data_contracts/build/protobufs .

WORKDIR $GOPATH/src/feature_materializer_service
COPY ./feature_materializer_service .

RUN rm -f go.mod && go mod init feature_materializer_service
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs
RUN go get -d -v

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s" -o /go/bin/feature_materializer_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/feature_materializer_service /go/bin/feature_materializer_service
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/feature-materializer-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl && rm -rf /var/cache/apk/*

USER feature_materializer_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/feature-materializer-service-entrypoint.sh"]
