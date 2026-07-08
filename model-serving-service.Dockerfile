ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S model_serving_service_server_group && \
    adduser -S model_serving_service_server_user -G model_serving_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/model_serving_service
COPY ./model_serving_service .

RUN rm -f go.mod && go mod init model_serving_service
RUN go mod edit -go=1.25.0
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go get -d -v

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s" -o /go/bin/model_serving_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/model_serving_service /go/bin/model_serving_service
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./shared_py /usr/local/src/shared_py
COPY ./scripts/docker/services/model-serving-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl python3 py3-pip && \
    python3 -m pip install --break-system-packages /usr/local/src/shared_py && \
    rm -rf /var/cache/apk/*

USER model_serving_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/model-serving-service-entrypoint.sh"]
