FROM golang as build

COPY . /build
WORKDIR /build

RUN go get -d
RUN go build -o kafka_exporter

FROM gcr.io/distroless/base

COPY --from=build /build/kafka_exporter /bin/kafka_exporter

EXPOSE 9308
ENTRYPOINT ["/bin/kafka_exporter"]
