FROM golang:1.21-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o kv-node ./cmd/kv-node

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/kv-node /app/kv-node
COPY config.yaml /app/config.yaml

EXPOSE 8080-8100
CMD ["/app/kv-node", "/app/config.yaml"]
