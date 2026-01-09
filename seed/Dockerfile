FROM golang:1.21-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o seed ./cmd

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/seed /app/seed

EXPOSE 9000
CMD ["/app/seed"]
