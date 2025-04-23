FROM golang:1.24-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /src


COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /app/cloud-data-sync ./cmd/cloud-data-sync


FROM alpine:latest

RUN apk add --no-cache sqlite-libs

WORKDIR /app

COPY --from=builder /app/cloud-data-sync /app/cloud-data-sync

ENTRYPOINT ["/app/cloud-data-sync"]
