# syntax=docker/dockerfile:1

# ── Stage 1: build ──────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 → fully static binary; no libc dependency in runtime image.
RUN CGO_ENABLED=0 GOOS=linux go build -o /lb ./cmd/lb

# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM alpine:latest

WORKDIR /app

# Binary from builder stage.
COPY --from=builder /lb /app/lb

# Config files (includes configs/docker/ subdirectory).
COPY configs/ /app/configs/

ENTRYPOINT ["/app/lb"]
