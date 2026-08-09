# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the OpenTelemetry Astronomy Shop Demo - a microservice-based distributed system designed to illustrate OpenTelemetry instrumentation and observability. The demo consists of multiple services written in different languages (Go, C#, Java, JavaScript, Python, Rust, PHP, Ruby, C++, Kotlin) communicating via gRPC and HTTP.

## Architecture

The demo follows a microservices architecture with these core services:

- **Frontend** (Next.js) - Web UI and API gateway
- **Ad Service** (Java/Gradle) - Advertisement recommendations
- **Cart Service** (C#/.NET) - Shopping cart management
- **Checkout Service** (Go) - Order processing
- **Currency Service** (C++) - Currency conversion
- **Email Service** (Ruby) - Email notifications
- **Payment Service** (Node.js) - Payment processing
- **Product Catalog** (Go) - Product information
- **Recommendation Service** (Python) - Product recommendations
- **Shipping Service** (Rust) - Shipping calculations
- **Quote Service** (PHP) - Quote generation
- **Fraud Detection** (Kotlin) - Fraud detection
- **Accounting** (C#) - Accounting operations

Support services include Kafka, Valkey (Redis), Jaeger, Grafana, Prometheus, OpenTelemetry Collector, and load generators.

## Common Commands

### Starting and Stopping the Demo

```bash
# Start full demo with all services
make start

# Start minimal demo with core services only
make start-minimal

# Stop all services
make stop
```

### Building

```bash
# Build all services
make build

# Build multiplatform images
make build-multiplatform

# Build and push images
make build-and-push
```

### Testing

```bash
# Run all tests (frontend + trace-based)
make run-tests

# Run trace-based tests for all services
make run-tracetesting

# Run trace-based tests for specific services
make run-tracetesting SERVICES_TO_TEST="ad cart checkout"
```

### Development

```bash
# Restart a single service
make restart service=frontend

# Rebuild and redeploy a single service
make redeploy service=cart

# Generate protobuf files (requires local tools)
make generate-protobuf

# Generate protobuf files using Docker
make docker-generate-protobuf
```

### Linting and Validation

```bash
# Run all checks (misspell, markdown lint, license check, link check)
make check

# Install development tools
make install-tools

# Run individual checks
make markdownlint
make misspell
make checklicense
make checklinks
make yamllint

# Auto-fix issues where possible
make fix
```

## Service-Specific Details

### Frontend Services

- **frontend/**: Next.js app with TypeScript, uses gRPC for backend communication
- **flagd-ui/**: Next.js feature flag management UI
- **react-native-app/**: React Native mobile app

### Backend Services

Each service in `src/` has its own README with specific build instructions:

- **Go services** (checkout, product-catalog): Use Go modules, Docker builds
- **C# services** (accounting, cart): Use .NET, NuGet packages
- **Java services** (ad, fraud-detection): Use Gradle with wrapper
- **Node.js services** (payment): Use npm with package-lock.json
- **Python services** (recommendation, load-generator): Use requirements.txt
- **Rust services** (shipping): Use Cargo
- **C++ services** (currency): Use CMake
- **PHP services** (quote): Use Composer
- **Ruby services** (email): Use Gemfile

## Environment Configuration

The demo uses Docker Compose with environment files:

- `.env` - Default configuration (created from compose)
- `.env.override` - Local overrides
- `.env.arm64` - ARM64-specific settings for macOS M-series chips

Key endpoints when running:

- Demo UI: http://localhost:8080
- Jaeger UI: http://localhost:8080/jaeger/ui
- Grafana: http://localhost:8080/grafana/
- Load Generator: http://localhost:8080/loadgen/
- Feature Flags: http://localhost:8080/feature/

## Protobuf Generation

The demo uses gRPC with protobuf definitions in `pb/demo.proto`. Generate code with:

- `make docker-generate-protobuf` (recommended, uses Docker)
- `make generate-protobuf` (requires local toolchain)

Generated files are placed in each service's `genproto/` directory.

## Testing Framework

Uses Tracetest for trace-based testing. Tests are defined in `test/tracetesting/` with YAML files that validate:

- Service functionality through API calls
- Generated trace data against expected patterns
- Cross-service communication and data flow

Run `make run-tracetesting` to execute all trace validation tests, or target specific services with the `SERVICES_TO_TEST` parameter.
