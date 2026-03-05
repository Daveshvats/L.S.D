<div align="center">

# 🔍 L.S.D

**Large Search of Data**

*A production-grade, high-performance API that automatically adapts to any PostgreSQL database schema*

[![GitHub stars](https://img.shields.io/github/stars/Daveshvats/L.S.D?style=for-the-badge&logo=github&color=yellow)](https://github.com/Daveshvats/L.S.D/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/Daveshvats/L.S.D?style=for-the-badge&logo=github&color=blue)](https://github.com/Daveshvats/L.S.D/fork)
[![GitHub issues](https://img.shields.io/github/issues/Daveshvats/L.S.D?style=for-the-badge&logo=github&color=red)](https://github.com/Daveshvats/L.S.D/issues)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=for-the-badge&logo=go)](https://go.dev/)
[![License](https://img.shields.io/github/license/Daveshvats/L.S.D?style=for-the-badge&color=green)](LICENSE)
[![Last Commit](https://img.shields.io/github/last-commit/Daveshvats/L.S.D?style=for-the-badge&color=orange)](https://github.com/Daveshvats/L.S.D/commits/main)

[🚀 Quick Start](#-quick-start) · [📖 Documentation](docs/architecture.md) · [🎮 Demo](#-use-cases--real-world-scenarios) · [🤝 Contributing](#-contributing)

</div>

---

## 🎯 What is L.S.D?

**L.S.D (Large Search of Data)** is a dynamic, schema-agnostic Go API that transforms any PostgreSQL database into a fully-functional REST API with blazing-fast search capabilities. Designed for massive datasets (2-4 TB), it auto-discovers your database schema at runtime and provides optimized endpoints for data access, search, and management—without writing a single line of backend code.

Whether you're building a web GUI, Telegram bot, WhatsApp integration, or need sub-second search across billions of rows, L.S.D has you covered with automatic ClickHouse integration and intelligent caching.

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| 🔮 **Dynamic Schema Discovery** | Auto-discovers tables, columns, primary keys, and indexes at startup—no manual configuration needed |
| ⚡ **Keyset Pagination** | O(1) cursor-based pagination that stays fast regardless of dataset size |
| 🚄 **ClickHouse Integration** | Sub-second multi-column text search on billions of rows using ngram indexes |
| 🔄 **CDC Pipeline** | Automatic PostgreSQL → ClickHouse synchronization with delete handling |
| 📁 **File Loading Pipeline** | Load CSV, XLSX, XLS, JSON files with auto-detection of headers, delimiters, and encodings |
| 📦 **Redis Caching** | Built-in caching layer with configurable TTL for lightning-fast responses |
| 🔐 **Authentication** | JWT-based auth with API key support for AI agents and integrations |
| 📊 **Rate Limiting** | Configurable per-client rate limiting (100-5000 req/min) |
| 🤖 **Bot Support** | Ready-to-use Telegram and WhatsApp webhook handlers |
| 🌐 **Swagger/OpenAPI** | Complete API documentation with interactive UI |
| 🎨 **Web Dashboard** | Beautiful, responsive admin dashboard included |

## 🏗️ High-Level Architecture

```mermaid
graph TB
    subgraph Clients
        WEB[Web Dashboard]
        TG[Telegram Bot]
        WA[WhatsApp Bot]
        API[API Consumers]
    end

    subgraph "L.S.D API Server"
        AUTH[Auth Middleware]
        RATE[Rate Limiter]
        CACHE[Redis Cache]
        SCHEMA[Schema Discovery]
        HANDLERS[Dynamic Handlers]
        SEARCH[Search Engine]
    end

    subgraph Datastores
        PG[(PostgreSQL<br/>Primary DB)]
        CH[(ClickHouse<br/>Search Index)]
        REDIS[(Redis<br/>Cache)]
    end

    WEB --> AUTH
    TG --> AUTH
    WA --> AUTH
    API --> AUTH

    AUTH --> RATE
    RATE --> HANDLERS
    HANDLERS --> SCHEMA
    HANDLERS --> CACHE
    HANDLERS --> SEARCH

    SCHEMA --> PG
    CACHE --> REDIS
    SEARCH --> CH
    SEARCH -.->|Fallback| PG

    PG -.->|CDC Sync| CH

    style PG fill:#336791,color:#fff
    style CH fill:#ffcc00,color:#000
    style REDIS fill:#dc382d,color:#fff
```

## 🚀 Quick Start

### Prerequisites

- **Go 1.24+** installed
- **PostgreSQL 15+** running
- **Redis** (optional, for caching)
- **ClickHouse** (optional, for accelerated search)

### Installation

```bash
# Clone the repository
git clone https://github.com/Daveshvats/L.S.D.git
cd L.S.D

# Copy environment configuration
cp .env.example .env

# Edit .env with your database credentials
# DATABASE_URL=postgresql://user:password@localhost:5432/your_db
```

### Run the Server

```bash
# Start Redis (if using caching)
redis-server --daemonize yes --port 6379

# Run the API server
go run ./cmd/api
```

The API will be available at `http://localhost:5000`

### First API Call

```bash
# List all discovered tables
curl http://localhost:5000/api/tables

# Get records from a table
curl "http://localhost:5000/api/tables/your_table/records?limit=10"

# Search across a table
curl "http://localhost:5000/api/tables/your_table/search?q=search_term"
```

## 📊 API Endpoints Overview

### Dynamic REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/tables` | GET | List all discovered tables |
| `/api/tables/{table}/schema` | GET | Get table schema and columns |
| `/api/tables/{table}/records` | GET | List records with cursor pagination |
| `/api/tables/{table}/records/{pk}` | GET | Get single record by primary key |
| `/api/tables/{table}/search` | GET | Multi-column text search |
| `/api/tables/{table}/stats` | GET | Get table statistics |
| `/api/cdc/status` | GET | CDC pipeline sync status |
| `/api/health` | GET | Health check endpoint |

### Pipeline & File Loading

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/pipeline/scan-folder` | POST | Scan folder for files (quick or deep scan) |
| `/api/pipeline/jobs` | GET | List all pipeline jobs |
| `/api/pipeline/jobs` | POST | Start job with preview mode (dry_run) |
| `/api/pipeline/jobs/{id}` | GET | Get job progress/status |
| `/api/pipeline/jobs/{id}/stream` | GET | SSE stream for real-time progress |
| `/api/pipeline/jobs/{id}/logs` | GET | Get job logs |
| `/api/pipeline/preview` | POST | Preview file before loading |
| `/api/pipeline/upload` | POST | Upload and process file |
| `/api/pipeline/load` | POST | Load file from server path |
| `/api/pipeline/configs` | GET/POST | Manage saved configurations |

**Supported File Types:** CSV, XLSX, **XLS** (Excel 97-2003), JSON, JSONL

#### Recommended Pipeline Workflow

```bash
# 1. Scan folder with deep analysis
curl -X POST http://localhost:5000/api/pipeline/scan-folder \
  -H "Content-Type: application/json" \
  -d '{"folder_path": "/data/uploads", "deep_scan": true}'

# 2. Preview what will happen (dry run)
curl -X POST http://localhost:5000/api/pipeline/jobs \
  -H "Content-Type: application/json" \
  -d '{"folder_path": "/data/uploads", "dry_run": true}'

# 3. Start the job
curl -X POST http://localhost:5000/api/pipeline/jobs \
  -H "Content-Type: application/json" \
  -d '{"folder_path": "/data/uploads", "quick_options": {"has_header": true}}'

# 4. Monitor progress
curl http://localhost:5000/api/pipeline/jobs/{job_id}
```

### Authentication

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/auth/register` | POST | Register new user |
| `/api/auth/login` | POST | Login and get JWT token |
| `/api/auth/refresh` | POST | Refresh access token |
| `/api/auth/logout` | POST | Logout and revoke session |
| `/api/api-keys` | GET/POST | Manage API keys |

### Webhooks

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/webhook/telegram` | POST | Telegram bot webhook |
| `/webhook/whatsapp` | GET/POST | WhatsApp bot webhook |

## 🎮 Use Cases & Real-World Scenarios

### 1. Enterprise Data Portal
Expose legacy PostgreSQL databases to modern web applications without writing custom APIs. L.S.D automatically handles schema changes—just add new tables or columns, and they're instantly available.

### 2. AI Agent Integration
Use API keys with scoped permissions to let AI agents search and retrieve data safely:
```bash
curl -H "X-API-Key: lsd_live_your_key" \
  "http://localhost:5000/api/tables/customers/search?q=john"
```

### 3. Real-Time Analytics Dashboard
Combine PostgreSQL's transactional integrity with ClickHouse's analytical speed. The CDC pipeline keeps search indexes fresh every 30 seconds.

### 4. Multi-Platform Bot Backend
Serve Telegram and WhatsApp bots from the same API. Built-in command handlers for `/list`, `/search`, `/get`, and `/stats`.

### 5. Data Migration Tooling
Use the dynamic API to extract data from any PostgreSQL database without writing migration scripts—perfect for ETL pipelines and data synchronization tasks.

## 📖 Documentation

| Document | Description |
|----------|-------------|
| [Architecture Guide](docs/architecture.md) | Deep dive into system design and components |
| [Setup Guide](docs/setup.md) | Detailed installation instructions |
| [Configuration](docs/configuration.md) | Environment variables and options |
| [Development Guide](docs/development.md) | Contributing and local development |
| [Deployment Guide](docs/deployment.md) | Production deployment strategies |
| [FAQ](docs/faq.md) | Frequently asked questions |

## 🛠️ Tech Stack

<p align="center">
  <img src="https://img.shields.io/badge/Go-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go"/>
  <img src="https://img.shields.io/badge/PostgreSQL-336791?style=flat-square&logo=postgresql&logoColor=white" alt="PostgreSQL"/>
  <img src="https://img.shields.io/badge/ClickHouse-FFCC00?style=flat-square&logo=clickhouse&logoColor=black" alt="ClickHouse"/>
  <img src="https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white" alt="Redis"/>
  <img src="https://img.shields.io/badge/JavaScript-F7DF1E?style=flat-square&logo=javascript&logoColor=black" alt="JavaScript"/>
  <img src="https://img.shields.io/badge/OpenAPI-6BA539?style=flat-square&logo=swagger&logoColor=white" alt="OpenAPI"/>
</p>

| Component | Technology | Purpose |
|-----------|------------|---------|
| Backend | Go 1.24+ | High-performance API server |
| Database | PostgreSQL 15+ | Primary data store |
| Search Engine | ClickHouse | Full-text search acceleration |
| Cache | Redis | Response caching layer |
| Frontend | Vanilla JS + HTML | Web dashboard |
| API Docs | OpenAPI 3.1 | Interactive documentation |

## 🗺️ Roadmap

- [x] Dynamic schema discovery
- [x] Cursor-based pagination
- [x] ClickHouse integration
- [x] CDC pipeline
- [x] Authentication system
- [x] API key management
- [ ] GraphQL endpoint
- [ ] WebSocket real-time updates
- [ ] Admin panel enhancements
- [ ] Kubernetes Helm charts
- [ ] Multi-database support
- [ ] Query builder UI

## 🤝 Contributing

We welcome contributions! Here's how to get started:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

Please read our [Development Guide](docs/development.md) for coding standards and best practices.

### Contributors

<a href="https://github.com/Daveshvats/L.S.D/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Daveshvats/L.S.D" />
</a>

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 💬 Community & Support

- **Issues**: [GitHub Issues](https://github.com/Daveshvats/L.S.D/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Daveshvats/L.S.D/discussions)

---

<div align="center">

**Made with ❤️ by the L.S.D Team**

*Star ⭐ this repo if you find it useful!*

[⬆ Back to Top](#-lsd)

</div>
