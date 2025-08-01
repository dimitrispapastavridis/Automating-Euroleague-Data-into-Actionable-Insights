# 🏀 EuroLeague Data Engineering Project

This is an end-to-end data engineering project that collects, stores, and visualizes EuroLeague basketball data. The project features player and team performance KPIs, attendance analysis, and standings — powered by a full data pipeline using Python, PostgreSQL, Docker, Airflow, and Metabase.

## 📊 Features

- Player performance dashboards (avg points, rebounds, assists)
- Team statistics: total wins, avg efficiency, shooting %
- Arena attendance analytics (Attendance refers to tickets sold, not total stadium capacity)
- Phase-based filtering: Regular Season, Playoffs, Final Four
- Dynamic Metabase filters (season, player, team, phase)

## ⚙️ Tech Stack

| Tool         | Usage                            |
|--------------|-----------------------------------|
| **Python**   | Web scraping & data ingestion     |
| **PostgreSQL**| Relational data storage          |
| **Docker**   | Containerized project setup       |
| **Airflow**  | DAG scheduling for ETL            |
| **Metabase** | Interactive dashboard layer       |
| **GitLab**   | Code hosting                      |


## 🚀 Getting Started

### 🔧 Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Git

### 📦 Installation

1. **Clone the repository**
   ```bash
   git clone github url
   cd euroleague-dashboard
   ```
2. **Start the container**
   ```bash
   docker-compose up -d
   ```
3. **Access the services**
   ```bash
   Metabase: http://4.231.121.38:3000/public/dashboard/49d445a8-fe4d-4c0c-9eb8-1dbc3948a2fe
   ```


### 🤝 Contributing
Pull requests are welcome. 

Please open an issue first to discuss what you’d like to add.

### 👤 Contributors
- **Dimitris Papastavridis**
- **Dimitris Kotsiras**