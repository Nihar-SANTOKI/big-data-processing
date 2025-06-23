# NYC Taxi Data Processing Pipeline 🚕

A comprehensive Big Data processing pipeline using **PySpark**, **Hadoop**, **PostgreSQL**, and **AWS S3** for processing and analyzing NYC taxi trip data.

## 🎯 Project Overview

This project demonstrates enterprise-level data engineering skills by building a complete ETL pipeline that:
- Processes **3+ million** NYC taxi trip records
- Implements **distributed computing** with Spark & Hadoop
- Provides **multi-tier storage** (PostgreSQL, HDFS, S3)
- Includes **comprehensive monitoring** and **data quality validation**

## 🏗️ Architecture

```mermaid
flowchart LR
    A["`**Data Source**
    📊 NYC Open Data
    (Parquet Files)`"] --> B["`**Processing Layer**
    ⚡ PySpark Cluster
    • 3 Workers
    • Jupyter Notebook`"]
    
    B --> C["`**Storage Options**
    💾 Multiple Backends`"]
    
    C --> D["`🐘 **PostgreSQL**
    Relational Database`"]
    
    C --> E["`🗄️ **HDFS**
    Distributed Storage`"]
    
    C --> F["`💿 **Local Storage**
    File System`"]
    
    style A fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style B fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style C fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    style D fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style E fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style F fill:#fff3e0,stroke:#e65100,stroke-width:2px

## 🔧 Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Processing** | PySpark 3.3.0 | Distributed data processing |
| **Storage** | Hadoop HDFS 3.2.1 | Distributed file system |
| **Database** | PostgreSQL | Structured data storage |
| **Orchestration** | Docker Compose | Container management |
| **Monitoring** | Great Expectations | Data quality validation |
| **Analytics** | Jupyter Lab | Interactive analysis |

## 📊 Data Pipeline Features

### 🔍 Data Processing
- **ETL Pipeline**: Extract → Transform → Load with validation
- **Data Cleaning**: Remove outliers, null values, invalid records
- **Feature Engineering**: 10+ derived features (trip duration, distance categories, etc.)
- **Aggregations**: Daily, hourly, and categorical summaries

### 🏪 Storage Strategy
- **Raw Data**: HDFS for fault-tolerant storage
- **Processed Data**: PostgreSQL for analytical queries
- **Analytics**: Local storage for reports and visualizations
- **Backup**: Multi-location redundancy

### 📈 Analytics Outputs
- Daily trip statistics and revenue trends
- Hourly demand patterns
- Weekend vs weekday analysis
- Distance category breakdowns
- Data quality reports with 95%+ accuracy

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 8GB+ RAM recommended
- AlwaysData PostgreSQL instance

### 1. Clone & Setup
```bash
git clone <big-data-processing>
cd big-data-processing

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env
```

### 2. Configure Environment
```bash
# .env file configuration
POSTGRES_HOST=postgresql-your-account.alwaysdata.net
POSTGRES_DB=your_database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

### 3. Launch Infrastructure
```bash
# Start all services
docker-compose up -d

# Verify services are healthy
docker-compose ps

# Check logs if needed
docker-compose logs -f jupyter
```

### 4. Setup HDFS
```bash
# Make script executable
chmod +x scripts/setup_hdfs.sh

# Create HDFS directories in BASH or using GIT BASH
./scripts/setup_hdfs.sh
```

### 5. Run the Pipeline
```bash
# Execute main pipeline
docker exec jupyter-spark python src/main.py

# Monitor progress
docker exec jupyter-spark tail -f logs/app.log
```

## 📱 Access Points

| Service | URL | Purpose |
|---------|-----|---------|
| **Jupyter Lab** | http://localhost:8888 | Interactive analysis |
| **Spark Master** | http://localhost:8080 | Cluster monitoring |
| **Spark Worker 1** | http://localhost:8081 | Worker status |
| **Spark Worker 2** | http://localhost:8082 | Worker status |
| **Hadoop NameNode** | http://localhost:9870 | HDFS management |
| **Hadoop DataNode** | http://localhost:9864 | Storage status |

## 📊 Expected Results

### Database Tables
```sql
-- PostgreSQL tables created:
taxi_trips_processed    -- 300K+ processed trip records
daily_trip_stats       -- Daily aggregations
taxi_trips_raw         -- Raw data backup
```

### HDFS Storage
```
/user/data/taxi/
├── raw/           -- Original parquet files
├── processed/     -- Cleaned & enriched data
└── analytics/     -- Aggregated results
```

### S3 Artifacts
```
local-storage/
├── analytics/     -- Daily & hourly reports
├── reports/       -- Comprehensive analytics
└── quality/       -- Data quality metrics
```

### Key Metrics
- **Data Volume**: 3+ million records processed
- **Data Quality**: 95%+ accuracy score
- **Processing Speed**: 100K+ records/minute
- **Storage**: Multi-tier with redundancy

## 🔍 Monitoring & Validation

### Data Quality Checks
- ✅ Schema validation
- ✅ Null value detection
- ✅ Outlier identification
- ✅ Business rule validation
- ✅ Duplicate detection

### System Monitoring
```bash
# Check system resources
docker exec jupyter-spark python -c "
from src.monitoring.pipeline_monitor import PipelineMonitor
monitor = PipelineMonitor()
monitor.log_system_metrics()
"
```

## 🧪 Testing

```bash
# Run unit tests
docker exec jupyter-spark python -m pytest tests/ -v

# Run integration tests
docker exec jupyter-spark python -m pytest tests/test_integration.py -v
```

## 📈 Analytics Examples

### Daily Trends Analysis
```python
# In Jupyter notebook
daily_stats = postgres_manager.fetch_data("""
    SELECT trip_date, total_trips, total_revenue
    FROM daily_trip_stats
    ORDER BY trip_date
""")

daily_stats.plot(x='trip_date', y=['total_trips', 'total_revenue'])
```

### Hourly Patterns
```python
hourly_data = postgres_manager.fetch_data("""
    SELECT hour_of_day, COUNT(*) as trips
    FROM taxi_trips_processed
    GROUP BY hour_of_day
    ORDER BY hour_of_day
""")
```

## 🐛 Troubleshooting

### Common Issues

**1. Memory Issues**
```bash
# Reduce memory allocation in .env
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=1g
```

**2. HDFS Connection Failed**
```bash
# Restart Hadoop services
docker-compose restart namenode datanode
./scripts/setup_hdfs.sh
```

**3. Database Connection Issues**
```bash
# Test PostgreSQL connection
docker exec jupyter-spark python -c "
from src.storage.postgres_manager import PostgreSQLManager
pg = PostgreSQLManager()
print('Connection successful!' if pg.fetch_data('SELECT 1') is not None else 'Connection failed!')
"
```

**4. S3 Permissions**
```bash
# Verify AWS credentials
docker exec jupyter-spark python -c "
from src.storage.s3_manager import S3Manager
s3 = S3Manager()
print(s3.list_objects())
"
```

This project demonstrates:

**Technical Skills:**
- Distributed computing with Spark
- Big data storage with Hadoop HDFS
- Database management (PostgreSQL)
- Container orchestration (Docker)
- Data quality validation
- ETL pipeline development

**Best Practices:**
- Modular, maintainable code
- Comprehensive error handling
- Logging and monitoring
- Testing framework
- Configuration management
- Documentation

**Business Value:**
- Scalable data processing
- Cost-effective storage strategy
- Real-time monitoring
- Data quality assurance
- Actionable analytics

## 📚 Learning Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Hadoop HDFS Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [NYC Taxi Data Schema](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Authout

Nihar SANTOKI