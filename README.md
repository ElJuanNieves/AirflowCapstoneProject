# Airflow Data Pipeline with Vault Integration

This project demonstrates a comprehensive Apache Airflow setup with HashiCorp Vault integration for secret management, featuring trigger-based DAG execution and Slack notifications.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   File Trigger  │───▶│   Trigger DAG    │───▶│   Jobs DAG      │
│   (FileSensor)  │    │  (trigger_dag)   │    │  (dag_id_1/2/3) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Slack Notification │◀─│ Cleanup & Results│◀───│ Database Insert │
│   (slack-sdk)   │    │   Processing     │    │  (PostgreSQL)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │ HashiCorp Vault  │
                       │ (Secret Storage) │
                       └──────────────────┘
```

### Components

1. **Jobs DAG** (`jobs_dag.py`): Database processing pipeline
2. **Trigger DAG** (`trigger_dag.py`): File-based trigger system with monitoring
3. **Packaged Trigger DAG** (`packaged_trigger_dag/`): Self-contained version with slack-sdk dependencies
4. **HashiCorp Vault**: Secure secret management
5. **PostgreSQL**: Data storage backend
6. **Slack Integration**: Workflow notifications

## 📦 DAG Descriptions

### Jobs DAG (jobs_dag.py)
**Purpose**: Simulates a data processing pipeline

**Features**:
- Dynamic DAG creation for multiple instances (`dag_id_1`, `dag_id_2`, `dag_id_3`)
- Conditional table creation based on existence checks
- Random data generation and insertion
- PostgreSQL database integration
- XCom-based result passing

**Workflow**:
1. Print execution information
2. Get current system user
3. Check if target table exists
4. Create table (if needed) or proceed to insertion
5. Prepare random data
6. Insert data into PostgreSQL
7. Return completion message via XCom

### Trigger DAG (trigger_dag.py)
**Purpose**: File-based trigger system with comprehensive monitoring

**Features**:
- File-based triggering using FileSensor
- External DAG triggering and monitoring
- XCom-based synchronization
- Multi-tier secret management (Vault/Variables/Environment)
- Slack notifications with error handling
- Automated cleanup operations

**Workflow**:
1. **File Monitoring**: Wait for trigger file
2. **DAG Triggering**: Launch target DAG (dag_id_1)
3. **Synchronization**: Store execution dates for proper monitoring
4. **Completion Monitoring**: Wait for target DAG completion
5. **Result Processing**: Retrieve and log results
6. **Cleanup**: Remove trigger files and create completion markers
7. **Notification**: Send Slack alerts

### Packaged Trigger DAG
**Purpose**: Self-contained DAG with embedded dependencies

**Features**:
- Includes `slack-sdk` and dependencies in the same directory
- No external Python package installation required
- Identical functionality to the main trigger DAG
- Useful for environments with restricted package installation

## 🔧 Setup Instructions

### Prerequisites
- Docker & Docker Compose
- Git

### 1. Initial Setup

```bash
# Clone the repository
git clone <repository-url>
cd airflow

# Copy environment configuration
cp .env.example .env

# Edit .env with your actual values
# - Set your AIRFLOW_UID (run: id -u)
# - Configure PostgreSQL credentials
# - Add your Slack Bot Token
# - Set Vault root token
```

### 2. Start Services

```bash
# Start all services
docker-compose up -d

# Check service health
docker-compose ps
```

### 3. Vault Configuration

After docker-compose has started, configure Vault for secret storage:

```bash
# Access Vault container
docker exec -it airflow-vault-1 sh

# Login to Vault using the root token from your .env file
vault login YOUR_VAULT_ROOT_TOKEN

# Enable KV secrets engine v2 for Airflow
vault secrets enable -path=airflow -version=2 kv

# Add your Slack token to Vault
vault kv put airflow/variables/slack_token value=YOUR_SLACK_TOKEN

# Verify the secret was stored
vault kv get airflow/variables/slack_token

# Exit container
exit
```

### 4. Airflow Configuration

Access Airflow Web UI at `http://localhost:8080` (admin/airflow)

**Required Connections**:
1. **PostgreSQL Connection** (`postgress_default`):
   - Host: `postgres`
   - Database: `airflow`
   - Username: `airflow`
   - Password: `my_airflow` (or your configured password)

2. **Vault Connection** (`vault_default`) - Optional:
   - Connection Type: `HashiCorp Vault`
   - Host: `vault`
   - Port: `8200`
   - Extra: `{"token": "YOUR_VAULT_ROOT_TOKEN", "url": "http://vault:8200"}`

**Required Variables**:
- `trigger_path`: Path to trigger file (e.g., `/opt/airflow/data/trigger_file.txt`)

## 🚀 Usage

### Running the Pipeline

1. **Create trigger file**:
```bash
# Create trigger file to start the workflow
docker exec -it airflow-airflow-apiserver-1 touch /opt/airflow/data/trigger_file.txt
```

2. **Monitor execution**:
   - Access Airflow UI at `http://localhost:8080`
   - Navigate to `trigger_dag` and monitor execution
   - Check logs for detailed execution information

3. **Verify results**:
   - Check Slack for completion notification
   - Verify database insertions in PostgreSQL
   - Review logs for any errors

### Secret Management Options

The system supports multiple secret retrieval methods in order of preference:

1. **Airflow Variables** (Recommended for simplicity):
```bash
docker exec -it airflow-airflow-apiserver-1 airflow variables set slack_token "YOUR_SLACK_TOKEN"
```

2. **Environment Variables** (Automatic with .env file):
```bash
# Already configured via docker-compose env_file
echo $SLACK_TOKEN
```

3. **HashiCorp Vault** (Most secure):
```bash
# Configured during Vault setup step
vault kv get airflow/variables/slack_token
```

## 🔒 Security Considerations

### Sensitive Information Handling
- ✅ Secrets stored in Vault or environment variables
- ✅ No hardcoded tokens or passwords in code
- ✅ `.env` file excluded from version control
- ✅ `.env.example` provided for reference
- ✅ Vault root token externalized to environment

### Best Practices Implemented
- Multiple fallback methods for secret retrieval
- Error handling prevents DAG failures from notification issues
- Development tokens clearly marked in configuration
- Comprehensive logging for debugging and monitoring

## 📁 Project Structure

```
.
├── dags/
│   ├── jobs_dag.py              # Main data processing DAGs
│   └── trigger_dag.py           # File-based trigger system
├── packaged_trigger_dag/
│   ├── trigger_dag.py           # Self-contained trigger DAG
│   ├── slack/                   # Embedded Slack library
│   └── slack_sdk/               # Embedded Slack SDK
├── config/
│   └── airflow.cfg              # Airflow configuration
├── data/
│   ├── trigger_file.txt         # Trigger file for testing
│   └── test.txt                 # Sample data
├── logs/                        # Airflow execution logs
├── plugins/                     # Custom Airflow plugins
├── docker-compose.yaml          # Docker services configuration
├── .env.example                 # Environment template
├── .gitignore                   # Git ignore rules
└── README.md                    # This file
```

## 🔧 Configuration Details

### Airflow Configuration
- **Executor**: CeleryExecutor (scalable)
- **Backend**: PostgreSQL
- **Message Broker**: Redis
- **Secret Backend**: HashiCorp Vault (optional)

### Vault Integration
- **Mount Point**: `airflow`
- **Variables Path**: `variables`
- **Full Path Format**: `airflow/variables/{variable_name}`

### Docker Services
- **airflow-apiserver**: Web UI and API (port 8080)
- **airflow-scheduler**: DAG scheduling
- **airflow-worker**: Task execution
- **postgres**: Database backend (port 5432)
- **redis**: Message broker
- **vault**: Secret management (port 8200)

## 🐛 Troubleshooting

### Common Issues

1. **DAG Import Errors**:
```bash
# Check DAG parsing errors
docker exec -it airflow-airflow-apiserver-1 airflow dags list-import-errors
```

2. **Connection Issues**:
```bash
# Test database connection
docker exec -it airflow-airflow-apiserver-1 airflow connections test postgress_default
```

3. **Vault Access Issues**:
```bash
# Verify Vault status
docker exec -it airflow-vault-1 vault status
```

4. **Slack Integration Issues**:
```bash
# Check environment variables
docker exec -it airflow-airflow-apiserver-1 env | grep SLACK
```

### Logs Location
- **Airflow Logs**: `./logs/`
- **Docker Logs**: `docker-compose logs [service_name]`

## 🚧 Development Notes

### Adding New DAGs
1. Place DAG files in `./dags/` directory
2. Restart scheduler: `docker-compose restart airflow-scheduler`
3. Check DAG appears in UI within 30 seconds

### Adding New Secrets
```bash
# Add to Vault
vault kv put airflow/variables/new_secret value="secret_value"

# Or use Airflow Variables
airflow variables set new_secret "secret_value"
```

### Package Management
- For adding Python packages, update `_PIP_ADDITIONAL_REQUIREMENTS` in docker-compose.yaml
- For production, build custom Docker image instead

## 📚 Dependencies

### Core Components
- Apache Airflow 3.0.2
- PostgreSQL 13
- Redis 7.2
- HashiCorp Vault (latest)

### Python Packages
- `slack-sdk`: Slack API integration
- `psycopg2`: PostgreSQL adapter
- Standard Airflow providers

## 🤝 Contributing

1. Follow existing code style and documentation standards
2. Add comprehensive comments for new functionality
3. Update README for significant changes
4. Test thoroughly with different secret management configurations

## 📄 License

This project is provided as-is for educational and demonstration purposes.
