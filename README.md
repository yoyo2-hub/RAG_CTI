# Multi-Agent CTI with RAG

## 🎯 Overview

Multi-Agent CTI with RAG is a complete system that combines **Retrieval-Augmented Generation (RAG)** with **multi-agent architecture** to provide intelligent Cyber Threat Intelligence analysis and insights. This system leverages LLMs and knowledge retrieval to deliver context-aware threat assessments and recommendations.

## ✨ Key Features

- **Multi-Agent Architecture**: Coordinated AI agents working together for comprehensive threat analysis
- **RAG Integration**: Retrieval-Augmented Generation for accurate, knowledge-grounded threat intelligence
- **Threat Analysis**: Automated analysis of security threats and vulnerabilities
- **Knowledge Base**: Integrated retrieval system for threat data and intelligence
- **Scalable Design**: Modular architecture for easy extension and customization

## 🏗️ Architecture

The system is built on a multi-agent framework with the following components:

- **Query Agent**: Processes and understands threat intelligence queries
- **Analysis Agent**: Performs in-depth analysis of threats and vulnerabilities
- **Retrieval Agent**: Fetches relevant information from the knowledge base
- **Synthesis Agent**: Consolidates findings and generates recommendations

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Required dependencies (see `requirements.txt`)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yoyo2-hub/Multi_Agent_CTI_with_RAG.git
cd Multi_Agent_CTI_with_RAG
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your environment:
```bash
cp .env.example .env
# Edit .env with your configuration
```

### Configuration

Update the configuration files with:
- API keys and credentials
- Knowledge base paths
- Model parameters
- Agent settings

## 📊 Usage

### Basic Example

```python
from multi_agent_cti import CTISystem

# Initialize the CTI system
cti = CTISystem()

# Query for threat intelligence
threat_analysis = cti.analyze_threat("CVE-2024-1234")
print(threat_analysis)
```

### Advanced Usage

```python
# Custom query with specific agents
results = cti.query(
    query="Analyze recent ransomware trends",
    agents=["analysis", "retrieval", "synthesis"],
    context="financial_sector"
)
```

## 📁 Project Structure

```
Multi_Agent_CTI_with_RAG/
├── src/
│   ├── agents/           # Multi-agent implementations
│   ├── rag/              # RAG components
│   ├── models/           # LLM integrations
│   └── utils/            # Utility functions
├── data/                 # Knowledge base and data
├── configs/              # Configuration files
├── tests/                # Unit and integration tests
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

## 🔧 Components

### Agents
- Query understanding and processing
- Threat analysis and correlation
- Knowledge retrieval
- Report generation and synthesis

### RAG System
- Vector store for semantic search
- Document processing and embedding
- Retrieval pipeline
- Context augmentation

### Knowledge Base
- Threat intelligence feeds
- Vulnerability databases
- Security advisories
- Historical threat data

## 🧪 Testing

Run the test suite:

```bash
pytest tests/
```

Run specific test:
```bash
pytest tests/test_agents.py -v
```

## 🔗 Related Resources

- [Retrieval-Augmented Generation (RAG)](https://arxiv.org/abs/2005.11401)
- [Multi-Agent Systems](https://en.wikipedia.org/wiki/Multi-agent_system)
- [Cyber Threat Intelligence](https://www.mitre.org/about/cybersecurity-center)

## 👥 Contributors
- Chayma Dallel
- Emna Ghorbel
- Ranim Bouguila

