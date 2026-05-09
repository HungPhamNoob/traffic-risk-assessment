
---

## ✅ Task 8: Docs Hướng Dẫn Team

### 📄 `docs/onboarding.md`
```markdown
# 🚀 Onboarding Guide cho Team Capstone 4

## 1. Setup Local Development
```bash
# Clone repo
git clone https://github.com/your-org/capstone-team4.git
cd capstone-team4

# Copy env template
cp .env.example .env
# Edit .env với config của bạn

# Cài dependencies
pip install -r requirements.txt

# Start core services (Node 1)
make up

# Verify
curl http://localhost:8080/health  # Airflow
curl http://localhost:3000/api/health  # Grafana