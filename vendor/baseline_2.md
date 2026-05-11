# Baseline 2 - Smart Home Energy Optimization

## Scope

Baseline 2 is a smart-home energy optimization system. It simulates IoT household data, trains machine learning models for energy recommendation, streams realtime records through Kafka, and serves suggestions through a Flask web UI.

## Architecture

| Layer | Components | Role |
|---|---|---|
| Data generation | `logic_gen_data.py` | Generates synthetic smart-home behavior records. |
| Model training | LightGBM, XGBoost, CatBoost, Random Forest, Decision Tree | Compares predictive models for energy usage and suggestions. |
| Streaming | Kafka producer/consumer | Sends simulated realtime household records and receives suggestions. |
| Application | Flask, HTML, JavaScript, Chart.js, SSE | Displays suggestions and energy analytics to users. |
| Testing | Unit-style scripts | Sends individual records and validates suggestion logic. |

## Workflow

1. Generate or load yearly household energy data.
2. Train an initial model on historical data.
3. Simulate realtime household records.
4. Send realtime records to Kafka.
5. Consumer/model service produces energy usage suggestions.
6. Flask UI displays charts and recommendations.

## Data and Output

The baseline uses generated smart-home data rather than a public national-scale dataset. Outputs are user-facing recommendations and charts, not geospatial risk predictions. Kafka uses multiple topics for realtime data, suggestions, test data, and test suggestions.

## Comparison With This Traffic Project

| Area | Baseline 2 | Traffic Risk Project |
|---|---|---|
| Domain | Smart-home energy recommendations | Traffic accident severity/risk prediction |
| Dataset | Synthetic household data | US Accidents dataset |
| Kafka design | One broker, multiple topics | Three brokers, one topic, three producers |
| Model stack | LightGBM, XGBoost, CatBoost, tree models | H2O AutoML with MLflow registry |
| Serving | Flask UI and suggestion scripts | FastAPI backend for dashboard/product APIs |
| Realtime simulation | Household readings every fixed interval | Post-2020 accident replay |
| Training split | Yearly data windows | Before-2020 pretraining, after-2020 replay/retraining |
| Monitoring | UI-level status and tests | Prometheus, Grafana, system status API |

## Lessons Adopted

- Realtime simulation should be deterministic and testable.
- A clear test-record path is valuable for scenario simulation.
- Model comparison should be explicit, not hidden inside training code.
- UI/backend endpoints should expose both prediction output and explanation-friendly fields.

## Key Difference

Baseline 2 is application-centric and simpler operationally. This traffic project is infrastructure-centric: it must prove multi-node data movement, streaming/batch consistency, model registry usage, and monitoring under a stricter Big Data architecture.
