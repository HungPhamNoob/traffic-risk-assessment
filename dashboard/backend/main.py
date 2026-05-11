from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Capstone API", docs_url=None, redoc_url=None)

# CORS cho frontend port 5173
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "healthy", "service": "fastapi"}

@app.get("/")
def root():
    return {"message": "Capstone API is running"}

@app.get("/api/hotspots")
def get_hotspots():
    return []  # Huy sẽ implement sau

@app.post("/api/predict")
def predict(data: dict):
    return {"risk_score": 0.0}  # Huy sẽ implement sau

@app.post("/api/what-if")
def what_if(data: dict):
    return {"baseline": 0.0, "modified": 0.0, "delta": 0.0}  # Huy sẽ implement sau