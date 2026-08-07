from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from generator import MockDataGenerator

app = FastAPI(
    title="Mock External NOC Telemetry API",
    description="Simulates live external monitoring systems providing Alarms, Incident Tickets, Network Events, Security Threats, and Performance Metrics.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {"status": "HEALTHY", "service": "mock-external-api"}


@app.get("/api/v1/alarms")
async def get_alarms(count: int = Query(default=50, ge=1, le=500)):
    records = MockDataGenerator.generate_alarms(count=count)
    return {"success": True, "count": len(records), "data": records}


@app.get("/api/v1/tickets")
async def get_tickets(count: int = Query(default=40, ge=1, le=500)):
    records = MockDataGenerator.generate_tickets(count=count)
    return {"success": True, "count": len(records), "data": records}


@app.get("/api/v1/network-events")
async def get_network_events(count: int = Query(default=50, ge=1, le=500)):
    records = MockDataGenerator.generate_network_events(count=count)
    return {"success": True, "count": len(records), "data": records}


@app.get("/api/v1/security-events")
async def get_security_events(count: int = Query(default=35, ge=1, le=500)):
    records = MockDataGenerator.generate_security_events(count=count)
    return {"success": True, "count": len(records), "data": records}


@app.get("/api/v1/performance")
async def get_performance(count: int = Query(default=60, ge=1, le=500)):
    records = MockDataGenerator.generate_performance(count=count)
    return {"success": True, "count": len(records), "data": records}


@app.get("/api/v1/sites")
async def get_sites(count: int = Query(default=25, ge=1, le=100)):
    records = MockDataGenerator.generate_sites(count=count)
    return {"success": True, "count": len(records), "data": records}


@app.get("/api/v1/devices")
async def get_devices(count: int = Query(default=40, ge=1, le=200)):
    records = MockDataGenerator.generate_devices(count=count)
    return {"success": True, "count": len(records), "data": records}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
