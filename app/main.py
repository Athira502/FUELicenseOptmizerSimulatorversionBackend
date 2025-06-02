from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import data_loader_router, client_sys_release_version_router, license_optimizer_router, logs_router, \
    user_group_router

app = FastAPI(
    title="SAP License Optimizer API",
    description="API to load SAP role data per client and analyze for license optimization.",
    version="1.0.0"
)
origins = [
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_security_headers(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["X-Frame-Options"] = "DENY"
    return response


app.include_router(data_loader_router.router)
app.include_router(client_sys_release_version_router.router)
app.include_router(license_optimizer_router.router)
app.include_router(logs_router.router)
app.include_router(user_group_router.router)

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to the SAP License Optimizer API"}


