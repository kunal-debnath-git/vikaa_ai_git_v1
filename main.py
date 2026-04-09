# main.py

import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Load environment variables once at startup
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import routers with standardized naming
from backend.routes.contact_router import router as contact_router
from backend.routes.auth_router import router as auth_router
from backend.routes.agent_router import router as agent_router
from backend.routes.api_protected_router import router as protected_router
from backend.routes.web_research_router import router as web_research_router
from backend.routes.gmail_router import router as gmail_router
from backend.routes.ai_scout_router import router as ai_scout_router
from backend.routes.tool_docs_router import router as tool_docs_router
from backend.routes.unity_catalog_router import (
    router as unity_catalog_router,
    router_api_alias as unity_catalog_api_alias,
    router_root as unity_catalog_root,
)
from backend.routes.catalog_search_router import (
    router as catalog_search_router,
    router_api_alias as catalog_search_api_alias,
    router_root as catalog_search_root,
)
from backend.routes.databricks_query_router import (
    router as databricks_query_router,
    router_api_alias as databricks_query_api_alias,
    router_root as databricks_query_root,
)
from backend.routes.databricks_synth_data_router import (
    router as databricks_synth_data_router,
    router_api_alias as databricks_synth_data_api_alias,
    router_root as databricks_synth_data_root,
)
from backend.routing.tool_mounts import include_router_triplet
from backend.routes.rag_configurator_router import (
    router as rag_configurator_router,
    router_api_alias as rag_configurator_api_alias,
    router_root as rag_configurator_root,
)
# from backend.routes.config_rag_router import router as config_rag_router
# from backend.routes.flashcard_router import router as flashcard_router
# from backend.routes.lead_router import router as lead_router
# from backend.routes.chat_with_files_router import router as chat_with_files_router
# from backend.routes.twitter_router import router as twitter_router

app = FastAPI(title="Vikaa AI Agent API")


@app.on_event("startup")
def _log_critical_tool_routes() -> None:
    for route in app.routes:
        p = getattr(route, "path", None)
        if p and (
            "catalog-search" in p
            or "databricks-query" in p
            or "databricks-synth-data" in p
            or "rag-configurator" in p
        ):
            logger.info("Tool route registered: %s", p)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://vikaa.ai",
        "https://www.vikaa.ai",
        "https://app-wtiw.onrender.com",   # Render production
        "http://localhost:3000",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:10000",
        "http://127.0.0.1:10000",
        "http://localhost:5500",            # Live Server
        "http://127.0.0.1:5500",
        "null",                             # file:// opened locally
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(contact_router)
app.include_router(auth_router)
app.include_router(agent_router)
app.include_router(protected_router)
app.include_router(web_research_router)
app.include_router(gmail_router)
app.include_router(ai_scout_router)
app.include_router(tool_docs_router)
include_router_triplet(
    app,
    (unity_catalog_router, unity_catalog_api_alias, unity_catalog_root),
)
include_router_triplet(
    app,
    (catalog_search_router, catalog_search_api_alias, catalog_search_root),
)
include_router_triplet(
    app,
    (
        databricks_query_router,
        databricks_query_api_alias,
        databricks_query_root,
    ),
)
include_router_triplet(
    app,
    (
        databricks_synth_data_router,
        databricks_synth_data_api_alias,
        databricks_synth_data_root,
    ),
)
include_router_triplet(
    app,
    (
        rag_configurator_router,
        rag_configurator_api_alias,
        rag_configurator_root,
    ),
)
# app.include_router(config_rag_router)
# app.include_router(flashcard_router)
# app.include_router(lead_router)
# app.include_router(chat_with_files_router)
# app.include_router(twitter_router)

@app.get("/health")
def health():
    return {"status": "ok", "environment": os.getenv("RENDER_SERVICE_NAME", "local")}

@app.get("/version")
def version():
    """Bump `build` when debugging deploy drift (open /version in the browser)."""
    dq = any(
        getattr(r, "path", "") == "/tools/databricks-query/health"
        for r in app.routes
    )
    synth = any(
        getattr(r, "path", "") == "/tools/databricks-synth-data/health"
        for r in app.routes
    )
    return {
        "version": "1.2.0",
        "build": "2026-03-31-databricks-synth-data-v1",
        "module": "Vikaa AI Multi-Agent Platform",
        "databricks_query_health_registered": dq,
        "databricks_synth_data_health_registered": synth,
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
