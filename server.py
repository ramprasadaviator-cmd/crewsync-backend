from fastapi import FastAPI, APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
import tempfile
from pathlib import Path
from pydantic import BaseModel
from typing import List, Optional
import uuid
from datetime import datetime, timezone

from parsers import parse_roster_pdf_text, extract_text_from_pdf

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB — os.getenv reads Railway env vars or falls back to .env
mongo_url = os.getenv('MONGO_URL')
db_name = os.getenv('DB_NAME')
if not mongo_url or not db_name:
    raise RuntimeError("MONGO_URL and DB_NAME environment variables are required")

client = AsyncIOMotorClient(mongo_url)
db = client[db_name]

app = FastAPI(title="CrewSync API", version="2.0.0")
api_router = APIRouter(prefix="/api")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ── Models ──

class ConfirmDutyItem(BaseModel):
    flight_number: Optional[str] = None
    duty_type: str
    departure_airport_iata: Optional[str] = None
    departure_airport_icao: Optional[str] = None
    arrival_airport_iata: Optional[str] = None
    arrival_airport_icao: Optional[str] = None
    reporting_time_utc: Optional[str] = None
    scheduled_departure_utc: Optional[str] = None
    scheduled_arrival_utc: Optional[str] = None
    aircraft_type: Optional[str] = None
    notes: Optional[str] = None
    overall_confidence: Optional[float] = None

class ConfirmRequest(BaseModel):
    duties: List[ConfirmDutyItem]
    pdf_filename: Optional[str] = None


# ── In-memory job store ──
jobs: dict = {}


def flatten_duties(duties: list) -> list:
    """Convert parsed day-level duties into flat list for API response."""
    flat = []
    for day in duties:
        if day['duty_type'] == 'FLIGHT' and day.get('duties'):
            for sector in day['duties']:
                flat.append({
                    'date': day.get('date'),
                    'flight_number': sector.get('flight_number'),
                    'duty_type': 'FLIGHT',
                    'departure_airport_iata': sector.get('departure_airport_iata'),
                    'arrival_airport_iata': sector.get('arrival_airport_iata'),
                    'scheduled_departure_utc': sector.get('scheduled_departure_utc'),
                    'scheduled_arrival_utc': sector.get('scheduled_arrival_utc'),
                    'aircraft_type': sector.get('aircraft_type'),
                    'reporting_time_utc': day.get('reporting_time'),
                    'overall_confidence': sector.get('overall_confidence', day.get('overall_confidence')),
                    'notes': None,
                })
        else:
            code = day.get('duty_code', '')
            flat.append({
                'date': day.get('date'),
                'flight_number': None,
                'duty_type': day['duty_type'],
                'duty_code': code,
                'departure_airport_iata': None,
                'arrival_airport_iata': None,
                'scheduled_departure_utc': None,
                'scheduled_arrival_utc': None,
                'aircraft_type': None,
                'reporting_time_utc': day.get('reporting_time'),
                'overall_confidence': day.get('overall_confidence', 0.7),
                'notes': f"{day['duty_type']}" + (f" ({code})" if code else ''),
            })
    return flat


async def process_pdf_job(job_id: str, file_path: str, filename: str):
    """Background task: extract text (OCR if needed) and parse roster."""
    try:
        jobs[job_id]['status'] = 'processing'
        jobs[job_id]['progress'] = 10

        logger.info(f"[{job_id}] Extracting text from {filename}")
        full_text = extract_text_from_pdf(file_path)
        jobs[job_id]['progress'] = 55

        logger.info(f"[{job_id}] Extracted {len(full_text)} chars")

        if not full_text.strip():
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = 'PDF text extraction returned empty. The PDF may be corrupted or contain only images that OCR cannot read.'
            jobs[job_id]['debug'] = {'text_length': 0, 'text_snippet': ''}
            return

        logger.info(f"[{job_id}] Parsing roster...")
        result = parse_roster_pdf_text(full_text)
        jobs[job_id]['progress'] = 90

        duties = result.get('duties', [])
        if not duties:
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = f'No duties extracted. Format: {result["debug"]["format_detected"]}. Text starts: {full_text[:200]}'
            jobs[job_id]['debug'] = result.get('debug', {})
            return

        flat = flatten_duties(duties)

        jobs[job_id]['status'] = 'completed'
        jobs[job_id]['progress'] = 100
        jobs[job_id]['duties'] = flat
        jobs[job_id]['metadata'] = result.get('metadata', {})
        jobs[job_id]['stats'] = result.get('stats', {})
        jobs[job_id]['debug'] = result.get('debug', {})
        jobs[job_id]['filename'] = filename

        logger.info(f"[{job_id}] Done: {len(flat)} entries ({result['stats']})")

    except Exception as e:
        logger.error(f"[{job_id}] Failed: {e}", exc_info=True)
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['error'] = f'Parsing error: {str(e)}'
    finally:
        try:
            os.unlink(file_path)
        except OSError:
            pass


# ── Routes ──

@api_router.get("/")
async def root():
    return {"message": "CrewSync API", "version": "2.0.0", "status": "operational"}

@api_router.get("/health")
async def health():
    return {"status": "healthy"}


# ── PDF Roster ──

@api_router.post("/roster/pdf/upload")
async def upload_roster_pdf(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file uploaded")
    if len(content) > 20 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 20MB)")

    job_id = str(uuid.uuid4())
    temp_path = os.path.join(tempfile.gettempdir(), f"roster_{job_id}.pdf")
    with open(temp_path, 'wb') as f:
        f.write(content)

    jobs[job_id] = {
        'status': 'queued', 'progress': 0,
        'duties': None, 'metadata': None, 'stats': None,
        'debug': None, 'error': None,
        'filename': file.filename,
        'created_at': datetime.now(timezone.utc).isoformat(),
    }

    background_tasks.add_task(process_pdf_job, job_id, temp_path, file.filename)
    logger.info(f"Upload accepted: {file.filename} ({len(content)}B) -> {job_id}")
    return {"job_id": job_id, "status": "queued", "message": f"Processing {file.filename}"}


@api_router.get("/roster/pdf/status/{job_id}")
async def get_pdf_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    job = jobs[job_id]
    return {
        "status": job['status'],
        "progress": job.get('progress', 0),
        "duties": job.get('duties'),
        "metadata": job.get('metadata'),
        "stats": job.get('stats'),
        "debug": job.get('debug'),
        "error": job.get('error'),
        "filename": job.get('filename'),
    }


@api_router.post("/roster/pdf/confirm")
async def confirm_pdf_duties(request: ConfirmRequest):
    if not request.duties:
        raise HTTPException(status_code=400, detail="No duties to confirm")

    docs = []
    for duty in request.duties:
        doc = duty.model_dump()
        doc['id'] = str(uuid.uuid4())
        doc['confirmed_at'] = datetime.now(timezone.utc).isoformat()
        doc['pdf_filename'] = request.pdf_filename
        docs.append(doc)

    if docs:
        await db.confirmed_duties.insert_many(docs)

    logger.info(f"Confirmed {len(docs)} duties from {request.pdf_filename}")
    return {"status": "confirmed", "count": len(docs), "message": f"Imported {len(docs)} duties"}


@api_router.get("/roster/duties")
async def get_duties():
    duties = await db.confirmed_duties.find({}, {"_id": 0}).to_list(1000)
    return duties


# Include router + middleware
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.getenv('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
