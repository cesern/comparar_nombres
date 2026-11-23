import asyncio
import io
import logging
import os
import threading
import time
import uuid

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool

from excel_io import (
    ALLOWED_EXTENSIONS,
    MAX_UPLOAD_BYTES,
    _build_excel_b64,
    _build_excel_bytes,
    _inspect_headers,
    _inspect_sheets_and_columns,
    _prepare_inputs,
    _validate_upload,
)
from matching import _build_comparison, _combined_scorer, normalize_name

logger = logging.getLogger("comparar_nombres")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Comparador de Nombres")

# Rutas absolutas respecto a este archivo (seguro en Render, no depende del cwd)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


def _parse_threshold(value) -> int:
    """Acepta int o str (compat con frontend viejo que no lo envía). Default 85."""
    if value is None or value == "":
        return 85
    try:
        t = int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Umbral inválido (0-100).")
    if t < 0 or t > 100:
        raise HTTPException(status_code=400, detail="Umbral fuera de rango (0-100).")
    return t


class JobCancelled(Exception):
    pass


# ── Jobs async con polling y descargas temporales ──
JOBS = {}
DOWNLOADS = {}
JOBS_LOCK = threading.Lock()
JOB_TTL_SECONDS = 30 * 60
DOWNLOAD_TTL_SECONDS = 15 * 60
MAX_JOBS = 10
MAX_DOWNLOADS = 10


def _job_evict_locked():
    now = time.time()
    for jid in [k for k, j in JOBS.items() if now - j["created"] > JOB_TTL_SECONDS]:
        del JOBS[jid]
    while len(JOBS) > MAX_JOBS:
        oldest = min(JOBS, key=lambda k: JOBS[k]["created"])
        del JOBS[oldest]


def _download_evict_locked():
    now = time.time()
    for did in [k for k, d in DOWNLOADS.items() if now - d["created"] > DOWNLOAD_TTL_SECONDS]:
        del DOWNLOADS[did]
    while len(DOWNLOADS) > MAX_DOWNLOADS:
        oldest = min(DOWNLOADS, key=lambda k: DOWNLOADS[k]["created"])
        del DOWNLOADS[oldest]


def _make_result(
    names1,
    names2,
    empty_file1,
    empty_file2,
    threshold,
    all_results,
    matches,
    not_found,
    duplicates_list1,
    duplicates_list2,
    excel_b64=None,
    download_url=None,
):
    total_processed = len(all_results)
    matches_count = len(matches)
    not_found_count = len(not_found)
    match_rate = round((matches_count / total_processed * 100), 1) if total_processed else 0
    dup_norm2 = {normalize_name(v) for v in duplicates_list2 if normalize_name(v)}
    dup_norm1 = {normalize_name(v) for v in duplicates_list1 if normalize_name(v)}
    out = {
        "results": all_results,
        "threshold": threshold,
        "stats": {
            "total_file1": len(names1),
            "total_file2": len(names2),
            "total_processed": total_processed,
            "empty_file1": empty_file1,
            "empty_file2": empty_file2,
            "matches": matches_count,
            "not_found": not_found_count,
            "match_rate": match_rate,
            "duplicates_file2": len(dup_norm2),
            "duplicates_file1": len(dup_norm1),
        },
    }
    if excel_b64 is not None:
        out["excel_b64"] = excel_b64
    if download_url is not None:
        out["download_url"] = download_url
    return out


async def _run_job(
    job_id,
    names1,
    original_names1,
    names2,
    original_names2,
    empty_file1,
    empty_file2,
    threshold,
):
    """Procesa un job en background actualizando su progreso."""

    def progress(done, total):
        with JOBS_LOCK:
            job = JOBS.get(job_id)
            if job is None or job.get("cancel"):
                raise JobCancelled()
            job["processed"] = done
            job["total"] = total

    try:
        with JOBS_LOCK:
            JOBS[job_id]["stage"] = "comparando"
        compared = await run_in_threadpool(
            _build_comparison,
            names1,
            original_names1,
            names2,
            original_names2,
            threshold,
            progress,
        )
        all_results, matches, not_found, dup1, dup2 = compared
        with JOBS_LOCK:
            JOBS[job_id]["stage"] = "generando"
        excel_bytes = await run_in_threadpool(_build_excel_bytes, matches, not_found, dup1, dup2)
        result = _make_result(
            names1,
            names2,
            empty_file1,
            empty_file2,
            threshold,
            all_results,
            matches,
            not_found,
            dup1,
            dup2,
            download_url=f"/download/{job_id}",
        )
        with JOBS_LOCK:
            job = JOBS.get(job_id)
            if job is None:
                return
            _download_evict_locked()
            DOWNLOADS[job_id] = {"bytes": excel_bytes, "created": time.time()}
            job.update(
                {
                    "stage": "done",
                    "status": "done",
                    "result": result,
                    "processed": job.get("total", 0),
                }
            )
    except JobCancelled:
        with JOBS_LOCK:
            if job_id in JOBS:
                JOBS[job_id].update({"stage": "cancelled", "status": "cancelled"})
    except Exception as e:
        logger.exception("Error en job %s: %s", job_id, e)
        with JOBS_LOCK:
            if job_id in JOBS:
                JOBS[job_id].update(
                    {
                        "stage": "error",
                        "status": "error",
                        "error": "No se pudo completar la comparación.",
                    }
                )


@app.get("/health")
async def health():
    """Healthcheck para Render (sin dependencias pesadas)."""
    return {"status": "ok"}


@app.get("/")
async def read_item(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.post("/sheets")
async def get_sheets(file: UploadFile = File(...)):
    """Devuelve las hojas del Excel y las columnas de la primera hoja."""
    try:
        contents = await file.read()
        _validate_upload(file, contents)
        sheets, columns = await run_in_threadpool(_inspect_sheets_and_columns, contents)
        return {"sheets": sheets, "columns": columns}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error en /sheets: %s", e)
        raise HTTPException(
            status_code=400,
            detail="No se pudo leer el Excel. Verifica que sea un .xlsx válido.",
        )


@app.post("/headers")
async def get_headers(file: UploadFile = File(...), sheet_name: str = Form("")):
    """Devuelve los encabezados de una hoja de un archivo Excel."""
    try:
        contents = await file.read()
        _validate_upload(file, contents)
        columns = await run_in_threadpool(_inspect_headers, contents, sheet_name)
        return {"columns": columns}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error en /headers: %s", e)
        raise HTTPException(status_code=400, detail="No se pudieron leer las columnas del Excel.")


@app.post("/compare")
async def compare_files(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    col1_name: str = Form(""),
    col2_name: str = Form(""),
    sheet1_name: str = Form(""),
    sheet2_name: str = Form(""),
    threshold: int = Form(85),
):
    try:
        threshold = _parse_threshold(threshold)
        contents1 = await file1.read()
        contents2 = await file2.read()
        _validate_upload(file1, contents1)
        _validate_upload(file2, contents2)

        names1, original_names1, names2, original_names2 = await run_in_threadpool(
            _prepare_inputs, contents1, contents2, col1_name, col2_name, sheet1_name, sheet2_name
        )

        empty_file1 = sum(1 for n in names1 if not n)
        empty_file2 = sum(1 for n in names2 if not n)

        all_results, matches, not_found, duplicates_list1, duplicates_list2 = await run_in_threadpool(
            _build_comparison, names1, original_names1, names2, original_names2, threshold
        )

        excel_b64 = await run_in_threadpool(
            _build_excel_b64, matches, not_found, duplicates_list1, duplicates_list2
        )

        return _make_result(
            names1,
            names2,
            empty_file1,
            empty_file2,
            threshold,
            all_results,
            matches,
            not_found,
            duplicates_list1,
            duplicates_list2,
            excel_b64,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error en /compare: %s", e)
        raise HTTPException(
            status_code=400, detail="No se pudo comparar. Verifica hojas, columnas y formato."
        )


@app.post("/start", status_code=202)
async def start_job(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    col1_name: str = Form(""),
    col2_name: str = Form(""),
    sheet1_name: str = Form(""),
    sheet2_name: str = Form(""),
    threshold: int = Form(85),
):
    """Crea un job async. Responde inmediato con job_id."""
    try:
        threshold = _parse_threshold(threshold)
        contents1 = await file1.read()
        contents2 = await file2.read()
        _validate_upload(file1, contents1)
        _validate_upload(file2, contents2)

        names1, original_names1, names2, original_names2 = await run_in_threadpool(
            _prepare_inputs, contents1, contents2, col1_name, col2_name, sheet1_name, sheet2_name
        )

        empty_file1 = sum(1 for n in names1 if not n)
        empty_file2 = sum(1 for n in names2 if not n)
        total = sum(1 for n in names1 if n)

        job_id = uuid.uuid4().hex[:12]
        with JOBS_LOCK:
            _job_evict_locked()
            JOBS[job_id] = {
                "status": "queued",
                "stage": "encolado",
                "processed": 0,
                "total": total,
                "cancel": False,
                "created": time.time(),
                "result": None,
            }

        asyncio.create_task(
            _run_job(
                job_id,
                names1,
                original_names1,
                names2,
                original_names2,
                empty_file1,
                empty_file2,
                threshold,
            )
        )
        return {"job_id": job_id, "total": total}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error en /start: %s", e)
        raise HTTPException(status_code=400, detail="No se pudo iniciar la comparación.")


@app.get("/progress/{job_id}")
async def job_progress(job_id: str):
    """Avance del job. Cuando status=done incluye el resultado completo."""
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job no encontrado o expirado.")
        out = {
            "job_id": job_id,
            "status": job["status"],
            "stage": job["stage"],
            "processed": job["processed"],
            "total": job["total"],
        }
        if job["status"] == "done":
            out["result"] = job["result"]
            del JOBS[job_id]
        elif job["status"] == "error":
            out["error"] = job.get("error", "Error en el job.")
            del JOBS[job_id]
        elif job["status"] == "cancelled":
            del JOBS[job_id]
        return out


@app.delete("/progress/{job_id}")
async def job_cancel(job_id: str):
    """Marca el job para cancelación (o lo limpia si ya terminó)."""
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job no encontrado o expirado.")
        if job["status"] in ("done", "error", "cancelled"):
            del JOBS[job_id]
            return {"job_id": job_id, "cancelled": False, "cleaned": True}
        job["cancel"] = True
        return {"job_id": job_id, "cancelled": True}


@app.get("/download/{job_id}")
async def download_excel(job_id: str):
    """Descarga binaria del Excel generado por un job."""
    with JOBS_LOCK:
        _download_evict_locked()
        entry = DOWNLOADS.get(job_id)
        if entry is None:
            raise HTTPException(status_code=404, detail="Archivo expirado o no encontrado.")
        content = entry["bytes"]
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="Resultados_Comparacion.xlsx"'},
    )


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
