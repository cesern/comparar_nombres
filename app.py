from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool
import pandas as pd
from rapidfuzz import fuzz, process
import unicodedata
import io
import re
import base64
import os
import logging

logger = logging.getLogger("comparar_nombres")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Comparador de Nombres")

# Límite de seguridad para no tumbar Render (10 MB por archivo)
MAX_UPLOAD_BYTES = 10 * 1024 * 1024
ALLOWED_EXTENSIONS = (".xlsx", ".xls")

# Rutas absolutas respecto a este archivo (seguro en Render, no depende del cwd)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


def _validate_upload(file: UploadFile, contents: bytes) -> None:
    filename = (file.filename or "").lower()
    if not filename.endswith(ALLOWED_EXTENSIONS):
        raise HTTPException(status_code=400, detail=f"Archivo '{file.filename}' no es Excel válido (.xlsx/.xls).")
    if len(contents) == 0:
        raise HTTPException(status_code=400, detail=f"Archivo '{file.filename}' está vacío.")
    if len(contents) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"Archivo '{file.filename}' supera 10 MB.")

def normalize_name(name):
    if pd.isna(name):
        return ""
    name = str(name).upper()
    # Quitar acentos (eliminar marcas diacríticas; la Ñ se conserva)
    name = ''.join((c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn'))
    # Quitar puntuación y símbolos (MA. -> MA, O'BRIEN -> O BRIEN) conservando Ñ, dígitos y espacios
    name = re.sub(r'[^A-ZÑ0-9 ]+', ' ', name)
    # Eliminar espacios duplicados y extremos
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def _combined_scorer(s1, s2, *, score_cutoff=0):
    """Máximo entre token_sort (orden invertido -> 100) y WRatio (typos, apellido extra).

    No se usa token_set_ratio: da 100 ante cualquier subconjunto
    (ej. 'JUAN' vs 'JUAN PEREZ GARCIA'), demasiados falsos positivos.
    Para casos estrictos, subir el umbral a 90-95.
    Compatible con la firma scorer de rapidfuzz: (s1, s2, *, score_cutoff).
    """
    best = fuzz.token_sort_ratio(s1, s2)
    tmp = fuzz.WRatio(s1, s2)
    if tmp > best:
        best = tmp
    return best if best >= score_cutoff else 0


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


def _build_comparison(names1, original_names1, names2, original_names2, threshold: int):
    """Bloque CPU-bound (fuzzy + Excel). Se ejecuta en threadpool para no bloquear el loop."""
    # Duplicados por nombre normalizado (conteo sobre normalizados, no originales)
    seen2, dup_norm2 = set(), set()
    for n in names2:
        if not n:
            continue
        if n in seen2:
            dup_norm2.add(n)
        else:
            seen2.add(n)
    duplicates_list2 = [o for n, o in zip(names2, original_names2) if n in dup_norm2]

    seen1, dup_norm1 = set(), set()
    for n in names1:
        if not n:
            continue
        if n in seen1:
            dup_norm1.add(n)
        else:
            seen1.add(n)
    duplicates_list1 = [o for n, o in zip(names1, original_names1) if n in dup_norm1]

    choices = {idx: name for idx, name in enumerate(names2) if name}

    # Exact-match conservando la PRIMERA ocurrencia (antes: la última la sobrescribía)
    exact_match_dict = {}
    for idx, (name, original) in enumerate(zip(names2, original_names2)):
        if name and name not in exact_match_dict:
            exact_match_dict[name] = (idx, original)

    all_results, matches, not_found = [], [], []
    for i, n1 in enumerate(names1):
        if not n1:
            continue

        best_score = 0
        best_original = None

        if n1 in exact_match_dict:
            best_score = 100
            best_original = exact_match_dict[n1][1]
        elif choices:
            res = process.extractOne(n1, choices, scorer=_combined_scorer, score_cutoff=1)
            if res:
                _match_str, best_score, match_idx = res
                best_original = original_names2[match_idx]

        result_status = "COINCIDENCIA" if best_score >= threshold else "NO ENCONTRADO"
        res_row = {
            "Nombre Archivo 1": str(original_names1[i]),
            "Mejor Coincidencia Archivo 2": str(best_original) if best_original else "N/A",
            "Similitud (%)": round(float(best_score), 2),
            "Resultado": result_status,
        }
        if result_status == "COINCIDENCIA":
            matches.append(res_row)
        else:
            not_found.append(res_row)
        all_results.append(res_row)

    return all_results, matches, not_found, duplicates_list1, duplicates_list2


@app.get("/health")
async def health():
    """Healthcheck para Render (sin dependencias pesadas)."""
    return {"status": "ok"}


@app.get("/")
async def read_item(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request},
    )

@app.post("/sheets")
async def get_sheets(file: UploadFile = File(...)):
    """Devuelve los nombres de las hojas de un archivo Excel."""
    try:
        contents = await file.read()
        _validate_upload(file, contents)
        xl = pd.ExcelFile(io.BytesIO(contents))
        return {"sheets": xl.sheet_names}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error en /sheets: %s", e)
        raise HTTPException(status_code=400, detail="No se pudo leer el Excel. Verifica que sea un .xlsx/.xls válido.")

@app.post("/headers")
async def get_headers(file: UploadFile = File(...), sheet_name: str = Form("")):
    """Devuelve los encabezados de una hoja de un archivo Excel."""
    try:
        contents = await file.read()
        _validate_upload(file, contents)
        kwargs = {"nrows": 0}
        if sheet_name:
            kwargs["sheet_name"] = sheet_name
        try:
            df = pd.read_excel(io.BytesIO(contents), **kwargs)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Hoja '{sheet_name}' no existe en el archivo.")
        if df.columns.empty:
            raise HTTPException(status_code=400, detail="La hoja no tiene columnas.")
        return {"columns": df.columns.tolist()}
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
        # 1. Leer ambos archivos Excel en memoria, respetando la hoja elegida
        contents1 = await file1.read()
        contents2 = await file2.read()
        _validate_upload(file1, contents1)
        _validate_upload(file2, contents2)

        read_kw1 = {"sheet_name": sheet1_name} if sheet1_name else {}
        read_kw2 = {"sheet_name": sheet2_name} if sheet2_name else {}

        try:
            df1 = pd.read_excel(io.BytesIO(contents1), **read_kw1)
            df2 = pd.read_excel(io.BytesIO(contents2), **read_kw2)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Hoja no encontrada: {e}")

        if df1.columns.empty or df2.columns.empty:
            raise HTTPException(status_code=400, detail="Un archivo no tiene columnas.")
        if df1.empty and df2.empty:
            raise HTTPException(status_code=400, detail="Ambos archivos están vacíos.")

        # 2. Usar la columna elegida por el usuario (o la primera si no viene)
        if col1_name and col1_name not in df1.columns:
            raise HTTPException(status_code=400, detail=f"Columna '{col1_name}' no existe en Archivo 1.")
        if col2_name and col2_name not in df2.columns:
            raise HTTPException(status_code=400, detail=f"Columna '{col2_name}' no existe en Archivo 2.")
        col1 = col1_name if col1_name else df1.columns[0]
        col2 = col2_name if col2_name else df2.columns[0]

        # 3. Normalizar nombres
        names1 = df1[col1].apply(normalize_name).tolist()
        original_names1 = df1[col1].tolist()

        names2 = df2[col2].apply(normalize_name).tolist()
        original_names2 = df2[col2].tolist()

        empty_file1 = sum(1 for n in names1 if not n)
        empty_file2 = sum(1 for n in names2 if not n)

        # 4. Comparación fuzzy en threadpool (no bloquea el event loop en Render)
        all_results, matches, not_found, duplicates_list1, duplicates_list2 = await run_in_threadpool(
            _build_comparison, names1, original_names1, names2, original_names2, threshold
        )

        # 5. Generar Excel en memoria para la exportación
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if matches:
                pd.DataFrame(matches).to_excel(writer, sheet_name='Coincidencias', index=False)
            else:
                pd.DataFrame(columns=["Nombre Archivo 1", "Mejor Coincidencia Archivo 2", "Similitud (%)", "Resultado"]).to_excel(writer, sheet_name='Coincidencias', index=False)

            if not_found:
                pd.DataFrame(not_found).to_excel(writer, sheet_name='No encontrados', index=False)
            else:
                pd.DataFrame(columns=["Nombre Archivo 1", "Mejor Coincidencia Archivo 2", "Similitud (%)", "Resultado"]).to_excel(writer, sheet_name='No encontrados', index=False)

            if duplicates_list2:
                pd.DataFrame({"Duplicados en Archivo 2": duplicates_list2}).to_excel(writer, sheet_name='Duplicados detectados', index=False)
            else:
                pd.DataFrame(columns=["Duplicados en Archivo 2"]).to_excel(writer, sheet_name='Duplicados detectados', index=False)

            if duplicates_list1:
                pd.DataFrame({"Duplicados en Archivo 1": duplicates_list1}).to_excel(writer, sheet_name='Duplicados archivo 1', index=False)
            else:
                pd.DataFrame(columns=["Duplicados en Archivo 1"]).to_excel(writer, sheet_name='Duplicados archivo 1', index=False)

        output.seek(0)
        # Codificamos el archivo en Base64 para que el frontend pueda descargarlo
        excel_b64 = base64.b64encode(output.read()).decode('utf-8')

        total_processed = len(all_results)
        matches_count   = len(matches)
        not_found_count = len(not_found)
        match_rate      = round((matches_count / total_processed * 100), 1) if total_processed else 0

        # Conteo de duplicados sobre normalizados (antes: sobre originales)
        dup_norm2 = {normalize_name(v) for v in duplicates_list2 if normalize_name(v)}
        dup_norm1 = {normalize_name(v) for v in duplicates_list1 if normalize_name(v)}

        return {
            "results": all_results,
            "excel_b64": excel_b64,
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
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error en /compare: %s", e)
        raise HTTPException(status_code=400, detail="No se pudo comparar. Verifica hojas, columnas y formato.")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
