"""Validación, lectura y exportación de archivos Excel (.xlsx)."""
import base64
import io

import pandas as pd
from fastapi import HTTPException, UploadFile

from matching import normalize_name

MAX_UPLOAD_BYTES = 10 * 1024 * 1024
ALLOWED_EXTENSIONS = (".xlsx",)

_RESULT_COLUMNS = [
    "Nombre Archivo 1",
    "Mejor Coincidencia Archivo 2",
    "Similitud (%)",
    "Resultado",
]


def _validate_upload(file: UploadFile, contents: bytes) -> None:
    filename = (file.filename or "").lower()
    if not filename.endswith(ALLOWED_EXTENSIONS):
        raise HTTPException(
            status_code=400,
            detail=f"Archivo '{file.filename}' no es Excel válido (.xlsx).",
        )
    if len(contents) == 0:
        raise HTTPException(status_code=400, detail=f"Archivo '{file.filename}' está vacío.")
    if len(contents) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"Archivo '{file.filename}' supera 10 MB.")


def _prepare_inputs(contents1, contents2, col1_name, col2_name, sheet1_name, sheet2_name):
    """Lee y valida los Excels ya cargados. Devuelve nombres normalizados + originales."""
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

    df1.columns = [str(c) for c in df1.columns]
    df2.columns = [str(c) for c in df2.columns]

    if col1_name and col1_name not in df1.columns:
        raise HTTPException(status_code=400, detail=f"Columna '{col1_name}' no existe en Archivo 1.")
    if col2_name and col2_name not in df2.columns:
        raise HTTPException(status_code=400, detail=f"Columna '{col2_name}' no existe en Archivo 2.")
    col1 = col1_name if col1_name else df1.columns[0]
    col2 = col2_name if col2_name else df2.columns[0]

    names1 = df1[col1].apply(normalize_name).tolist()
    original_names1 = df1[col1].tolist()
    names2 = df2[col2].apply(normalize_name).tolist()
    original_names2 = df2[col2].tolist()
    return names1, original_names1, names2, original_names2


def _build_excel_bytes(matches, not_found, duplicates_list1, duplicates_list2) -> bytes:
    """Genera el Excel de resultados en memoria y devuelve los bytes."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(matches or [], columns=_RESULT_COLUMNS).to_excel(
            writer, sheet_name="Coincidencias", index=False
        )
        pd.DataFrame(not_found or [], columns=_RESULT_COLUMNS).to_excel(
            writer, sheet_name="No encontrados", index=False
        )
        pd.DataFrame(
            {"Duplicados en Archivo 2": duplicates_list2} if duplicates_list2 else [],
            columns=["Duplicados en Archivo 2"],
        ).to_excel(writer, sheet_name="Duplicados detectados", index=False)
        pd.DataFrame(
            {"Duplicados en Archivo 1": duplicates_list1} if duplicates_list1 else [],
            columns=["Duplicados en Archivo 1"],
        ).to_excel(writer, sheet_name="Duplicados archivo 1", index=False)

    return output.getvalue()


def _build_excel_b64(matches, not_found, duplicates_list1, duplicates_list2) -> str:
    """Genera el Excel de resultados y lo devuelve en Base64."""
    return base64.b64encode(
        _build_excel_bytes(matches, not_found, duplicates_list1, duplicates_list2)
    ).decode("utf-8")
