"""Validación, lectura (streaming read_only) y exportación de archivos Excel (.xlsx)."""
import base64
import io

import openpyxl
import pandas as pd
from fastapi import HTTPException, UploadFile

from matching import normalize_name

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
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
        raise HTTPException(status_code=413, detail=f"Archivo '{file.filename}' supera 50 MB.")


def _format_headers(raw_headers) -> list[str]:
    return [
        f"Unnamed: {idx}" if val is None or str(val).strip() == "" else str(val)
        for idx, val in enumerate(raw_headers)
    ]


def _inspect_sheets_and_columns(contents: bytes) -> tuple[list[str], list[str]]:
    """Lee nombres de hojas y encabezados de la primera hoja en modo streaming."""
    wb = openpyxl.load_workbook(io.BytesIO(contents), read_only=True, data_only=True)
    try:
        sheets = list(wb.sheetnames)
        columns: list[str] = []
        if sheets:
            ws = wb[sheets[0]]
            first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
            if first_row:
                columns = _format_headers(first_row)
        return sheets, columns
    finally:
        wb.close()


def _inspect_headers(contents: bytes, sheet_name: str = "") -> list[str]:
    """Lee encabezados de una hoja específica en modo streaming."""
    wb = openpyxl.load_workbook(io.BytesIO(contents), read_only=True, data_only=True)
    try:
        target = sheet_name or (wb.sheetnames[0] if wb.sheetnames else "")
        if not target or target not in wb.sheetnames:
            raise HTTPException(status_code=400, detail=f"Hoja '{sheet_name}' no existe en el archivo.")
        ws = wb[target]
        first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
        if not first_row:
            raise HTTPException(status_code=400, detail="La hoja no tiene columnas.")
        return _format_headers(first_row)
    finally:
        wb.close()


def _read_single_column(contents: bytes, sheet_name: str, col_name: str, file_label: str):
    """Lee únicamente la columna elegida usando openpyxl read_only (bajo uso de RAM)."""
    wb = openpyxl.load_workbook(io.BytesIO(contents), read_only=True, data_only=True)
    try:
        target = sheet_name or (wb.sheetnames[0] if wb.sheetnames else "")
        if not target or target not in wb.sheetnames:
            raise HTTPException(status_code=400, detail=f"Hoja no encontrada: '{sheet_name}'.")
        ws = wb[target]
        rows_iter = ws.iter_rows(values_only=True)
        first_row = next(rows_iter, None)
        if not first_row:
            raise HTTPException(status_code=400, detail="Un archivo no tiene columnas.")

        headers = _format_headers(first_row)
        if col_name:
            if col_name not in headers:
                raise HTTPException(
                    status_code=400,
                    detail=f"Columna '{col_name}' no existe en {file_label}.",
                )
            col_idx = headers.index(col_name)
        else:
            col_idx = 0

        names = []
        original_names = []
        for row in rows_iter:
            val = row[col_idx] if col_idx < len(row) else None
            names.append(normalize_name(val))
            original_names.append(val)
        return names, original_names
    finally:
        wb.close()


def _prepare_inputs(contents1, contents2, col1_name, col2_name, sheet1_name, sheet2_name):
    """Lee y valida los Excels ya cargados. Devuelve nombres normalizados + originales."""
    names1, original_names1 = _read_single_column(contents1, sheet1_name, col1_name, "Archivo 1")
    names2, original_names2 = _read_single_column(contents2, sheet2_name, col2_name, "Archivo 2")

    if not names1 and not names2:
        raise HTTPException(status_code=400, detail="Ambos archivos están vacíos.")

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
