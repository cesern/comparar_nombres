"""Tests de integración HTTP/ASGI sobre los endpoints FastAPI usando TestClient."""
import io
import os
import sys
import time

import openpyxl
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import DOWNLOADS, JOBS, app

client = TestClient(app)

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _make_xlsx_bytes(sheets: dict[str, list[list]]) -> bytes:
    wb = openpyxl.Workbook()
    first = True
    for name, rows in sheets.items():
        ws = wb.active if first else wb.create_sheet(title=name)
        ws.title = name
        first = False
        for row in rows:
            ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_health_y_index():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

    home = client.get("/")
    assert home.status_code == 200
    assert "Comparador de Nombres" in home.text


def test_sheets_y_headers_con_encabezado_numerico():
    data = _make_xlsx_bytes({"Hoja1": [[2025, "NOMBRE"], ["A", "Juan Perez"]]})

    r_sheets = client.post("/sheets", files={"file": ("datos.xlsx", data, XLSX_MIME)})
    assert r_sheets.status_code == 200
    assert r_sheets.json() == {"sheets": ["Hoja1"], "columns": ["2025", "NOMBRE"]}

    r_headers = client.post(
        "/headers",
        files={"file": ("datos.xlsx", data, XLSX_MIME)},
        data={"sheet_name": "Hoja1"},
    )
    assert r_headers.status_code == 200
    assert r_headers.json() == {"columns": ["2025", "NOMBRE"]}


def test_headers_hoja_inexistente():
    data = _make_xlsx_bytes({"Hoja1": [["NOMBRE"], ["Juan"]]})
    r = client.post(
        "/headers",
        files={"file": ("datos.xlsx", data, XLSX_MIME)},
        data={"sheet_name": "NoExiste"},
    )
    assert r.status_code == 400


def test_compare_endpoint_con_encabezado_numerico():
    f1 = _make_xlsx_bytes({"Datos": [[2025], ["Juan Perez"], ["Maria Lopez"]]})
    f2 = _make_xlsx_bytes({"Ref": [[2026], ["PEREZ JUAN"]]})

    r = client.post(
        "/compare",
        files={
            "file1": ("f1.xlsx", f1, XLSX_MIME),
            "file2": ("f2.xlsx", f2, XLSX_MIME),
        },
        data={
            "col1_name": "2025",
            "col2_name": "2026",
            "sheet1_name": "Datos",
            "sheet2_name": "Ref",
            "threshold": "85",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["stats"]["matches"] == 1
    assert body["stats"]["not_found"] == 1
    assert body["excel_b64"]


def test_start_progress_download_y_limpieza():
    f1 = _make_xlsx_bytes({"H": [["NOMBRE"], ["Juan Perez"], ["Ana Ruiz"]]})
    f2 = _make_xlsx_bytes({"H": [["NOMBRE"], ["PEREZ JUAN"]]})

    with TestClient(app) as c:
        r_start = c.post(
            "/start",
            files={
                "file1": ("f1.xlsx", f1, XLSX_MIME),
                "file2": ("f2.xlsx", f2, XLSX_MIME),
            },
            data={
                "col1_name": "NOMBRE",
                "col2_name": "NOMBRE",
                "sheet1_name": "H",
                "sheet2_name": "H",
                "threshold": "85",
            },
        )
        assert r_start.status_code == 202
        jid = r_start.json()["job_id"]

        result = None
        for _ in range(100):
            r_prog = c.get(f"/progress/{jid}")
            assert r_prog.status_code == 200
            prog = r_prog.json()
            assert prog["status"] != "error", prog
            if prog["status"] == "done":
                result = prog["result"]
                break
            time.sleep(0.05)

        assert result is not None
        assert result["stats"]["matches"] == 1
        assert "excel_b64" not in result
        assert result["download_url"] == f"/download/{jid}"

        # El job fue eliminado de JOBS tras entregar status=done
        assert jid not in JOBS
        assert c.get(f"/progress/{jid}").status_code == 404

        # La descarga binaria funciona y devuelve un .xlsx válido
        r_dl = c.get(result["download_url"])
        assert r_dl.status_code == 200
        assert r_dl.headers["content-type"].startswith(XLSX_MIME)
        wb = openpyxl.load_workbook(io.BytesIO(r_dl.content))
        assert "Coincidencias" in wb.sheetnames
        DOWNLOADS.pop(jid, None)


def test_cancel_job():
    f1 = _make_xlsx_bytes({"H": [["NOMBRE"], ["Juan Perez"]]})
    f2 = _make_xlsx_bytes({"H": [["NOMBRE"], ["Juan Perez"]]})

    r_start = client.post(
        "/start",
        files={
            "file1": ("f1.xlsx", f1, XLSX_MIME),
            "file2": ("f2.xlsx", f2, XLSX_MIME),
        },
        data={"col1_name": "NOMBRE", "col2_name": "NOMBRE", "sheet1_name": "H", "sheet2_name": "H"},
    )
    assert r_start.status_code == 202
    jid = r_start.json()["job_id"]

    r_del = client.delete(f"/progress/{jid}")
    assert r_del.status_code == 200
    JOBS.pop(jid, None)
    DOWNLOADS.pop(jid, None)
