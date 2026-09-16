"""Tests del comparador de nombres. Solo cubren lógica pura (sin HTTP ni disco).

Ejecutar desde la raíz del proyecto con el venv activo:
    pytest
"""
import sys
import io
import os

import pytest
from fastapi import HTTPException, UploadFile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import (
    _build_comparison,
    _combined_scorer,
    _parse_threshold,
    _validate_upload,
    normalize_name,
)


# ── normalize_name ──

def test_normalize_mayusculas_y_espacios():
    assert normalize_name("  juan   pérez  ") == "JUAN PEREZ"


def test_normalize_quita_acentos_y_puntuacion():
    assert normalize_name("Ma. José-María") == "MA JOSE MARIA"
    assert normalize_name("O'BRIEN") == "O BRIEN"


def test_normalize_pliega_ene_y_conserva_digitos():
    # La Ñ se pliega a N (NFD la descompone y se quita la tilde).
    # Es correcto para matching: ambos lados normalizan igual.
    assert normalize_name("niño 123") == "NINO 123"


def test_normalize_vacios():
    assert normalize_name(None) == ""
    assert normalize_name("") == ""
    assert normalize_name("   ...  ") == ""


# ── _combined_scorer ──

def test_scorer_orden_invertido_es_100():
    assert _combined_scorer("JUAN PEREZ", "PEREZ JUAN") == 100


def test_scorer_segundo_apellido_alto():
    assert _combined_scorer("JUAN PEREZ GARCIA", "JUAN PEREZ") >= 85


def test_scorer_nombres_distintos_bajo():
    assert _combined_scorer("MARIA LOPEZ", "PEDRO GOMEZ") < 60


def test_scorer_respeta_score_cutoff():
    assert _combined_scorer("MARIA LOPEZ", "PEDRO GOMEZ", score_cutoff=90) == 0


# ── _parse_threshold ──

def test_threshold_default():
    assert _parse_threshold(None) == 85
    assert _parse_threshold("") == 85


def test_threshold_valido():
    assert _parse_threshold(70) == 70
    assert _parse_threshold("100") == 100


def test_threshold_invalido():
    with pytest.raises(HTTPException):
        _parse_threshold(101)
    with pytest.raises(HTTPException):
        _parse_threshold(-1)
    with pytest.raises(HTTPException):
        _parse_threshold("alto")


# ── _validate_upload ──

def _upload(name: str) -> UploadFile:
    return UploadFile(file=io.BytesIO(b"x"), filename=name)


def test_validate_extension():
    with pytest.raises(HTTPException) as e:
        _validate_upload(_upload("datos.txt"), b"hola")
    assert e.value.status_code == 400


def test_validate_vacio():
    with pytest.raises(HTTPException) as e:
        _validate_upload(_upload("datos.xlsx"), b"")
    assert e.value.status_code == 400


def test_validate_tamano():
    with pytest.raises(HTTPException) as e:
        _validate_upload(_upload("datos.xlsx"), b"x" * (10 * 1024 * 1024 + 1))
    assert e.value.status_code == 413


def test_validate_ok():
    _validate_upload(_upload("DATOS.XLSX"), b"contenido")  # no debe lanzar


# ── _build_comparison ──

def test_compare_exacta_y_fallida():
    n1 = ["JUAN PEREZ", "MARIA LOPEZ"]
    o1 = ["Juan Perez", "Maria Lopez"]
    n2 = ["PEREZ JUAN", "PEDRO GOMEZ"]
    o2 = ["Perez Juan", "Pedro Gomez"]
    all_r, matches, not_found, dup1, dup2 = _build_comparison(n1, o1, n2, o2, 85)
    assert len(all_r) == 2
    assert len(matches) == 1
    assert len(not_found) == 1
    assert matches[0]["Mejor Coincidencia Archivo 2"] == "Perez Juan"
    assert matches[0]["Similitud (%)"] == 100


def test_compare_umbral_estricto_filtra():
    n1 = ["JUAN PEREZ GARCIA"]
    o1 = ["Juan Perez Garcia"]
    n2 = ["JUAN PEREZ"]
    o2 = ["Juan Perez"]
    _, matches_85, _, _, _ = _build_comparison(n1, o1, n2, o2, 85)
    _, matches_100, _, _, _ = _build_comparison(n1, o1, n2, o2, 100)
    assert len(matches_85) == 1
    assert len(matches_100) == 0  # 90 < 100


def test_compare_exact_conserva_primera_ocurrencia():
    n1 = ["JUAN PEREZ"]
    o1 = ["Juan Perez"]
    n2 = ["JUAN PEREZ", "JUAN PEREZ"]
    o2 = ["Primero", "Segundo"]
    all_r, _, _, _, dup2 = _build_comparison(n1, o1, n2, o2, 85)
    assert all_r[0]["Mejor Coincidencia Archivo 2"] == "Primero"
    assert dup2 == ["Primero", "Segundo"]


def test_compare_duplicados_archivo1():
    n1 = ["JUAN PEREZ", "JUAN PEREZ"]
    o1 = ["Juan Perez", "Juan Perez"]
    n2 = ["JUAN PEREZ"]
    o2 = ["Juan Perez"]
    _, _, _, dup1, _ = _build_comparison(n1, o1, n2, o2, 85)
    assert dup1 == ["Juan Perez", "Juan Perez"]


def test_compare_vacios_se_omiten():
    n1 = ["JUAN PEREZ", ""]
    o1 = ["Juan Perez", ""]
    n2 = ["JUAN PEREZ"]
    o2 = ["Juan Perez"]
    all_r, matches, _, _, _ = _build_comparison(n1, o1, n2, o2, 85)
    assert len(all_r) == 1
    assert len(matches) == 1


def test_compare_sin_candidatos_marca_na():
    all_r, matches, not_found, _, _ = _build_comparison(
        ["JUAN PEREZ"], ["Juan Perez"], [""], [""], 85
    )
    assert len(matches) == 0
    assert not_found[0]["Mejor Coincidencia Archivo 2"] == "N/A"
