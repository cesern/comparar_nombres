"""Lógica pura de normalización y comparación fuzzy de nombres."""
import re
import unicodedata

import pandas as pd
from rapidfuzz import fuzz, process


def normalize_name(name):
    if pd.isna(name):
        return ""
    name = str(name).upper()
    # Quitar acentos (eliminar marcas diacríticas; la Ñ se pliega a N — ambos lados igual)
    name = "".join(
        c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn"
    )
    # Quitar puntuación y símbolos (MA. -> MA, O'BRIEN -> O BRIEN); quedan letras, dígitos y espacios
    name = re.sub(r"[^A-Z0-9 ]+", " ", name)
    # Eliminar espacios duplicados y extremos
    name = re.sub(r"\s+", " ", name).strip()
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


def _best_fuzzy_match(query: str, choices: dict[int, str]):
    """Evalúa token_sort_ratio y WRatio usando los scorers nativos en C++ de RapidFuzz."""
    r1 = process.extractOne(query, choices, scorer=fuzz.token_sort_ratio, score_cutoff=1)
    r2 = process.extractOne(query, choices, scorer=fuzz.WRatio, score_cutoff=1)
    if r1 is None:
        return r2
    if r2 is None:
        return r1
    return r1 if r1[1] >= r2[1] else r2


def _find_duplicates(names, original_names):
    seen, dup_norm = set(), set()
    for n in names:
        if not n:
            continue
        if n in seen:
            dup_norm.add(n)
        else:
            seen.add(n)
    return [o for n, o in zip(names, original_names) if n in dup_norm]


def _build_comparison(names1, original_names1, names2, original_names2, threshold: int, progress=None):
    """Bloque CPU-bound (fuzzy). Se ejecuta en threadpool para no bloquear el loop.

    `progress(done, total)` es opcional: se llama cada ~25 filas para jobs async.
    Si lanza excepción, se propaga (permite cancelar el job).
    """
    duplicates_list2 = _find_duplicates(names2, original_names2)
    duplicates_list1 = _find_duplicates(names1, original_names1)

    choices = {idx: name for idx, name in enumerate(names2) if name}

    # Exact-match conservando la PRIMERA ocurrencia
    exact_match_dict = {}
    for idx, (name, original) in enumerate(zip(names2, original_names2)):
        if name and name not in exact_match_dict:
            exact_match_dict[name] = (idx, original)

    all_results, matches, not_found = [], [], []
    fuzzy_cache = {}
    total_work = sum(1 for n in names1 if n)
    done_work = 0
    for i, n1 in enumerate(names1):
        if not n1:
            continue

        best_score = 0
        best_original = None

        if n1 in exact_match_dict:
            best_score = 100
            best_original = exact_match_dict[n1][1]
        elif n1 in fuzzy_cache:
            best_score, best_original = fuzzy_cache[n1]
        elif choices:
            res = _best_fuzzy_match(n1, choices)
            if res:
                _match_str, best_score, match_idx = res
                best_original = original_names2[match_idx]
            fuzzy_cache[n1] = (best_score, best_original)

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

        done_work += 1
        if progress is not None and (done_work % 25 == 0 or done_work == total_work):
            progress(done_work, total_work)

    return all_results, matches, not_found, duplicates_list1, duplicates_list2
