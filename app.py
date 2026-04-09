from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
from rapidfuzz import fuzz, process
import unicodedata
import io
import re
import base64
import os

app = FastAPI()

# Asegurar que existan los directorios
os.makedirs("templates", exist_ok=True)
os.makedirs("static", exist_ok=True)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def normalize_name(name):
    if pd.isna(name):
        return ""
    name = str(name).upper()
    # Quitar acentos (eliminar marcas diacríticas)
    name = ''.join((c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn'))
    # Eliminar espacios duplicados y extremos
    name = re.sub(r'\s+', ' ', name).strip()
    return name

@app.get("/", response_class=HTMLResponse)
async def read_item(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/compare")
async def compare_files(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    # 1. Leer ambos archivos Excel en memoria
    contents1 = await file1.read()
    contents2 = await file2.read()

    df1 = pd.read_excel(io.BytesIO(contents1))
    df2 = pd.read_excel(io.BytesIO(contents2))

    # 2. Tomar siempre la primera columna
    col1 = df1.columns[0]
    col2 = df2.columns[0]

    # 3. Normalizar nombres
    names1 = df1[col1].apply(normalize_name).tolist()
    original_names1 = df1[col1].tolist()
    
    df2['normalized'] = df2[col2].apply(normalize_name)
    names2 = df2['normalized'].tolist()
    original_names2 = df2[col2].tolist()
    
    # 4. Detectar duplicados en Archivo 2 basándonos en nombres normalizados
    duplicates_df2 = df2[df2.duplicated(subset=['normalized'], keep=False)]
    duplicates_list = duplicates_df2[col2].tolist()
    
    # Configurar diccionario para rapidfuzz excluyendo vacíos
    choices = {idx: name for idx, name in enumerate(names2) if name}
    
    all_results = []
    matches = []
    not_found = []
    
    # OPTIMIZACIÓN: Precomputar coincidencias exactas en O(1)
    exact_match_dict = {name: (idx, original) for idx, (name, original) in enumerate(zip(names2, original_names2)) if name}
    
    for i, n1 in enumerate(names1):
        if not n1:
            continue
            
        best_score = 0
        best_original = None
        
        # Fast path: si existe coincidencia exacta luego de normalizar
        if n1 in exact_match_dict:
            best_score = 100
            best_original = exact_match_dict[n1][1]
        else:
            # Fuzzy match usando token_sort_ratio
            res = process.extractOne(n1, choices, scorer=fuzz.token_sort_ratio, score_cutoff=1)
            if res:
                match_str, best_score, match_idx = res
                best_original = original_names2[match_idx]
                
        # Preparar la fila de resultado
        result_status = "COINCIDENCIA" if best_score >= 80 else "NO ENCONTRADO"
        
        res_row = {
            "Nombre Archivo 1": str(original_names1[i]),
            "Mejor Coincidencia Archivo 2": str(best_original) if best_original else "N/A",
            "Similitud (%)": round(best_score, 2),
            "Resultado": result_status
        }
        
        if result_status == "COINCIDENCIA":
            matches.append(res_row)
        else:
            not_found.append(res_row)
            
        all_results.append(res_row)
        
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
            
        if duplicates_list:
            pd.DataFrame({"Duplicados en Archivo 2": duplicates_list}).to_excel(writer, sheet_name='Duplicados detectados', index=False)
        else:
            pd.DataFrame(columns=["Duplicados en Archivo 2"]).to_excel(writer, sheet_name='Duplicados detectados', index=False)
            
    output.seek(0)
    # Codificamos el archivo en Base64 para que el frontend pueda descargarlo
    excel_b64 = base64.b64encode(output.read()).decode('utf-8')
    
    return {
        "results": all_results,
        "excel_b64": excel_b64
    }
