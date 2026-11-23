# Comparador de Nombres

Compara dos listas de nombres en archivos Excel y detecta coincidencias exactas o
aproximadas con **fuzzy matching** (RapidFuzz). Pensada para datos reales con
diferencias de acentos, mayúsculas, puntuación, orden invertido o typos.

## Requisitos del Excel

- La **primera fila** de cada hoja se usa como **encabezado** y no se compara.
  Pon un título (ej. `NOMBRE`) en la fila 1 y los datos desde la fila 2.
- Formatos: `.xlsx`, máximo 50 MB por archivo.

## Uso local (Windows)

Doble clic en `iniciar.bat`: crea el venv, instala dependencias, arranca el
servidor y abre `http://127.0.0.1:8000`.

Manual:

```bat
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8000
```

`iniciar.bat` es solo local (está en `.gitignore`, no se sube a Git/Render).

## Cómo funciona

1. Sube el **Archivo 1** (lista principal) y elige hoja + columna.
2. Sube el **Archivo 2** (lista de referencia) y elige hoja + columna.
3. Ajusta el **umbral** (default 85/100) y presiona **Comparar**.
4. Revisa el resumen, filtra la tabla de detalle y **exporta el Excel**
   (`Coincidencias`, `No encontrados`, `Duplicados detectados`,
   `Duplicados archivo 1`).

Normalización: mayúsculas, sin acentos (Ñ→N), sin puntuación, sin espacios
duplicados. Scorer: `max(token_sort_ratio, WRatio)` — tolera orden invertido,
segundo apellido y typos.

## API

| Método | Ruta       | Descripción                              |
| ------ | ---------- | ---------------------------------------- |
| GET    | `/`        | Interfaz web                             |
| GET    | `/health`  | Healthcheck (Render) → `{"status":"ok"}` |
| POST   | `/sheets`  | Hojas del Excel y columnas de la 1ª hoja (`file`) |
| POST   | `/headers` | Columnas de una hoja (`file`, `sheet_name`) |
| POST   | `/compare` | Compara directo/síncrono (`file1`, `file2`, `col1_name`, `col2_name`, `sheet1_name`, `sheet2_name`, `threshold`=85) |
| POST   | `/start`   | Job async (mismos campos; responde `202` con `job_id`) |
| GET    | `/progress/{job_id}` | Avance `{status, stage, processed, total}`; con `status=done` incluye el resultado |
| GET    | `/download/{job_id}` | Descarga binaria (`.xlsx`) del resultado de un job |
| DELETE | `/progress/{job_id}` | Cancela el job (o lo limpia si ya terminó) |

La interfaz web usa `/start` para mostrar barra de progreso real y botón Cancelar.

Errores de validación → `400` (o `413` si supera 50 MB) con `{"detail": ...}`.

## Tests

```bat
.\.venv\Scripts\python.exe -m pip install pytest httpx
.\.venv\Scripts\python.exe -m pytest tests/ -q
```

Cubren normalización, scorer, umbral, validación de archivos y comparación
(duplicados, primera ocurrencia, vacíos). `pytest` es solo dev, no va en
`requirements.txt` para no engordar el deploy.

## Deploy en Render

Web Service con auto-deploy desde `main`:

- Build: `pip install -r requirements.txt`
- Start: `uvicorn app:app --host 0.0.0.0 --port $PORT` (1 solo *worker*, ya que los jobs `/start` se guardan en memoria del proceso)
- Health check path: `/health`

Cada push a `main` despliega. Flujo seguro: validar local (`pytest` + una
comparación real) → commit pequeño → push → verificar `Live` y probar la URL.
Rollback desde el dashboard si algo falla.

## Estructura

```text
app.py               # FastAPI, endpoints y gestión de jobs
matching.py          # Normalización y comparación fuzzy (RapidFuzz)
excel_io.py          # Validación, lectura y exportación Excel
templates/index.html # UI (el JS/CSS vive en static/)
static/app.js        # Frontend
static/styles.css    # Estilos
tests/               # pytest
```
