# CLAUDE.md — pricing-meta

Contexto y guía para Claude Code al trabajar en este proyecto.

---

## Propósito del proyecto

Ingesta de archivos CSV de precios de la plataforma **WhatsApp Business (Meta)** en **MariaDB**.
Meta publica hasta 4 actualizaciones por año en dos archivos:

- `Pricing.csv` — tarifas base (list rate) por mercado y tipo de mensaje (`FILE_TYPE=BASE`)
- `Tier Pricing.csv` — tarifas con descuento por volumen mensual (`FILE_TYPE=TIER`)

El sistema **nunca elimina tarifas anteriores**: cada versión queda acotada por `valid_from`/`valid_to`.

---

## Stack

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.10+ |
| Base de datos | MariaDB 10.6+ |
| Dependencias | `pandas`, `mysql-connector-python`, `python-dotenv` |
| CLI | `argparse` (en `main.py`) |

---

## Estructura de archivos

```
pricing-meta/
├── CLAUDE.md
├── README.md
├── .env                    # credenciales (no versionar)
├── .env.example
├── requirements.txt
├── config.py               # DatabaseConfig dataclass, carga .env
├── models.py               # BaseRateRecord, TierRateRecord, FileType, RateType
├── main.py                 # CLI: --init-db | --file | --dir + --valid-from
├── csv/
│   └── Meta_Countries.csv  # Lista oficial de países y regiones publicada por Meta
├── db/
│   ├── __init__.py
│   ├── connection.py           # get_connection() context manager transaccional
│   ├── initializer.py          # init_schema(): CREATE TABLE IF NOT EXISTS + seed
│   ├── schema.sql              # DDL de referencia (no ejecutado por el script)
│   └── migrate_market_country.sql  # Migración para entornos con tablas ya existentes
├── loaders/
│   ├── __init__.py         # expone load_base_rates, load_tier_rates
│   ├── base_loader.py      # parser de Pricing.csv
│   └── tier_loader.py      # parser de Tier Pricing.csv
└── utils/
    ├── __init__.py
    └── date_utils.py       # parse_date(), day_before()
```

---

## Modelo de base de datos

5 tablas (prefijo `waba_`) + integración con tablas externas:

```
waba_market         → catálogo de mercados de facturación de Meta
                      (sembrado por --init-db desde Meta_Countries.csv)
waba_message_type   → catálogo de tipos: MARKETING, UTILITY, AUTHENTICATION,
                      AUTH_INTL, SERVICE  (sembrado por --init-db)
waba_pricing_load   → una fila por archivo CSV cargado (versioning anchor)
waba_base_rate      → tarifas planas (Pricing.csv)
waba_tier_rate      → tarifas por volumen (Tier Pricing.csv)
```

### Mercados de facturación (`waba_market`)

Un mercado es una unidad de tarificación de Meta. Existen dos tipos:

- **Mercado individual** (`region IS NULL`): países con tarifa propia (ej. `Brazil`, `Mexico`).
- **Mercado regional** (`region IS NOT NULL`): grupo de países que comparten tarifa (ej. `Rest of Africa`).

La tabla externa `country` tiene una columna `waba_market_id` que apunta a `waba_market.id`,
permitiendo que múltiples países resuelvan al mismo mercado regional.

```
country.waba_market_id ──→ waba_market.id ──→ waba_base_rate / waba_tier_rate
```

### Relación con tablas externas

| Tabla externa | Columna nueva | Apunta a |
|---|---|---|
| `country` | `waba_market_id` | `waba_market.id` |

Para entornos con tablas ya existentes, ejecutar: `db/migrate_market_country.sql`

### Regla de vigencia

`waba_pricing_load` tiene `valid_from` y `valid_to` (NULL = vigente).
Al cargar un nuevo archivo, el script cierra el load activo anterior:
```
valid_to = nuevo_valid_from - 1 día
```
La unicidad está garantizada por: `UNIQUE KEY uq_load (currency, file_type, valid_from)`.

---

## Detalles de parseo de CSV (críticos)

### Pricing.csv (BASE)
- Filas lógicas 0-4: metadata/notas → saltar con `skiprows=5, header=0`
- Detección de moneda: `skiprows=6` (hay una fila extra de header)
- La columna `"Authentication-\nInternational"` tiene un salto de línea en el nombre → se limpia con `.replace("\n", "")`
- Output: 160 registros (32 mercados × 5 tipos de mensaje)

### Tier Pricing.csv (TIER)
- Filas lógicas 0-4: title + URL + section headers + sub-headers + column headers → saltar con `skiprows=5, header=None, names=_RAW_COLUMNS`
- El campo "Market\n(per rate card)" tiene salto de línea (quoted) → pandas lo maneja solo
- 17 columnas: `market`, `currency` + 5 cols × 3 grupos (Utility, Authentication, AUTH_INTL)
- La columna `market` se rellena hacia abajo (`ffill`) porque solo aparece en la primera fila del grupo
- Filas con grupo completamente en `n/a` se omiten
- Detección de moneda: `skiprows=5`
- Output: ~438 registros (varía según mercados con AUTH_INTL)

### Normalización de moneda
Meta usa `$US` en lugar del código ISO `USD`.
Diccionario de alias en `main.py` y en cada loader:
```python
_CURRENCY_ALIASES = {"$US": "USD"}
```
Para agregar una nueva moneda, actualizar este dict en los tres archivos.

---

## Convenciones del proyecto

- **Type hints** en todas las funciones públicas.
- **Docstrings** en todas las funciones y módulos públicos.
- **`logging`** en lugar de `print`; nivel INFO por defecto.
- **Transacciones explícitas**: `get_connection()` hace commit en éxito y rollback en excepción.
- **Dataclasses frozen** para los records (`BaseRateRecord`, `TierRateRecord`).
- Ninguna operación de BD fuera de `main.py`; los loaders solo parsean y retornan listas.
- No hay lógica de negocio en `db/connection.py` ni en `db/initializer.py`.

---

## Comandos frecuentes

```bash
# Instalar dependencias
pip install -r requirements.txt

# Inicializar esquema (primera vez, idempotente)
python main.py --init-db

# Validar un archivo sin escribir en BD
python main.py --file "Pricing.csv" --valid-from 2026-01-01 --dry-run

# Carga real — archivo individual
python main.py --file "Pricing.csv"      --valid-from 2026-01-01
python main.py --file "Tier Pricing.csv" --valid-from 2026-01-01

# Carga masiva — todos los .csv de un directorio
python main.py --dir "./csv/" --valid-from 2026-01-01
python main.py --dir "./csv/" --valid-from 2026-01-01 --dry-run

# Carga del trimestre siguiente (cierra Q1 automáticamente)
python main.py --file "Pricing.csv"      --valid-from 2026-04-01 --uploaded-by "user"
python main.py --file "Tier Pricing.csv" --valid-from 2026-04-01 --uploaded-by "user"
python main.py --dir "./csv/"            --valid-from 2026-04-01 --uploaded-by "user"
```

---

## Puntos de extensión conocidos

| Necesidad | Dónde actuar |
|---|---|
| Nueva moneda con alias distinto | `_CURRENCY_ALIASES` en `main.py`, `base_loader.py` y `tier_loader.py` |
| Nuevo tipo de mensaje | Insertar en `waba_message_type` + actualizar `_COLUMN_TO_MSG_TYPE` en `base_loader.py` |
| Nuevo archivo CSV con estructura diferente | Crear nuevo loader en `loaders/`, registrar tipo en `FileType` enum (`models.py`) y en `run_load()` de `main.py` |
| Nuevo mercado individual publicado por Meta | Agregar fila en `_SEED_MARKETS` de `initializer.py` y en `migrate_market_country.sql` |
| Nuevo mercado regional publicado por Meta | Igual que el anterior + mapear los países correspondientes en el paso 4 del script de migración |
| Calcular costo de un mensaje enviado | JOIN: `messages` → `wa_contacts` → `country.waba_market_id` → `waba_market` → `waba_base_rate` / `waba_tier_rate` filtrando por `valid_from`/`valid_to` |

---

## Lo que este proyecto NO hace

- No gestiona la tabla de mensajes enviados ni el consumo real de tarifas.
- No convierte entre monedas.
- No descarga los CSV automáticamente desde Meta (carga manual).
