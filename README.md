# pricing-meta

Herramienta de Python para ingestar los archivos CSV de precios de la plataforma **WhatsApp Business (Meta)** en una base de datos **MariaDB**, manteniendo un historial completo de tarifas con rangos de fechas de vigencia.

---

## Contenido

- [Contexto](#contexto)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Modelo de base de datos](#modelo-de-base-de-datos)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso](#uso)
- [Archivos CSV soportados](#archivos-csv-soportados)
- [Multi-moneda](#multi-moneda)
- [Carga masiva](#carga-masiva)

---

## Contexto

Meta publica hasta 4 actualizaciones de precios al año para el envío de mensajes vía WhatsApp Business Platform. Cada actualización se distribuye en dos archivos CSV:

| Archivo | Descripción |
|---|---|
| `Pricing.csv` | Tarifas base (list rate) por mercado y tipo de mensaje |
| `Tier Pricing.csv` | Tarifas con descuento por volumen mensual (aplica solo a Utility y Authentication) |

Este proyecto carga ambos archivos en MariaDB preservando el historial: **nunca se eliminan tarifas anteriores**, sino que cada versión queda acotada a su rango de fechas de vigencia.

---

## Estructura del proyecto

```
pricing-meta/
├── .env.example            # Plantilla de variables de entorno
├── requirements.txt
├── config.py               # Configuración de BD leída desde .env
├── models.py               # Dataclasses: BaseRateRecord, TierRateRecord
├── main.py                 # CLI principal
├── csv/
│   └── Meta_Countries.csv  # Lista oficial de países y regiones de Meta
├── db/
│   ├── __init__.py
│   ├── connection.py               # Context manager transaccional para MariaDB
│   ├── initializer.py              # Crea tablas y siembra datos de referencia
│   ├── schema.sql                  # DDL de referencia (todas las tablas)
│   └── migrate_market_country.sql  # Migración para entornos con tablas existentes
├── loaders/
│   ├── __init__.py
│   ├── base_loader.py      # Parser de Pricing.csv
│   └── tier_loader.py      # Parser de Tier Pricing.csv
└── utils/
    ├── __init__.py
    └── date_utils.py       # Helpers de fechas
```

---

## Modelo de base de datos

### Diagrama de relaciones

```
country ──────────────────────────────────────────────────────────┐
  (waba_market_id)                                                 │
                                                                   ▼
waba_market ──────────────┐                               (mercado resuelto)
                          ├──→ waba_base_rate ──→ waba_pricing_load
waba_message_type ────────┤
                          └──→ waba_tier_rate ──→ waba_pricing_load
```

### Tablas

#### `waba_market`
Catálogo de mercados de facturación de Meta. Sembrado por `--init-db` a partir de `Meta_Countries.csv`.

Un mercado es una unidad de tarificación:
- **Individual** (`region IS NULL`): países con tarifa propia (ej. `Brazil`, `Mexico`, `United States`).
- **Regional** (`region IS NOT NULL`): grupo de países que comparten tarifa (ej. `Rest of Africa`).

La tabla `country` tiene `waba_market_id` → `waba_market.id`, permitiendo que múltiples países
resuelvan al mismo mercado regional.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | SMALLINT UNSIGNED | PK autoincremental |
| `name` | VARCHAR(100) | Nombre del mercado tal como aparece en los CSV de Meta |
| `region` | VARCHAR(100) | Región de `Meta_Countries.csv`; `NULL` = mercado de país individual |

#### `waba_message_type`
Catálogo fijo de tipos de mensaje de WhatsApp.

| Código | Nombre |
|---|---|
| `MARKETING` | Marketing |
| `UTILITY` | Utility |
| `AUTHENTICATION` | Authentication |
| `AUTH_INTL` | Authentication-International |
| `SERVICE` | Service |

#### `waba_pricing_load`
Registro de cada archivo CSV cargado. Es el **eje del versionado histórico**.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INT UNSIGNED | PK autoincremental |
| `currency` | CHAR(3) | Código ISO 4217 (ej. `USD`) |
| `file_type` | ENUM(`BASE`,`TIER`) | Tipo de archivo cargado |
| `file_name` | VARCHAR(255) | Nombre del archivo original |
| `valid_from` | DATE | Inicio del período de vigencia |
| `valid_to` | DATE | Fin del período (`NULL` = vigente actualmente) |
| `uploaded_at` | DATETIME | Fecha y hora de carga |
| `uploaded_by` | VARCHAR(100) | Usuario que realizó la carga |
| `notes` | TEXT | Notas libres |

> Cuando se carga un nuevo archivo, el script cierra automáticamente el período anterior asignando `valid_to = nuevo_valid_from − 1 día`.

#### `waba_base_rate`
Tarifas planas provenientes de `Pricing.csv`. Una fila por cada combinación `mercado × tipo_de_mensaje`.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | BIGINT UNSIGNED | PK autoincremental |
| `load_id` | INT UNSIGNED | FK → `waba_pricing_load` |
| `market_id` | SMALLINT UNSIGNED | FK → `waba_market` |
| `message_type_id` | TINYINT UNSIGNED | FK → `waba_message_type` |
| `rate` | DECIMAL(10,6) | Tarifa en la moneda del load (`NULL` = no aplica) |

#### `waba_tier_rate`
Tarifas por volumen provenientes de `Tier Pricing.csv`. Una fila por cada banda de volumen.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | BIGINT UNSIGNED | PK autoincremental |
| `load_id` | INT UNSIGNED | FK → `waba_pricing_load` |
| `market_id` | SMALLINT UNSIGNED | FK → `waba_market` |
| `message_type_id` | TINYINT UNSIGNED | FK → `waba_message_type` |
| `volume_from` | INT UNSIGNED | Inicio de la banda de volumen (mensajes/mes) |
| `volume_to` | INT UNSIGNED | Fin de la banda (`NULL` = ilimitado) |
| `rate_type` | ENUM(`LIST`,`TIER`) | `LIST` = tarifa base, `TIER` = tarifa con descuento |
| `rate` | DECIMAL(10,6) | Tarifa aplicable en la banda |
| `discount_pct` | TINYINT | Descuento vs. list rate: `0`, `-5`, `-10`, `-15`, `-20`, `-25` |

---

## Requisitos

- Python 3.10+
- MariaDB 10.6+

---

## Instalación

```bash
# 1. Clonar o descargar el proyecto
cd pricing-meta

# 2. (Opcional) Crear entorno virtual
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
.venv\Scripts\activate           # Windows

# 3. Instalar dependencias
pip install -r requirements.txt
```

---

## Configuración

Copia `.env.example` a `.env` y completa los valores:

```bash
cp .env.example .env
```

```ini
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=tu_contraseña
DB_NAME=whatsapp_pricing
```

Asegúrate de que la base de datos exista antes de continuar:

```sql
CREATE DATABASE IF NOT EXISTS whatsapp_pricing
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

---

## Uso

### 1. Inicializar el esquema (primera vez)

Crea las tablas, siembra los 5 tipos de mensaje y los 33 mercados de facturación de Meta.
Es idempotente: seguro de ejecutar varias veces.

```bash
python main.py --init-db
```

Alternativamente, puedes ejecutar el DDL directamente:

```bash
mysql -u root -p whatsapp_pricing < db/schema.sql
```

**Si las tablas ya existen** (entorno previo), ejecuta la migración en su lugar:

```bash
mysql -u root -p whatsapp_pricing < db/migrate_market_country.sql
```

> La migración agrega la columna `region` a `waba_market`, siembra los mercados,
> y agrega la columna `waba_market_id` a la tabla `country` con su mapeo completo.

### 2. Cargar un archivo de precios

```bash
# Carga básica — el período queda abierto (valid_to = NULL)
python main.py --file "Pricing.csv" --valid-from 2026-01-01

# Carga con metadatos opcionales
python main.py --file "Tier Pricing.csv" --valid-from 2026-01-01 \
    --uploaded-by "nombre.apellido" \
    --notes "Precios Q1 2026"

# Especificar fecha de cierre manualmente
python main.py --file "Pricing.csv" --valid-from 2026-01-01 --valid-to 2026-03-31
```

> El período anterior se cierra automáticamente al cargar el siguiente archivo de la misma moneda y tipo.

### 3. Actualización trimestral

```bash
# Q1 → carga inicial (período abierto)
python main.py --file "Pricing.csv"      --valid-from 2026-01-01
python main.py --file "Tier Pricing.csv" --valid-from 2026-01-01

# Q2 → cierra Q1 automáticamente (valid_to = 2026-03-31) y abre Q2
python main.py --file "Pricing.csv"      --valid-from 2026-04-01
python main.py --file "Tier Pricing.csv" --valid-from 2026-04-01
```

### 4. Dry-run (sin escritura en BD)

Útil para validar el archivo antes de cargarlo:

```bash
python main.py --file "Pricing.csv" --valid-from 2026-04-01 --dry-run
```

### Referencia de argumentos

| Argumento | Requerido | Descripción |
|---|---|---|
| `--init-db` | — | Inicializa el esquema de BD |
| `--file PATH` | Sí* | Ruta al archivo CSV a cargar |
| `--dir DIR` | Sí* | Directorio con archivos `.csv` a cargar en bloque |
| `--valid-from YYYY-MM-DD` | Sí* | Fecha de inicio de vigencia |
| `--valid-to YYYY-MM-DD` | No | Fecha de fin de vigencia (por defecto queda abierta) |
| `--uploaded-by NAME` | No | Nombre del usuario que realiza la carga |
| `--notes TEXT` | No | Comentario libre asociado a la carga |
| `--dry-run` | No | Parsea el CSV y reporta conteos sin escribir en BD |

*`--init-db`, `--file` y `--dir` son mutuamente excluyentes. `--valid-from` es requerido con `--file` y `--dir`.

---

## Archivos CSV soportados

El script detecta automáticamente el tipo de archivo por su contenido:

| Tipo | Señal de detección | Registros típicos |
|---|---|---|
| `BASE` | Primera línea **no** contiene "volume tier" | 160 (32 mercados × 5 tipos) |
| `TIER` | Primera línea contiene "volume tier" | ~438 (varía según mercados con AUTH_INTL) |

---

## Carga masiva

Cuando se dispone de múltiples archivos CSV (por ejemplo, varias monedas en un mismo trimestre), se puede usar `--dir` en lugar de `--file` para procesarlos todos en una sola invocación.

```bash
# Cargar todos los .csv de una carpeta
python main.py --dir "./csv/" --valid-from 2026-01-01

# Dry-run masivo para validar antes de escribir en BD
python main.py --dir "./csv/" --valid-from 2026-01-01 --dry-run

# Con metadatos opcionales
python main.py --dir "./csv/" --valid-from 2026-04-01 \
    --uploaded-by "nombre.apellido" \
    --notes "Q2 2026 — todas las monedas"
```

**Comportamiento:**
- Los archivos se procesan en **orden alfabético**.
- Un fallo en un archivo se registra en el log y el proceso **continúa** con los demás.
- Al finalizar se imprime un resumen `X/N file(s) loaded successfully`.
- El exit code es `1` si algún archivo falló.

---

## Multi-moneda

Los archivos CSV de Meta están disponibles en distintas monedas (USD, MXN, BRL, etc.). Todas las monedas conviven en las **mismas tablas**, diferenciadas por la columna `currency` en `waba_pricing_load`.

Cada moneda tiene su propio ciclo de actualizaciones independiente: actualizar las tarifas en USD no afecta las filas vigentes en MXN.

> **Nota:** Meta publica la moneda como `$US` en lugar del código ISO `USD`. El script normaliza este valor automáticamente. Para agregar alias de otras monedas, edita el diccionario `_CURRENCY_ALIASES` en `main.py` y en los loaders correspondientes.
