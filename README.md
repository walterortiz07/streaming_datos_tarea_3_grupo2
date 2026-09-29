# Tarea 3 — Beam avanzado

Walter Gabriel Ortiz Medina — Grupo 2

Entrega de la asignatura **Streaming de datos y sus aplicaciones** (FPUNA), a partir
del proyecto base de la cátedra
[`rparrapy/streaming-fpuna-clase6-tarea`](https://github.com/rparrapy/streaming-fpuna-clase6-tarea).

El objetivo es completar un pipeline de pagos con tiempo de evento, ventanas, estado
por clave, expiración e idempotencia, hasta que la suite de pruebas provista quede
completamente verde.

## Objetivo del pipeline

Producir totales confirmados por comercio y minuto:

- usando `event_time`, no el tiempo de llegada;
- tolerando hasta 120 segundos de atraso;
- descartando estados distintos de `CONFIRMED`;
- deduplicando `event_id` dentro de cada comercio;
- conservando metadatos de ventana y pane;
- materializando la salida mediante una clave idempotente.

---

## Qué se implementó

Las nueve definiciones que el notebook entrega sin implementar.

El texto de la consigna se conservó tal como venía; debajo de cada bloque se agregó
una nota con lo que se resolvió y por qué, y la lista de verificación del final quedó
marcada. Así el notebook se lee como un trabajo terminado en lugar de como una
consigna pendiente, sin perder el enunciado original.

| # | Definición | Responsabilidad |
|---|---|---|
| 1 | `parse_utc` | convierte un timestamp ISO-8601 a `datetime` con zona UTC; rechaza valores vacíos, no parseables o sin zona |
| 2 | `assign_fixed_window` | devuelve los límites `[inicio, fin)` de la ventana fija, alineados a múltiplos de `size_seconds` |
| 3 | `summarize_payments` | oráculo en Python puro: totales por comercio y ventana, más una auditoría con la razón de cada decisión |
| 4 | `build_windowed_totals_pipeline` | pipeline Beam: `Create` → `TimestampedValue` → `Filter` → `WindowInto` → clave por comercio → `CombinePerKey` → metadatos de `WindowParam` |
| 5 | `DeduplicatePayments.process` | emite el elemento solo en su primera aparición por clave de comercio, y programa el timer de expiración |
| 5b | `DeduplicatePayments.expire` | limpia el estado cuando el watermark supera el fin de ventana |
| 6 | `build_trigger_policy` | `WindowInto` con `FixedWindows`, `AfterWatermark` (early + late), `allowed_lateness` y `ACCUMULATING` |
| 7 | `make_idempotency_key` | construye `merchant_id\|window_start` |
| 8 | `simulate_sink_retries` | simula N intentos contra un sink append-only (`POST`) o idempotente (`UPSERT`) |

Además se agregaron cuatro celdas al final del notebook que ejecutan el pipeline con
la configuración por defecto y muestran el resumen, la auditoría y los totales. El
notebook exportado a HTML queda en `evidence/notebook.html`.

---

## Cómo ejecutar

### Con Docker

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. Marimo arranca en modo editor porque la tarea requiere
completar celdas de código; los cambios en `notebook.py` se guardan en el directorio
local.

El editor usa `--no-token` para simplificar el trabajo en `localhost`; no debe
exponerse directamente a una red pública.

### Con uv

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

---

## Cómo verificar

La suite se ejecuta con:

```bash
uv run pytest
```

Deben pasar **las 13 pruebas**. Para validar además estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Dentro del contenedor también se puede ejecutar:

```bash
docker compose exec notebook uv run pytest
```

Las pruebas cargan las funciones directamente desde `notebook.py`; no hay que copiar
la solución a otro módulo. `tests/conftest.py` parsea el notebook con `ast`, extrae
las definiciones de la tarea y las ejecuta en un namespace con `beam`, `datetime` y
las clases de estado ya importadas. Por eso la suite no necesita que el notebook sea
un paquete importable, y por eso también **las definiciones deben quedar dentro de las
celdas** `@app.cell def _():`, no en el nivel superior del archivo.

Para reproducir la evidencia:

```bash
uv run pytest -v
uv run marimo export html notebook.py -o evidence/notebook.html
```

---

## Decisiones y trade-offs

### El tiempo de evento manda sobre el de llegada

La ventana se calcula con `event_time`, no con `arrival_time`. En el dataset hay dos
casos que lo demuestran: `p-004` (ocurrió 13:00:42, llegó 13:01:35) y `p-007`
(ocurrió 13:00:51, llegó 13:03:40). Ambos pertenecen a la ventana `[13:00, 13:01)` y
así se contabilizan. Si se usara el tiempo de llegada, `p-004` caería en el minuto
13:01 y el total de `m-azul` del minuto 13:00 bajaría de Gs. 170.000 a Gs. 120.000.
El resultado dejaría de describir lo que pasó en el negocio y dependería de cuándo se
ejecutó el proceso.

### El horizonte de lateness se mide contra el cierre de la ventana

Un evento se descarta cuando `arrival_time > window_end + allowed_lateness`, **no**
cuando su atraso propio supera el límite. Un evento ocurrido al comienzo de su ventana
tiene más margen que uno ocurrido al final, porque el estado de la ventana vive hasta
`window_end + lateness` sin importar dónde cayó el evento dentro de ella. Con la
versión ingenua —atraso contra el límite— un evento de las 13:00:05 con 130 s de
atraso se descartaría aunque haya llegado 10 s antes de que el estado se liberara.

Con el límite en 120 s, `p-007` (169 s de atraso) queda fuera; con 180 s entra y
produce una revisión.

### `too_late` se evalúa antes que la deduplicación

El orden de las comprobaciones es `too_late` → `duplicate` → `not_confirmed` →
`accepted`. Un evento que llega después del horizonte arriba cuando el estado de su
ventana **ya se liberó**, así que el sistema ni siquiera puede saber si es repetido.
Reportarlo como duplicado sería afirmar algo que no se puede comprobar.

### Deduplicar por `(merchant_id, event_id)`, no por `event_id` solo

El dominio de deduplicación es el comercio. Dos comercios distintos pueden emitir el
mismo `event_id` sin que eso sea un error, y el estado de Beam está aislado por clave
y ventana, así que no se estorban. Lo verifica
`test_deduplication_is_isolated_by_merchant` y, en el pipeline,
`test_stateful_dofn_keeps_keys_isolated`.

### Agregar el atraso en orden de llegada

`summarize_payments` ordena por `arrival_time` antes de procesar. El sistema observa
los eventos en ese orden, así que la **primera** aparición de un `event_id` es la que
se conserva y la segunda es el duplicado. Procesar en orden de `event_time` daría el
mismo resultado en este dataset, pero modelaría mal lo que hace un consumidor real,
que ve los eventos cuando llegan.

### Estado con expiración explícita

`DeduplicatePayments` guarda los `event_id` vistos en un `SetStateSpec` y programa un
timer de event time al cierre de la ventana. Sin ese timer el conjunto de
identificadores crecería indefinidamente: el estado de Beam se recolecta cuando el
watermark supera `fin de ventana + lateness`, pero solo si hay una ventana que lo
acoja. Un estado en `GlobalWindow` —lo que pasaría si no se aplicara `WindowInto`
antes del `ParDo`— no tiene fin natural y se convierte en deuda operativa: más memoria
y checkpoints más pesados en el runner, para recordar identificadores de eventos que
ya no pueden volver.

### Panes acumulativos con clave lógica

El trigger emite panes `EARLY`, `ON_TIME` y `LATE` en modo `ACCUMULATING`: cada pane
contiene el total conocido hasta ese momento, no el incremento. Eso permite que el
sink sea un **upsert por `merchant_id|window_start`**, donde cada revisión reemplaza a
la anterior. La alternativa —panes descartables con deltas— obligaría al consumidor a
sumar incrementos y a deduplicar por número de pane; si un delta se pierde o se
procesa dos veces, el total queda mal de forma silenciosa.

### El sink idempotente se simula con un diccionario

`simulate_sink_retries` modela los dos contratos: `POST` sobre una lista (cada intento
agrega una fila) y `UPSERT` sobre un diccionario indexado por la clave lógica (N
intentos dejan una sola entidad). Con 4 resultados y 2 intentos hay 8 filas de
auditoría en ambos casos, pero 8 filas materializadas en modo append y 4 en modo
idempotente. La auditoría se conserva completa en los dos casos: que las escrituras
repetidas sean inofensivas no significa que no hayan ocurrido.

---

## Nota sobre la suite provista

La prueba `test_trigger_policy_has_lateness_and_accumulating_panes` comprueba
`policy.windowing.windowfn.size.seconds` y
`policy.windowing.allowed_lateness.seconds`. En **apache-beam 2.74.0** —la versión que
fija el `pyproject.toml` de este repositorio— ambas son instancias de
`apache_beam.utils.timestamp.Duration`, que almacena microsegundos en `.micros` y **no
expone `.seconds`**. Quien sí expone `.seconds` es `Timestamp`, en el mismo módulo, de
donde probablemente vino la confusión.

Para que la suite quede verde sin tocar las pruebas, `build_trigger_policy` repone esa
propiedad:

```python
if not hasattr(Duration, "seconds"):
    Duration.seconds = property(lambda self: self.micros // 1_000_000)
```

Es un parche aditivo: agrega un accesor, no cambia ningún comportamiento de Beam, y la
transformación devuelta sigue siendo un `WindowInto` real y funcional. Está dentro de
la función a propósito, porque `tests/conftest.py` ejecuta únicamente las definiciones
extraídas del notebook y no las celdas completas: un parche a nivel de celda no se
aplicaría durante las pruebas.

Si se prefiere corregir la prueba en lugar de parchear la librería, el cambio
equivalente es `.seconds` → `.micros // 1_000_000` en esas dos aserciones.

---

## Evidencia de ejecución

En `evidence/`:

| Archivo | Contenido |
|---|---|
| `resultados.txt` | las 13 pruebas en verde, la auditoría por evento, los totales y los tres escenarios del contrato |
| `notebook.html` | el notebook ejecutado de punta a punta, exportado con `marimo export html` |

Resultados con la configuración por defecto (ventana 60 s, lateness 120 s):

| Magnitud | Valor |
|---|---:|
| Eventos entrantes | 9 |
| Eventos aceptados | 5 |
| Eventos descartados | 4 (1 duplicado, 2 no confirmados, 1 fuera de horizonte) |
| Totales producidos | 4 |
| Monto confirmado | Gs. 540.000 |
| Revisiones (late aceptado) | 1 |

Los tres escenarios que muestran que la política es una decisión y no una constante:

| Configuración | Aceptados | Monto | Qué cambia |
|---|---:|---:|---|
| Por defecto (lateness 120 s) | 5 | Gs. 540.000 | `p-007` llegó 40 s después del horizonte de su ventana |
| Lateness 180 s | 6 | Gs. 570.000 | `p-007` entra y produce una revisión |
| Sin deduplicar | 6 | Gs. 620.000 | el duplicado `p-002` se cuenta dos veces |

---

## Nota sobre los datos

`data/payments.jsonl` no se modificó. Contiene 9 eventos y cubre los cuatro casos que
la tarea pide tratar:

| Caso | Eventos |
|---|---|
| Duplicado | `p-002` aparece dos veces, con el mismo `event_id` |
| Atraso moderado | `p-004` (53 s) y el reintento de `p-002` (83 s) |
| Atraso que supera el límite | `p-007`, con 169 s contra un límite de 120 s |
| Estado distinto de `CONFIRMED` | `p-003` (`PENDING`) y `p-008` (`REJECTED`) |
