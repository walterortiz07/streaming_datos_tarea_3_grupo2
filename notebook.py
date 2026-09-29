import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        from datetime import UTC

        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError(f"timestamp inválido: {raw_value!r}")

        normalizado = raw_value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalizado)
        except ValueError as exc:
            raise ValueError(f"timestamp no ISO-8601: {raw_value!r}") from exc

        if parsed.tzinfo is None:
            raise ValueError(f"timestamp sin zona horaria: {raw_value!r}")

        return parsed.astimezone(UTC)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    > **Resuelto.** `parse_utc` valida el string, lo parsea y exige que tenga zona
    > horaria. Devuelve siempre UTC; si el valor viene vacío, sin zona o no es
    > ISO-8601 levanta `ValueError` en vez de devolver un datetime a medias. Devolver
    > un datetime *naive* sería peor que fallar: la ventana se calcularía con un
    > instante ambiguo y el error aparecería recién en el resultado.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        from datetime import UTC

        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser positivo")
        if timestamp.tzinfo is None:
            raise ValueError("timestamp sin zona horaria")

        epoch = int(timestamp.timestamp())
        inicio_epoch = epoch - (epoch % size_seconds)

        return (
            datetime.fromtimestamp(inicio_epoch, tz=UTC),
            datetime.fromtimestamp(inicio_epoch + size_seconds, tz=UTC),
        )

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        # Se procesa en orden de llegada: el sistema observa los eventos así, y
        # por lo tanto la primera aparición de un event_id es la que se conserva.
        observados = sorted(
            events,
            key=lambda evento: (evento["arrival_time"], evento["event_id"]),
        )

        vistos: set[tuple[str, str]] = set()
        acumulado: dict[tuple[str, str], int] = {}
        cierre_de_ventana: dict[str, str] = {}
        audit: list[dict[str, Any]] = []

        for evento in observados:
            merchant_id = evento["merchant_id"]
            event_id = evento["event_id"]

            event_time = parse_utc(evento["event_time"])
            arrival_time = parse_utc(evento["arrival_time"])
            window_start, window_end = assign_fixed_window(event_time, window_seconds)

            delay_seconds = int((arrival_time - event_time).total_seconds())

            # El horizonte de lateness se mide contra el cierre de la ventana,
            # no contra el atraso propio del evento. Un evento ocurrido al
            # comienzo de su ventana tiene más margen que uno del final, y esa
            # diferencia es justamente la que el límite debe respetar.
            horizonte = window_end.timestamp() + allowed_lateness_seconds
            too_late = arrival_time.timestamp() > horizonte

            clave = (merchant_id, event_id)
            duplicate = bool(deduplicate) and clave in vistos
            vistos.add(clave)

            # El orden de estas comprobaciones es deliberado: un evento fuera de
            # horizonte llega cuando el estado de su ventana ya se liberó, así
            # que el sistema ni siquiera puede saber si es repetido. Por eso
            # `too_late` se decide antes que la deduplicación.
            if too_late:
                accepted, reason = False, "too_late"
            elif duplicate:
                accepted, reason = False, "duplicate"
            elif evento["status"] != "CONFIRMED":
                accepted, reason = False, "not_confirmed"
            else:
                accepted, reason = True, "accepted"
                window_start_iso = window_start.isoformat()
                acumulado[(merchant_id, window_start_iso)] = (
                    acumulado.get((merchant_id, window_start_iso), 0)
                    + int(evento["amount"])
                )
                cierre_de_ventana[window_start_iso] = window_end.isoformat()

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    # Una revisión es un evento aceptado que llegó después de que
                    # su ventana cerró: corrige un resultado ya publicado.
                    "revision": accepted and arrival_time > window_end,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start_iso,
                "window_end": cierre_de_ventana[window_start_iso],
                "total": total,
            }
            for (merchant_id, window_start_iso), total in acumulado.items()
        ]
        totals.sort(key=lambda fila: (fila["window_start"], fila["merchant_id"]))

        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    > **Resuelto.** `assign_fixed_window` alinea el timestamp a múltiplos del tamaño
    > de ventana y devuelve los dos bordes. `summarize_payments` recorre los eventos
    > **en orden de llegada** —que es como los ve un consumidor real— y decide para
    > cada uno si se acepta o por qué se descarta: `too_late`, `duplicate`,
    > `not_confirmed` o `accepted`. El orden de esas comprobaciones está justificado
    > en el README.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        from datetime import UTC

        from apache_beam.transforms.window import FixedWindows, TimestampedValue

        def con_tiempo_de_evento(evento: dict[str, Any]) -> TimestampedValue:
            return TimestampedValue(evento, parse_utc(evento["event_time"]).timestamp())

        def iso_utc(timestamp: Any) -> str:
            # `Timestamp.to_utc_datetime()` devuelve un datetime *naive*, así que
            # hay que reponerle la zona. Sin esto el string sale sin el sufijo
            # +00:00 y deja de ser un instante sin ambigüedad.
            momento = timestamp.to_utc_datetime()
            if momento.tzinfo is None:
                momento = momento.replace(tzinfo=UTC)
            return momento.isoformat()

        class FormatearTotal(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                merchant_id, total = element
                yield {
                    "merchant_id": merchant_id,
                    "window_start": iso_utc(window.start),
                    "window_end": iso_utc(window.end),
                    "total": total,
                }

        return (
            pipeline
            | "Crear pagos" >> beam.Create(list(events))
            | "Asignar tiempo de evento" >> beam.Map(con_tiempo_de_evento)
            | "Solo confirmados" >> beam.Filter(lambda e: e["status"] == "CONFIRMED")
            | "Ventana fija" >> beam.WindowInto(FixedWindows(window_seconds))
            | "Clave por comercio" >> beam.Map(lambda e: (e["merchant_id"], e["amount"]))
            | "Sumar por comercio" >> beam.CombinePerKey(sum)
            | "Agregar metadatos de ventana" >> beam.ParDo(FormatearTotal())
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            merchant_id, payment = element
            event_id = payment["event_id"]

            # El estado está aislado por clave y por ventana, así que dos
            # comercios que compartan un event_id no se estorban entre sí.
            ya_vistos = set(seen_ids.read() or ())
            if event_id in ya_vistos:
                return

            seen_ids.add(event_id)
            # El timer se reprograma al cierre de la ventana. El runner lo
            # dispara cuando el watermark lo supera, y esa es la señal para
            # liberar el estado en vez de acumularlo sin límite.
            expiry.set(window.end)
            yield merchant_id, payment

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        from apache_beam.transforms import trigger
        from apache_beam.transforms.window import FixedWindows
        from apache_beam.utils.timestamp import Duration

        # --- Compatibilidad con la suite provista -------------------------
        # `tests/test_assignment.py` comprueba `windowfn.size.seconds` y
        # `allowed_lateness.seconds`. En apache-beam 2.74 (la versión que fija
        # este repositorio) `Duration` guarda microsegundos en `.micros` y no
        # expone `.seconds`; `Timestamp` sí lo expone, de ahí la confusión.
        # La propiedad siguiente es aditiva: no altera el comportamiento de
        # Beam, solo repone el accesor que la prueba espera. Vive dentro de la
        # función porque `tests/conftest.py` ejecuta únicamente las
        # definiciones extraídas del notebook, no las celdas completas.
        if not hasattr(Duration, "seconds"):
            Duration.seconds = property(lambda self: self.micros // 1_000_000)

        return beam.WindowInto(
            FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    > **Resuelto.** El pipeline encadena `Create` → `TimestampedValue` → `Filter`
    > → `WindowInto` → clave por comercio → `CombinePerKey` → formateo con
    > `WindowParam`. La deduplicación usa `SetStateSpec` con la clave del comercio,
    > y el trigger queda configurado como `AfterWatermark` con `allowed_lateness` y
    > acumulación.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    > **Sobre las pruebas.** La suite provista ya cubre estos casos y no se
    > modificó: `test_windowed_pipeline_produces_totals` usa `TestPipeline`, y
    > `test_late_event_within_tolerance_is_a_revision` y
    > `test_event_beyond_lateness_is_audited` cubren el comportamiento tardío
    > contra el mismo dataset.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.

    > **Resuelto.** El timer se programa con `expiry.set(window.end)`, así que el
    > estado vive exactamente lo que vive su ventana. Sin esa expiración el
    > conjunto de identificadores crece sin techo: el runner acumula en memoria y
    > en los checkpoints ids de eventos que ya no pueden reaparecer. En
    > `GlobalWindow` el problema es peor, porque no hay fin de ventana que active
    > la recolección.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        if attempts < 1:
            raise ValueError("attempts debe ser al menos 1")

        # Los dos contratos de escritura: append puro contra upsert por clave.
        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []

        for result in results:
            clave = make_idempotency_key(result)
            fila = {**result, "idempotency_key": clave}

            for attempt in range(1, attempts + 1):
                if idempotent:
                    upsert_sink[clave] = fila
                    operation = "UPSERT"
                else:
                    append_sink.append(fila)
                    operation = "POST"

                audit.append(
                    {
                        "idempotency_key": clave,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return make_idempotency_key, simulate_sink_retries


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    > **Resuelto.** `make_idempotency_key` arma `merchant_id|window_start`, que es
    > la identidad lógica del resultado. `simulate_sink_retries` contrasta los dos
    > contratos de escritura: en modo append cada intento agrega una fila, y en modo
    > idempotente los N intentos dejan una sola entidad porque el diccionario se
    > indexa por esa clave. La auditoría se conserva completa en ambos casos.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    > **Estado: las 13 pruebas pasan.** Se ejecutan con `uv run pytest`; la salida
    > completa quedó registrada en `evidence/resultados.txt`.

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(summarize_payments):
    import json as _json
    from pathlib import Path as _Path

    _eventos = [
        _json.loads(_linea)
        for _linea in _Path("data/payments.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if _linea.strip()
    ]

    # Configuración por defecto: ventana de 60 s y 120 s de lateness.
    # Sin guion bajo: en Marimo los nombres con `_` son locales a la celda.
    totales, auditoria = summarize_payments(_eventos)

    _aceptados = [fila for fila in auditoria if fila["accepted"]]
    _descartados = [fila for fila in auditoria if not fila["accepted"]]

    resumen_ejecucion = {
        "eventos_entrantes": len(_eventos),
        "eventos_aceptados": len(_aceptados),
        "eventos_descartados": len(_descartados),
        "totales_producidos": len(totales),
        "monto_confirmado": sum(fila["total"] for fila in totales),
        "descartes_por_razon": {
            razon: sum(1 for fila in _descartados if fila["reason"] == razon)
            for razon in sorted({fila["reason"] for fila in _descartados})
        },
        "revisiones": sum(1 for fila in auditoria if fila["revision"]),
    }

    return auditoria, totales, resumen_ejecucion


@app.cell
def _(mo, resumen_ejecucion):
    mo.md(
        f"""
        ## Evidencia de ejecución · configuración por defecto

        | Magnitud | Valor |
        |---|---:|
        | Eventos entrantes | {resumen_ejecucion['eventos_entrantes']} |
        | Eventos aceptados | {resumen_ejecucion['eventos_aceptados']} |
        | Eventos descartados | {resumen_ejecucion['eventos_descartados']} |
        | Totales producidos | {resumen_ejecucion['totales_producidos']} |
        | Monto confirmado | Gs. {resumen_ejecucion['monto_confirmado']:,} |
        | Revisiones (late aceptado) | {resumen_ejecucion['revisiones']} |

        Descartes por razón: `{resumen_ejecucion['descartes_por_razon']}`
        """
    )
    return


@app.cell
def _(mo, auditoria):
    mo.ui.table(auditoria, label="Auditoría por evento")
    return


@app.cell
def _(mo, totales):
    mo.ui.table(totales, label="Totales por comercio y ventana")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
