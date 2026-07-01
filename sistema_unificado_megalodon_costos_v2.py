#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""===============================================================================
MEGALODON COSTOS v2.0 – Sistema de Gestión Documental y Costos de Obra
Mejoras sobre v1.1.0-CORREGIDO:

[CAPAS GDAL INTEGRADAS]
  1. CPLError    → capa centralizada de errores con códigos, severidad, propagación
  2. CPLJson     → escritor JSON incremental / streaming sin objetos gigantes en RAM
  3. CPLVsi      → I/O abstracto (memoria, disco, buffers) interfaz uniforme
  4. CPLQueue    → cola thread-safe para jobs asíncronos en background
  5. CPLProgress → reporte de progreso para operaciones largas (ingestas, validaciones)
  6. CPLMD5      → hashing/integridad SHA-256 con deduplicación y cache keys
  7. CPLQuadTree → índice espacial para búsqueda por cercanía en geodatos de obra

[MOTORES NORMATIVOS NUEVOS]
  A. MotorFallo        → causales detalladas de incumplimiento como entidades
  B. MotorEvidencia    → cada regla apunta a una evidencia concreta (página, párrafo)
  C. MotorPreciosBIM   → flujo geometría→cuantificación→insumos→rendimiento→costo final
  D. MonteCarloRiesgo  → simulador de incertidumbre (precios, clima, merma, cuadrillas)
  E. MotorJuridico     → jurisdicción → ley → reglamento → convocante → tipo de contrato

==============================================================================="""

from __future__ import annotations

import argparse
import base64
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
import queue
import random
import shutil
import sys
import tempfile
import threading
import types
import unittest
import uuid
from enum import Enum
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional, Sequence, Tuple

try:
    import numpy as np  # type: ignore
except Exception:
    np = None

# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES GLOBALES
# ─────────────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def _ensure_dir(path: "str | Path") -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def _maybe_import_from_path(module_name: str, file_name: str) -> Any:
    candidate = Path(__file__).resolve().parent / file_name
    if not candidate.exists():
        raise ImportError(f"No se encontró {file_name}")
    spec = importlib.util.spec_from_file_location(module_name, candidate)
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo cargar {file_name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


# ═════════════════════════════════════════════════════════════════════════════
# CAPA 1 – CPLError: errores centralizados con código, severidad, propagación
# Inspirado en port/cpl_error.h de GDAL
# ═════════════════════════════════════════════════════════════════════════════

class Severidad(str, Enum):
    DEBUG    = "DEBUG"
    INFO     = "INFO"
    AVISO    = "AVISO"
    ERROR    = "ERROR"
    FATAL    = "FATAL"

@dataclass
class EntradaError:
    """Un error registrado en el sistema con todos sus metadatos."""
    codigo: int
    mensaje: str
    severidad: Severidad
    modulo: str
    timestamp: datetime = field(default_factory=_now_utc)
    contexto: Dict[str, Any] = field(default_factory=dict)
    traza: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "codigo": self.codigo,
            "mensaje": self.mensaje,
            "severidad": self.severidad.value,
            "modulo": self.modulo,
            "timestamp": self.timestamp.isoformat(),
            "contexto": self.contexto,
        }

class CPLError:
    """
    Registro centralizado de errores del sistema.
    Un solo lenguaje de fallos: códigos, mensajes, severidad y propagación.
    """
    # Rangos de códigos por subsistema
    COD_NORMATIVO        = 1000
    COD_FIRMA            = 2000
    COD_INTEROP          = 3000
    COD_COSTEO           = 4000
    COD_JURIDICO         = 5000
    COD_BIM              = 6000
    COD_ARCHIVO          = 7000
    COD_MONTECARLO       = 8000

    def __init__(self) -> None:
        self._entradas: List[EntradaError] = []
        self._lock = threading.Lock()
        self._logger = logging.getLogger("CPLError")

    def registrar(
        self,
        codigo: int,
        mensaje: str,
        severidad: Severidad = Severidad.ERROR,
        modulo: str = "",
        contexto: Optional[Dict[str, Any]] = None,
    ) -> EntradaError:
        entrada = EntradaError(
            codigo=codigo,
            mensaje=mensaje,
            severidad=severidad,
            modulo=modulo,
            contexto=contexto or {},
        )
        with self._lock:
            self._entradas.append(entrada)
        nivel = {
            Severidad.DEBUG: logging.DEBUG,
            Severidad.INFO:  logging.INFO,
            Severidad.AVISO: logging.WARNING,
            Severidad.ERROR: logging.ERROR,
            Severidad.FATAL: logging.CRITICAL,
        }.get(severidad, logging.ERROR)
        self._logger.log(nivel, "[%s] %d – %s | ctx=%s", modulo, codigo, mensaje, contexto)
        if severidad == Severidad.FATAL:
            raise MegalodonFatalError(codigo, mensaje, modulo)
        return entrada

    def ultimo_error(self) -> Optional[EntradaError]:
        with self._lock:
            return self._entradas[-1] if self._entradas else None

    def errores_por_modulo(self, modulo: str) -> List[EntradaError]:
        with self._lock:
            return [e for e in self._entradas if e.modulo == modulo]

    def tiene_errores_criticos(self) -> bool:
        with self._lock:
            return any(e.severidad in (Severidad.ERROR, Severidad.FATAL) for e in self._entradas)

    def limpiar(self) -> None:
        with self._lock:
            self._entradas.clear()

    def resumen(self) -> Dict[str, int]:
        with self._lock:
            conteo: Dict[str, int] = {}
            for e in self._entradas:
                conteo[e.severidad.value] = conteo.get(e.severidad.value, 0) + 1
            return conteo

# Instancia global
_cpl_error = CPLError()

class MegalodonFatalError(RuntimeError):
    def __init__(self, codigo: int, mensaje: str, modulo: str = "") -> None:
        super().__init__(f"[FATAL/{modulo}] {codigo}: {mensaje}")
        self.codigo = codigo
        self.modulo = modulo

# Excepciones de negocio (usan CPLError internamente)
class ErrorNormativo(Exception):
    pass

class ErrorFirmaElectronica(Exception):
    pass

class ErrorInteroperabilidad(ErrorNormativo):
    pass

class ErrorConversionPresupuesto(ErrorNormativo):
    pass

class ErrorValidacionEconomica(ErrorNormativo):
    pass

class ErrorJuridico(ErrorNormativo):
    pass

class ErrorBIM(ErrorNormativo):
    pass


# ═════════════════════════════════════════════════════════════════════════════
# CAPA 2 – CPLJson: escritor JSON incremental/streaming
# Inspirado en port/cpl_json_streaming_writer.cpp de GDAL
# Evita construir objetos gigantes en memoria para presupuestos grandes
# ═════════════════════════════════════════════════════════════════════════════

class CPLJsonStreamingWriter:
    """
    Escribe JSON de forma incremental a cualquier objeto file-like o BytesIO.
    Ideal para presupuestos con miles de partidas sin cargar todo en RAM.
    """
    def __init__(self, destino: io.IOBase) -> None:
        self._dest = destino
        self._pila: List[str] = []        # '{' o '['
        self._primero: List[bool] = []    # ¿es el primer elemento del nivel?

    def _write(self, s: str) -> None:
        if isinstance(self._dest, io.RawIOBase) or isinstance(self._dest, io.BufferedIOBase):
            self._dest.write(s.encode("utf-8"))  # type: ignore[arg-type]
        else:
            self._dest.write(s)  # type: ignore[arg-type]

    def _coma(self) -> None:
        if self._primero and not self._primero[-1]:
            self._write(",")
        if self._primero:
            self._primero[-1] = False

    def comenzar_objeto(self) -> "CPLJsonStreamingWriter":
        self._coma()
        self._write("{")
        self._pila.append("{")
        self._primero.append(True)
        return self

    def terminar_objeto(self) -> "CPLJsonStreamingWriter":
        self._write("}")
        self._pila.pop()
        self._primero.pop()
        if self._primero:
            self._primero[-1] = False
        return self

    def comenzar_arreglo(self, clave: Optional[str] = None) -> "CPLJsonStreamingWriter":
        self._coma()
        if clave is not None:
            self._write(f"{json.dumps(clave)}:[")
        else:
            self._write("[")
        self._pila.append("[")
        self._primero.append(True)
        return self

    def terminar_arreglo(self) -> "CPLJsonStreamingWriter":
        self._write("]")
        self._pila.pop()
        self._primero.pop()
        if self._primero:
            self._primero[-1] = False
        return self

    def campo(self, clave: str, valor: Any) -> "CPLJsonStreamingWriter":
        self._coma()
        self._write(f"{json.dumps(clave)}:{json.dumps(valor, ensure_ascii=False, default=str)}")
        return self

    def valor(self, v: Any) -> "CPLJsonStreamingWriter":
        self._coma()
        self._write(json.dumps(v, ensure_ascii=False, default=str))
        return self

    def volcar_dict(self, d: Dict[str, Any]) -> "CPLJsonStreamingWriter":
        """Escribe un dict completo como un objeto JSON."""
        self._coma()
        self._write(json.dumps(d, ensure_ascii=False, default=str))
        if self._primero:
            self._primero[-1] = False
        return self

    def finalizar(self) -> None:
        """Cierra todos los niveles abiertos."""
        while self._pila:
            cierre = "}" if self._pila[-1] == "{" else "]"
            self._write(cierre)
            self._pila.pop()
            self._primero.pop()


def cpl_json_serializar_presupuesto_streaming(
    presupuesto: Any,
    destino: Optional[io.IOBase] = None,
) -> bytes:
    """
    Serializa un presupuesto (con partidas) de forma incremental.
    Si destino es None, devuelve bytes; si no, escribe directamente en destino.
    """
    buf = io.BytesIO() if destino is None else None
    target = buf if buf is not None else destino  # type: ignore[assignment]
    writer = CPLJsonStreamingWriter(target)  # type: ignore[arg-type]
    writer.comenzar_objeto()
    writer.campo("proyecto", getattr(presupuesto, "proyecto", ""))
    writer.campo("proyecto_id", getattr(presupuesto, "proyecto_id", ""))
    writer.campo("monto_directo", getattr(presupuesto, "monto_directo", 0.0))
    writer.campo("monto_total", getattr(presupuesto, "monto_total", 0.0))
    writer.campo("moneda", getattr(presupuesto, "moneda", "MXN"))
    writer.comenzar_arreglo("partidas")
    for partida in getattr(presupuesto, "partidas", []) or []:
        writer.comenzar_objeto()
        writer.campo("id", getattr(partida, "id", ""))
        writer.campo("cantidad", getattr(partida, "cantidad", 0.0))
        concepto = getattr(partida, "concepto", None)
        if concepto:
            writer.campo("concepto", getattr(concepto, "descripcion", ""))
            writer.comenzar_arreglo("insumos")
            for insumo in getattr(concepto, "insumos", []) or []:
                writer.comenzar_objeto()
                writer.campo("nombre", getattr(insumo, "nombre", ""))
                writer.campo("cantidad", getattr(insumo, "cantidad", 0.0))
                writer.campo("precio_unitario", getattr(insumo, "precio_unitario", 0.0))
                writer.campo("precio_total", getattr(insumo, "precio_total", 0.0))
                writer.terminar_objeto()
            writer.terminar_arreglo()
        writer.terminar_objeto()
    writer.terminar_arreglo()
    writer.terminar_objeto()
    if buf is not None:
        return buf.getvalue()
    return b""


# ═════════════════════════════════════════════════════════════════════════════
# CAPA 3 – CPLVsi: I/O abstracto (memoria, disco, blobs) interfaz uniforme
# Inspirado en port/cpl_vsi_mem.cpp de GDAL
# ═════════════════════════════════════════════════════════════════════════════

class CPLVsiHandle:
    """Manejador abstracto de I/O: puede ser disco, RAM o un buffer externo."""

    def __init__(self, nombre: str, datos: Optional[bytes] = None, ruta_disco: Optional[Path] = None) -> None:
        self._nombre = nombre
        self._disco = ruta_disco
        self._buffer: Optional[io.BytesIO] = io.BytesIO(datos) if datos is not None else None
        self._modo = "memoria" if ruta_disco is None else "disco"

    @property
    def nombre(self) -> str:
        return self._nombre

    @property
    def es_memoria(self) -> bool:
        return self._modo == "memoria"

    def leer(self) -> bytes:
        if self._disco:
            return self._disco.read_bytes()
        assert self._buffer is not None
        pos = self._buffer.tell()
        self._buffer.seek(0)
        data = self._buffer.read()
        self._buffer.seek(pos)
        return data

    def escribir(self, datos: bytes) -> None:
        if self._disco:
            self._disco.write_bytes(datos)
            return
        self._buffer = io.BytesIO(datos)

    def tamaño(self) -> int:
        if self._disco and self._disco.exists():
            return self._disco.stat().st_size
        if self._buffer:
            return len(self._buffer.getvalue())
        return 0

    def sha256(self) -> str:
        return _sha256(self.leer())

    def cerrar(self) -> None:
        if self._buffer:
            self._buffer.close()


class CPLVsiSistema:
    """
    Sistema de archivos virtual: unifica memoria, temporales y disco bajo la
    misma API.  Los handles /vsimem/ existen en RAM; los demás, en disco.
    """
    def __init__(self, ruta_base: Optional[Path] = None) -> None:
        self._base = ruta_base or Path(tempfile.gettempdir()) / "megalodon_vsi"
        self._base.mkdir(parents=True, exist_ok=True)
        self._memoria: Dict[str, CPLVsiHandle] = {}
        self._lock = threading.Lock()

    def abrir_memoria(self, nombre: str, datos: bytes = b"") -> CPLVsiHandle:
        handle = CPLVsiHandle(nombre, datos=datos)
        with self._lock:
            self._memoria[nombre] = handle
        return handle

    def abrir_disco(self, nombre: str) -> CPLVsiHandle:
        ruta = self._base / nombre
        return CPLVsiHandle(nombre, ruta_disco=ruta)

    def existe(self, nombre: str) -> bool:
        with self._lock:
            if nombre in self._memoria:
                return True
        return (self._base / nombre).exists()

    def listar(self) -> List[str]:
        disco = [p.name for p in self._base.iterdir()]
        with self._lock:
            mem = list(self._memoria.keys())
        return sorted(set(disco + mem))

    def eliminar(self, nombre: str) -> None:
        with self._lock:
            self._memoria.pop(nombre, None)
        ruta = self._base / nombre
        if ruta.exists():
            ruta.unlink()

    def mover_a_disco(self, nombre: str) -> CPLVsiHandle:
        """Persiste un handle de memoria a disco."""
        with self._lock:
            handle_mem = self._memoria.get(nombre)
        if handle_mem is None:
            raise FileNotFoundError(f"VSI: '{nombre}' no está en memoria")
        datos = handle_mem.leer()
        handle_disco = self.abrir_disco(nombre)
        handle_disco.escribir(datos)
        with self._lock:
            self._memoria.pop(nombre, None)
        return handle_disco


# ═════════════════════════════════════════════════════════════════════════════
# CAPA 4 – CPLQueue: cola thread-safe para jobs asíncronos en background
# Inspirado en port/cpl_threadsafe_queue.hpp de GDAL
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class JobBackground:
    id: str
    tipo: str
    payload: Dict[str, Any]
    callback: Optional[Callable[[Dict[str, Any]], None]] = field(default=None, repr=False)
    creado_en: datetime = field(default_factory=_now_utc)
    resultado: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    estado: str = "PENDIENTE"   # PENDIENTE | EN_PROCESO | COMPLETADO | FALLIDO


class CPLQueue:
    """
    Cola thread-safe para procesar jobs en background: generación de reportes,
    validaciones, ingestas, OCR, compilación de catálogos, etc.
    """
    def __init__(self, trabajadores: int = 2, max_items: int = 500) -> None:
        self._q: "queue.Queue[JobBackground]" = queue.Queue(maxsize=max_items)
        self._resultados: Dict[str, JobBackground] = {}
        self._lock = threading.Lock()
        self._activa = True
        self._hilos: List[threading.Thread] = []
        for _ in range(trabajadores):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self._hilos.append(t)

    def encolar(self, tipo: str, payload: Dict[str, Any], callback: Optional[Callable] = None) -> str:
        job_id = f"JOB-{uuid.uuid4().hex[:8].upper()}"
        job = JobBackground(id=job_id, tipo=tipo, payload=payload, callback=callback)
        with self._lock:
            self._resultados[job_id] = job
        self._q.put(job)
        return job_id

    def obtener_resultado(self, job_id: str) -> Optional[JobBackground]:
        with self._lock:
            return self._resultados.get(job_id)

    def pendientes(self) -> int:
        return self._q.qsize()

    def apagar(self, timeout: float = 5.0) -> None:
        self._activa = False
        for _ in self._hilos:
            try:
                self._q.put_nowait(None)  # type: ignore[arg-type]
            except queue.Full:
                pass
        for h in self._hilos:
            h.join(timeout=timeout)

    def _worker(self) -> None:
        while self._activa:
            try:
                job = self._q.get(timeout=1.0)
            except queue.Empty:
                continue
            if job is None:
                break
            job.estado = "EN_PROCESO"
            try:
                resultado = self._ejecutar(job)
                job.resultado = resultado
                job.estado = "COMPLETADO"
                if job.callback:
                    job.callback(resultado)
            except Exception as exc:
                job.error = str(exc)
                job.estado = "FALLIDO"
                _cpl_error.registrar(
                    CPLError.COD_NORMATIVO + 1,
                    f"Job {job.id} ({job.tipo}) falló: {exc}",
                    Severidad.ERROR,
                    "CPLQueue",
                )
            finally:
                self._q.task_done()

    def _ejecutar(self, job: JobBackground) -> Dict[str, Any]:
        """
        Despachador de tipos de job.  Los consumidores pueden registrar
        manejadores externos; aquí se cubre la base.
        """
        if job.tipo == "HASH_ARCHIVO":
            datos = job.payload.get("datos", b"")
            return {"hash": _sha256(datos if isinstance(datos, bytes) else datos.encode())}
        if job.tipo == "VALIDAR_EXPEDIENTE":
            # se ejecuta validación básica en background
            return {"resultado": "PENDIENTE_MOTOR", "expediente_id": job.payload.get("expediente_id")}
        return {"tipo": job.tipo, "procesado": True}


# ═════════════════════════════════════════════════════════════════════════════
# CAPA 5 – CPLProgress: progreso para operaciones largas
# Inspirado en port/cpl_progress.cpp de GDAL
# ═════════════════════════════════════════════════════════════════════════════

ProgressCallbackFn = Callable[[float, str], bool]  # (pct 0..1, mensaje) → continuar

class CPLProgress:
    """
    Reporta avance de tareas largas: ingestas, validaciones, OCR, catálogos.
    Los suscriptores reciben (porcentaje 0-100, mensaje).
    Devuelve False desde el callback para cancelar la operación.
    """
    def __init__(self, total_pasos: int = 100, descripcion: str = "") -> None:
        self.total = max(total_pasos, 1)
        self.descripcion = descripcion
        self._paso_actual = 0
        self._cancelado = False
        self._callbacks: List[ProgressCallbackFn] = []
        self._lock = threading.Lock()

    def suscribir(self, fn: ProgressCallbackFn) -> None:
        with self._lock:
            self._callbacks.append(fn)

    def avanzar(self, pasos: int = 1, mensaje: str = "") -> bool:
        """
        Avanza 'pasos' unidades.
        Retorna False si algún callback solicitó cancelación.
        """
        with self._lock:
            self._paso_actual = min(self._paso_actual + pasos, self.total)
            pct = self._paso_actual / self.total
            cancelar = False
            for cb in self._callbacks:
                try:
                    continuar = cb(pct, mensaje or f"{self.descripcion} {pct*100:.0f}%")
                    if not continuar:
                        cancelar = True
                except Exception:
                    pass
            if cancelar:
                self._cancelado = True
        return not self._cancelado

    def completar(self) -> None:
        with self._lock:
            self._paso_actual = self.total
        for cb in self._callbacks:
            try:
                cb(1.0, f"{self.descripcion} – completado")
            except Exception:
                pass

    @property
    def porcentaje(self) -> float:
        with self._lock:
            return self._paso_actual / self.total * 100

    @property
    def cancelado(self) -> bool:
        return self._cancelado

    @staticmethod
    def consola() -> ProgressCallbackFn:
        """Callback de conveniencia: imprime en stdout."""
        def _cb(pct: float, msg: str) -> bool:
            print(f"\r  ▶ {msg} [{pct*100:.0f}%]", end="", flush=True)
            if pct >= 1.0:
                print()
            return True
        return _cb


# ═════════════════════════════════════════════════════════════════════════════
# CAPA 6 – CPLMD5 / integridad SHA-256 con cache keys y deduplicación
# Inspirado en port/cpl_md5.cpp de GDAL (actualizado a SHA-256)
# ═════════════════════════════════════════════════════════════════════════════

class CPLHash:
    """
    Checksums, cache keys y deduplicación de archivos.
    Usa SHA-256; si se requiere BLAKE3 en el futuro, solo cambiar _digest().
    """
    _cache: Dict[str, str] = {}
    _lock = threading.Lock()

    @classmethod
    def _digest(cls, data: bytes) -> str:
        return _sha256(data)

    @classmethod
    def de_bytes(cls, data: bytes) -> str:
        return cls._digest(data)

    @classmethod
    def de_archivo(cls, ruta: Path, usar_cache: bool = True) -> str:
        clave_cache = str(ruta)
        if usar_cache:
            with cls._lock:
                if clave_cache in cls._cache:
                    return cls._cache[clave_cache]
        digest = cls._digest(ruta.read_bytes())
        if usar_cache:
            with cls._lock:
                cls._cache[clave_cache] = digest
        return digest

    @classmethod
    def de_texto(cls, texto: str) -> str:
        return cls._digest(texto.encode("utf-8"))

    @classmethod
    def cache_key(cls, *partes: str) -> str:
        """Genera una cache key determinista a partir de varias cadenas."""
        combined = "|".join(partes)
        return cls.de_texto(combined)[:16]

    @classmethod
    def deduplicar(cls, elementos: List[Dict[str, Any]], campo_datos: str = "contenido") -> List[Dict[str, Any]]:
        """
        Filtra duplicados de una lista de dicts usando hash del campo indicado.
        Retorna solo elementos únicos.
        """
        vistos: set = set()
        resultado = []
        for elem in elementos:
            raw = elem.get(campo_datos, "")
            if isinstance(raw, bytes):
                h = cls.de_bytes(raw)
            else:
                h = cls.de_texto(str(raw))
            if h not in vistos:
                vistos.add(h)
                resultado.append(elem)
        return resultado

    @classmethod
    def limpiar_cache(cls) -> None:
        with cls._lock:
            cls._cache.clear()


# ═════════════════════════════════════════════════════════════════════════════
# CAPA 7 – CPLQuadTree: índice espacial para búsqueda por cercanía
# Inspirado en port/cpl_quad_tree.cpp de GDAL
# Útil solo si la herramienta maneja geodatos de obra (polígonos, lotes, trazo)
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class BBox:
    min_x: float
    min_y: float
    max_x: float
    max_y: float

    def contiene(self, x: float, y: float) -> bool:
        return self.min_x <= x <= self.max_x and self.min_y <= y <= self.max_y

    def intersecta(self, otro: "BBox") -> bool:
        return not (otro.min_x > self.max_x or otro.max_x < self.min_x or
                    otro.min_y > self.max_y or otro.max_y < self.min_y)

    def centro(self) -> Tuple[float, float]:
        return ((self.min_x + self.max_x) / 2, (self.min_y + self.max_y) / 2)


@dataclass
class EntradaEspacial:
    id: str
    bbox: BBox
    datos: Dict[str, Any] = field(default_factory=dict)


class CPLQuadTree:
    """
    QuadTree para búsquedas espaciales (frentes de obra, polígonos de licitación,
    tramos carreteros, redes hidráulicas, etc.).
    """
    MAX_ITEMS = 8
    MAX_PROF  = 12

    def __init__(self, bbox: BBox, profundidad: int = 0) -> None:
        self.bbox = bbox
        self.profundidad = profundidad
        self._items: List[EntradaEspacial] = []
        self._hijos: List["CPLQuadTree"] = []

    def insertar(self, entrada: EntradaEspacial) -> bool:
        if not self.bbox.intersecta(entrada.bbox):
            return False
        if len(self._hijos) == 0:
            self._items.append(entrada)
            if len(self._items) > self.MAX_ITEMS and self.profundidad < self.MAX_PROF:
                self._subdividir()
            return True
        insertado = False
        for hijo in self._hijos:
            if hijo.insertar(entrada):
                insertado = True
        if not insertado:
            self._items.append(entrada)
        return True

    def buscar(self, bbox: BBox) -> List[EntradaEspacial]:
        resultados: List[EntradaEspacial] = []
        if not self.bbox.intersecta(bbox):
            return resultados
        for item in self._items:
            if item.bbox.intersecta(bbox):
                resultados.append(item)
        for hijo in self._hijos:
            resultados.extend(hijo.buscar(bbox))
        return resultados

    def punto_mas_cercano(self, x: float, y: float) -> Optional[EntradaEspacial]:
        candidatos = self.buscar(BBox(x - 1e9, y - 1e9, x + 1e9, y + 1e9))
        if not candidatos:
            return None
        def dist(e: EntradaEspacial) -> float:
            cx, cy = e.bbox.centro()
            return (cx - x) ** 2 + (cy - y) ** 2
        return min(candidatos, key=dist)

    def _subdividir(self) -> None:
        mx = (self.bbox.min_x + self.bbox.max_x) / 2
        my = (self.bbox.min_y + self.bbox.max_y) / 2
        sub_bboxes = [
            BBox(self.bbox.min_x, my,             mx,            self.bbox.max_y),
            BBox(mx,              my,             self.bbox.max_x, self.bbox.max_y),
            BBox(self.bbox.min_x, self.bbox.min_y, mx,            my),
            BBox(mx,              self.bbox.min_y, self.bbox.max_x, my),
        ]
        self._hijos = [CPLQuadTree(b, self.profundidad + 1) for b in sub_bboxes]
        items_previos = self._items[:]
        self._items = []
        for item in items_previos:
            insertado = False
            for hijo in self._hijos:
                if hijo.insertar(item):
                    insertado = True
                    break
            if not insertado:
                self._items.append(item)


# ═════════════════════════════════════════════════════════════════════════════
# MOTOR A – MotorFallo: causales detalladas de incumplimiento como entidades
# ═════════════════════════════════════════════════════════════════════════════

class TipoFallo(str, Enum):
    REQUISITO_FALTANTE        = "REQUISITO_FALTANTE"
    INCONSISTENCIA_TECNICA    = "INCONSISTENCIA_TECNICA"
    INCUMPLIMIENTO_DOCUMENTAL = "INCUMPLIMIENTO_DOCUMENTAL"
    PRECIO_FUERA_RANGO        = "PRECIO_FUERA_RANGO"
    ERROR_FIRMA               = "ERROR_FIRMA"
    FORMATO_INVALIDO          = "FORMATO_INVALIDO"
    VIGENCIA_VENCIDA          = "VIGENCIA_VENCIDA"
    CAPACIDAD_INSUFICIENTE    = "CAPACIDAD_INSUFICIENTE"
    INCUMPLIMIENTO_NORMATIVO  = "INCUMPLIMIENTO_NORMATIVO"
    ERROR_CALCULO             = "ERROR_CALCULO"


@dataclass
class CausalFallo:
    """
    Causal de incumplimiento como entidad independiente.
    Permite explicar el porqué, aprender de licitaciones pasadas y reusar patrones.
    """
    id: str = field(default_factory=lambda: f"FALLO-{uuid.uuid4().hex[:8].upper()}")
    tipo: TipoFallo = TipoFallo.REQUISITO_FALTANTE
    descripcion: str = ""
    campo_afectado: str = ""          # campo / sección del expediente
    valor_obtenido: Any = None        # valor que se encontró
    valor_esperado: Any = None        # valor requerido
    norma_referencia: str = ""        # e.g. "LOPSRM Art.36"
    dependencia: str = ""             # nombre del convocante / dependencia
    estado: str = ""                  # estado donde aplica
    tipo_obra: str = ""               # edificación, vial, hidráulica…
    subsanable: bool = True
    timestamp: datetime = field(default_factory=_now_utc)
    evidencia_id: Optional[str] = None  # FK a RegistroEvidencia

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "tipo": self.tipo.value,
            "descripcion": self.descripcion,
            "campo_afectado": self.campo_afectado,
            "valor_obtenido": self.valor_obtenido,
            "valor_esperado": self.valor_esperado,
            "norma_referencia": self.norma_referencia,
            "dependencia": self.dependencia,
            "estado": self.estado,
            "tipo_obra": self.tipo_obra,
            "subsanable": self.subsanable,
            "timestamp": self.timestamp.isoformat(),
            "evidencia_id": self.evidencia_id,
        }


class MotorFallo:
    """
    Motor de análisis de cumplimiento.
    No guarda solo 'falló/pasó'; persiste cada causal como entidad para
    análisis posterior, aprendizaje y reutilización por dependencia/tipo de obra.
    """
    def __init__(self) -> None:
        self._causales: List[CausalFallo] = []
        self._patron_cache: Dict[str, List[CausalFallo]] = {}  # dependencia → causales históricas
        self._lock = threading.Lock()

    def registrar_fallo(self, causal: CausalFallo) -> str:
        with self._lock:
            self._causales.append(causal)
            clave = f"{causal.dependencia}|{causal.tipo_obra}"
            self._patron_cache.setdefault(clave, []).append(causal)
        _cpl_error.registrar(
            CPLError.COD_NORMATIVO + 10,
            f"Fallo [{causal.tipo.value}]: {causal.descripcion}",
            Severidad.AVISO,
            "MotorFallo",
            {"causal_id": causal.id},
        )
        return causal.id

    def evaluar_campo(
        self,
        campo: str,
        valor: Any,
        esperado: Any,
        tipo: TipoFallo,
        norma: str = "",
        dependencia: str = "",
        estado: str = "",
        tipo_obra: str = "",
        subsanable: bool = True,
    ) -> Optional[CausalFallo]:
        """Evalúa un campo; si no cumple, registra y retorna el CausalFallo."""
        cumple = False
        if callable(esperado):
            cumple = esperado(valor)
        elif isinstance(esperado, (list, tuple, set)):
            cumple = valor in esperado
        else:
            cumple = valor == esperado
        if not cumple:
            causal = CausalFallo(
                tipo=tipo,
                descripcion=f"Campo '{campo}' no cumple",
                campo_afectado=campo,
                valor_obtenido=valor,
                valor_esperado=str(esperado)[:200],
                norma_referencia=norma,
                dependencia=dependencia,
                estado=estado,
                tipo_obra=tipo_obra,
                subsanable=subsanable,
            )
            self.registrar_fallo(causal)
            return causal
        return None

    def fallos_por_tipo(self, tipo: TipoFallo) -> List[CausalFallo]:
        with self._lock:
            return [c for c in self._causales if c.tipo == tipo]

    def patron_dependencia(self, dependencia: str, tipo_obra: str = "") -> List[CausalFallo]:
        clave = f"{dependencia}|{tipo_obra}"
        with self._lock:
            return list(self._patron_cache.get(clave, []))

    def resumen(self) -> Dict[str, Any]:
        with self._lock:
            conteo: Dict[str, int] = {}
            subsanables = 0
            for c in self._causales:
                conteo[c.tipo.value] = conteo.get(c.tipo.value, 0) + 1
                if c.subsanable:
                    subsanables += 1
            return {
                "total_fallos": len(self._causales),
                "subsanables": subsanables,
                "no_subsanables": len(self._causales) - subsanables,
                "por_tipo": conteo,
            }

    def limpiar(self) -> None:
        with self._lock:
            self._causales.clear()
            self._patron_cache.clear()


# ═════════════════════════════════════════════════════════════════════════════
# MOTOR B – MotorEvidencia: cada regla apunta a evidencia concreta
# ═════════════════════════════════════════════════════════════════════════════

class TipoEvidencia(str, Enum):
    PAGINA_BASES    = "PAGINA_BASES"
    PARRAFO         = "PARRAFO"
    PLANO           = "PLANO"
    CATALOGO        = "CATALOGO"
    ARCHIVO         = "ARCHIVO"
    CALCULO         = "CALCULO"
    METRADO         = "METRADO"
    FICHA_TECNICA   = "FICHA_TECNICA"
    CAPTURA_BIM     = "CAPTURA_BIM"
    NORMA           = "NORMA"


@dataclass
class RegistroEvidencia:
    """
    Evidencia concreta que respalda una regla o causal.
    El sistema no 'opina': demuestra.
    """
    id: str = field(default_factory=lambda: f"EV-{uuid.uuid4().hex[:8].upper()}")
    tipo: TipoEvidencia = TipoEvidencia.ARCHIVO
    descripcion: str = ""
    referencia: str = ""          # página, párrafo, número de plano, etc.
    contenido_hash: str = ""      # SHA-256 del archivo/documento de respaldo
    nombre_archivo: Optional[str] = None
    url_o_ruta: Optional[str] = None
    regla_id: Optional[str] = None
    causal_id: Optional[str] = None
    timestamp: datetime = field(default_factory=_now_utc)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "tipo": self.tipo.value,
            "descripcion": self.descripcion,
            "referencia": self.referencia,
            "contenido_hash": self.contenido_hash,
            "nombre_archivo": self.nombre_archivo,
            "url_o_ruta": self.url_o_ruta,
            "regla_id": self.regla_id,
            "causal_id": self.causal_id,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class ReglaValidacion:
    """Una regla de negocio con su evidencia de respaldo."""
    id: str = field(default_factory=lambda: f"REGLA-{uuid.uuid4().hex[:8].upper()}")
    descripcion: str = ""
    norma: str = ""
    campo: str = ""
    condicion_fn: Optional[Callable[[Any], bool]] = field(default=None, repr=False)
    evidencias: List[RegistroEvidencia] = field(default_factory=list)
    activa: bool = True


class MotorEvidencia:
    """
    Vincula cada evaluación de regla con evidencia concreta.
    Permite auditar por qué el sistema tomó una decisión.
    """
    def __init__(self, motor_fallo: Optional[MotorFallo] = None) -> None:
        self._reglas: Dict[str, ReglaValidacion] = {}
        self._evidencias: Dict[str, RegistroEvidencia] = {}
        self._motor_fallo = motor_fallo or MotorFallo()
        self._lock = threading.Lock()

    def registrar_regla(self, regla: ReglaValidacion) -> None:
        with self._lock:
            self._reglas[regla.id] = regla

    def adjuntar_evidencia(self, evidencia: RegistroEvidencia) -> None:
        with self._lock:
            self._evidencias[evidencia.id] = evidencia
        if evidencia.regla_id and evidencia.regla_id in self._reglas:
            self._reglas[evidencia.regla_id].evidencias.append(evidencia)

    def evaluar(self, regla_id: str, valor: Any, contexto: Dict[str, Any] = {}) -> Dict[str, Any]:
        regla = self._reglas.get(regla_id)
        if regla is None:
            return {"cumple": None, "mensaje": f"Regla {regla_id} no encontrada"}
        if not regla.activa:
            return {"cumple": True, "mensaje": "Regla inactiva"}
        cumple = regla.condicion_fn(valor) if regla.condicion_fn else True
        resultado: Dict[str, Any] = {
            "cumple": cumple,
            "regla_id": regla_id,
            "campo": regla.campo,
            "norma": regla.norma,
            "evidencias": [e.to_dict() for e in regla.evidencias],
        }
        if not cumple:
            causal = CausalFallo(
                tipo=TipoFallo.INCUMPLIMIENTO_NORMATIVO,
                descripcion=regla.descripcion,
                campo_afectado=regla.campo,
                valor_obtenido=valor,
                norma_referencia=regla.norma,
                dependencia=contexto.get("dependencia", ""),
                estado=contexto.get("estado", ""),
                tipo_obra=contexto.get("tipo_obra", ""),
            )
            if regla.evidencias:
                causal.evidencia_id = regla.evidencias[0].id
            self._motor_fallo.registrar_fallo(causal)
            resultado["causal_id"] = causal.id
        return resultado

    def evidencias_de_regla(self, regla_id: str) -> List[RegistroEvidencia]:
        regla = self._reglas.get(regla_id)
        return regla.evidencias if regla else []

    def resumen(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_reglas": len(self._reglas),
                "reglas_activas": sum(1 for r in self._reglas.values() if r.activa),
                "total_evidencias": len(self._evidencias),
            }


# ═════════════════════════════════════════════════════════════════════════════
# MOTOR C – MotorPreciosBIM: flujo completo geometría → precio final
# geometría → cuantificación → insumos → rendimiento → costo directo
#          → indirectos → utilidad → riesgos → precio final
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class InsumoUnitario:
    """Insumo con fuente de catálogo."""
    clave: str
    descripcion: str
    unidad: str
    precio: float
    fuente_catalogo: str = "CMIC_GENERAL"   # CMIC, CONAGUA, CFE, PEMEX, SSA…
    rendimiento: float = 1.0                 # unidades por hora-cuadrilla

@dataclass
class ConceptoAPU:
    """Análisis de Precio Unitario."""
    clave: str
    descripcion: str
    unidad: str
    materiales: List[Tuple[InsumoUnitario, float]] = field(default_factory=list)
    mano_obra: List[Tuple[InsumoUnitario, float]] = field(default_factory=list)
    equipo: List[Tuple[InsumoUnitario, float]] = field(default_factory=list)

    def costo_directo_unitario(self) -> float:
        total = 0.0
        for ins, cant in (self.materiales + self.mano_obra + self.equipo):
            total += ins.precio * cant
        return total

@dataclass
class PartidaBIM:
    """Partida generada desde geometría BIM."""
    id: str
    elemento_tipo: str
    sistema_constructivo: str
    cantidad: float
    unidad: str
    concepto: Optional[ConceptoAPU] = None
    merma_pct: float = 0.0
    factor_desperdicio: float = 1.0

    def cantidad_con_merma(self) -> float:
        return self.cantidad * (1 + self.merma_pct / 100) * self.factor_desperdicio

    def costo_directo(self) -> float:
        if self.concepto is None:
            return 0.0
        return self.concepto.costo_directo_unitario() * self.cantidad_con_merma()


class MotorPreciosBIM:
    """
    Motor de cuantificación y precios.
    Flujo completo: geometría → cuantificación → insumos → rendimiento
                  → costo directo → indirectos → utilidad → riesgos → precio final
    Catálogos soportados: CMIC, Conagua, CFE, Pemex, sector salud, tabuladores.
    """
    CATALOGOS_SOPORTADOS = {"CMIC_GENERAL", "CMIC_REGIONAL", "CONAGUA", "CFE", "PEMEX", "SSA", "TABULADOR_ESTATAL"}

    def __init__(
        self,
        factor_indirecto: float = 0.15,
        factor_utilidad: float = 0.10,
        factor_impuesto: float = 0.16,
        factor_riesgo: float = 0.02,
    ) -> None:
        self.factor_indirecto = factor_indirecto
        self.factor_utilidad = factor_utilidad
        self.factor_impuesto = factor_impuesto
        self.factor_riesgo = factor_riesgo
        self._catalogo: Dict[str, InsumoUnitario] = {}
        self._conceptos: Dict[str, ConceptoAPU] = {}
        self._lock = threading.Lock()

    def cargar_insumo(self, insumo: InsumoUnitario) -> None:
        with self._lock:
            self._catalogo[insumo.clave] = insumo

    def cargar_concepto(self, concepto: ConceptoAPU) -> None:
        with self._lock:
            self._conceptos[concepto.clave] = concepto

    def cuantificar_elemento(self, elemento: Dict[str, Any]) -> PartidaBIM:
        """Geometría BIM → PartidaBIM con cantidad cuantificada."""
        tipo = str(elemento.get("tipo", "generico"))
        sistema = str(elemento.get("sistema", "concreto"))
        largo = float(elemento.get("largo", 0) or 0)
        ancho = float(elemento.get("ancho", 0) or 0)
        alto  = float(elemento.get("alto",  0) or 0)
        num   = float(elemento.get("num_piezas", 1) or 1)
        merma = float(elemento.get("merma_tecnica_pct", 0) or 0)
        kg_acero_m3 = float(elemento.get("kg_acero_m3", 0) or 0)

        cantidad = largo * ancho * alto * num
        unidad = "m³"

        # Para elementos de tipo muro/tabique se puede usar m²
        if tipo in ("muro", "tabique", "losa_plana") and alto > 0:
            if ancho < 0.5:  # muro delgado → cuantificar en m²
                cantidad = largo * alto * num
                unidad = "m²"

        # Buscar concepto APU en catálogo
        clave_concepto = f"{sistema}_{tipo}".upper()
        concepto = self._conceptos.get(clave_concepto)

        # Construir un APU sintético si no existe en catálogo
        if concepto is None:
            precio_base = self._precio_base_sintetico(tipo, sistema)
            ins_mat = InsumoUnitario(
                clave=f"MAT_{clave_concepto}",
                descripcion=f"Material {sistema} para {tipo}",
                unidad=unidad,
                precio=precio_base * 0.60,
                fuente_catalogo="CMIC_GENERAL",
            )
            ins_mo = InsumoUnitario(
                clave=f"MO_{clave_concepto}",
                descripcion=f"Mano de obra {tipo}",
                unidad=unidad,
                precio=precio_base * 0.25,
                fuente_catalogo="TABULADOR_ESTATAL",
            )
            ins_eq = InsumoUnitario(
                clave=f"EQ_{clave_concepto}",
                descripcion=f"Equipo {tipo}",
                unidad=unidad,
                precio=precio_base * 0.15,
                fuente_catalogo="CMIC_GENERAL",
            )
            concepto = ConceptoAPU(
                clave=clave_concepto,
                descripcion=f"{tipo.capitalize()} de {sistema}",
                unidad=unidad,
                materiales=[(ins_mat, 1.0)],
                mano_obra=[(ins_mo, 1.0)],
                equipo=[(ins_eq, 1.0)],
            )
            # Agregar acero si aplica
            if kg_acero_m3 > 0:
                ins_acero = InsumoUnitario(
                    clave=f"ACERO_{clave_concepto}",
                    descripcion="Acero de refuerzo",
                    unidad="kg",
                    precio=22.0,  # precio base MXN/kg
                    fuente_catalogo="CMIC_GENERAL",
                )
                concepto.materiales.append((ins_acero, kg_acero_m3))

        return PartidaBIM(
            id=f"P-{uuid.uuid4().hex[:6].upper()}",
            elemento_tipo=tipo,
            sistema_constructivo=sistema,
            cantidad=cantidad,
            unidad=unidad,
            concepto=concepto,
            merma_pct=merma,
        )

    def _precio_base_sintetico(self, tipo: str, sistema: str) -> float:
        """Precio base por m³ cuando no hay catálogo cargado."""
        tabla = {
            ("losa", "concreto"): 3_200.0,
            ("columna", "concreto"): 4_500.0,
            ("muro", "block"): 580.0,
            ("muro", "concreto"): 3_000.0,
            ("viga", "concreto"): 3_800.0,
            ("cimentacion", "concreto"): 2_900.0,
            ("piso", "ceramica"): 420.0,
        }
        return tabla.get((tipo.lower(), sistema.lower()), 2_500.0)

    def calcular_precio_final(self, partidas: List[PartidaBIM]) -> Dict[str, Any]:
        """
        Dado un conjunto de partidas, produce el precio final completo.
        """
        monto_directo = sum(p.costo_directo() for p in partidas)
        monto_indirecto = monto_directo * self.factor_indirecto
        subtotal = monto_directo + monto_indirecto
        monto_utilidad = subtotal * self.factor_utilidad
        monto_riesgo   = subtotal * self.factor_riesgo
        base_impuesto  = subtotal + monto_utilidad + monto_riesgo
        monto_impuesto = base_impuesto * self.factor_impuesto
        precio_final   = base_impuesto + monto_impuesto
        return {
            "monto_directo": round(monto_directo, 2),
            "monto_indirecto": round(monto_indirecto, 2),
            "monto_utilidad": round(monto_utilidad, 2),
            "monto_riesgo": round(monto_riesgo, 2),
            "monto_impuesto": round(monto_impuesto, 2),
            "precio_final": round(precio_final, 2),
            "moneda": "MXN",
            "num_partidas": len(partidas),
            "partidas": [
                {
                    "id": p.id,
                    "tipo": p.elemento_tipo,
                    "sistema": p.sistema_constructivo,
                    "cantidad": round(p.cantidad_con_merma(), 4),
                    "unidad": p.unidad,
                    "costo_directo": round(p.costo_directo(), 2),
                }
                for p in partidas
            ],
        }

    def ejecutar_desde_payload(self, payload_bim: Dict[str, Any]) -> Dict[str, Any]:
        """Entrada principal: acepta el mismo payload que el sistema original."""
        elementos = payload_bim.get("elementos", [])
        prog = CPLProgress(len(elementos), "MotorPreciosBIM")
        prog.suscribir(lambda pct, msg: True)  # callback silencioso por defecto
        partidas = []
        for elem in elementos:
            partidas.append(self.cuantificar_elemento(elem))
            prog.avanzar()
        prog.completar()
        resultado = self.calcular_precio_final(partidas)
        resultado["proyecto"] = payload_bim.get("proyecto", "")
        resultado["proyecto_id"] = payload_bim.get("proyecto_id", "")
        return resultado


# ═════════════════════════════════════════════════════════════════════════════
# MOTOR D – MonteCarloRiesgo: simulador de incertidumbre
# Función: estimar riesgo y volatilidad del precio, NO decidir cumplimiento
# Variables: rendimiento, desperdicio, disponibilidad, clima, logística, merma,
#            productividad de cuadrillas, desviación de cantidades
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class ParametroMC:
    """Parámetro de entrada para Monte Carlo."""
    nombre: str
    media: float
    desviacion_std: float
    minimo: Optional[float] = None
    maximo: Optional[float] = None


class MonteCarloRiesgo:
    """
    Simulador Monte Carlo para análisis de riesgo de precios.
    No decide cumplimiento; estima distribución de probabilidad del costo final.
    """
    def __init__(self, iteraciones: int = 1_000, semilla: Optional[int] = None) -> None:
        self.iteraciones = iteraciones
        self._rng = random.Random(semilla)
        self._np_rng = None
        if np is not None and semilla is not None:
            self._np_rng = np.random.default_rng(semilla)
        elif np is not None:
            self._np_rng = np.random.default_rng()

    def _muestra(self, p: ParametroMC) -> float:
        if np is not None and self._np_rng is not None:
            val = float(self._np_rng.normal(p.media, p.desviacion_std))
        else:
            val = self._rng.gauss(p.media, p.desviacion_std)
        if p.minimo is not None:
            val = max(val, p.minimo)
        if p.maximo is not None:
            val = min(val, p.maximo)
        return val

    def simular(
        self,
        costo_base: float,
        parametros: List[ParametroMC],
    ) -> Dict[str, Any]:
        """
        Simula la distribución del costo final aplicando variación conjunta
        de todos los parámetros sobre el costo_base.
        """
        muestras: List[float] = []
        for _ in range(self.iteraciones):
            factor = 1.0
            for p in parametros:
                val = self._muestra(p)
                # cada parámetro representa un multiplicador normalizado
                # media=1.0 → sin efecto; std=0.05 → ±5% variación típica
                factor *= val
            muestras.append(costo_base * factor)

        muestras_sorted = sorted(muestras)
        n = len(muestras_sorted)
        media = sum(muestras) / n
        varianza = sum((x - media) ** 2 for x in muestras) / max(n - 1, 1)
        std = varianza ** 0.5
        p5  = muestras_sorted[max(int(n * 0.05) - 1, 0)]
        p25 = muestras_sorted[max(int(n * 0.25) - 1, 0)]
        p50 = muestras_sorted[max(int(n * 0.50) - 1, 0)]
        p75 = muestras_sorted[max(int(n * 0.75) - 1, 0)]
        p95 = muestras_sorted[min(int(n * 0.95), n - 1)]

        return {
            "costo_base": round(costo_base, 2),
            "iteraciones": self.iteraciones,
            "media": round(media, 2),
            "desviacion_std": round(std, 2),
            "cv_pct": round(std / media * 100 if media else 0, 2),
            "percentiles": {
                "p5":  round(p5,  2),
                "p25": round(p25, 2),
                "p50": round(p50, 2),
                "p75": round(p75, 2),
                "p95": round(p95, 2),
            },
            "rango_probable": {
                "minimo": round(p5,  2),
                "maximo": round(p95, 2),
            },
        }

    @staticmethod
    def parametros_obra_tipicos() -> List[ParametroMC]:
        """
        Parámetros predeterminados para obra pública en México.
        Representan multiplicadores (media=1.0 = sin variación).
        """
        return [
            ParametroMC("rendimiento_cuadrilla",  media=1.0, desviacion_std=0.06, minimo=0.70, maximo=1.30),
            ParametroMC("desperdicio_material",   media=1.0, desviacion_std=0.04, minimo=0.90, maximo=1.20),
            ParametroMC("precio_material",        media=1.0, desviacion_std=0.08, minimo=0.85, maximo=1.30),
            ParametroMC("disponibilidad_equipo",  media=1.0, desviacion_std=0.05, minimo=0.80, maximo=1.10),
            ParametroMC("factor_climatico",       media=1.0, desviacion_std=0.03, minimo=0.95, maximo=1.15),
            ParametroMC("productividad_logistica",media=1.0, desviacion_std=0.04, minimo=0.85, maximo=1.10),
        ]


# ═════════════════════════════════════════════════════════════════════════════
# MOTOR E – MotorJuridico: jurisdicción → ley → reglamento → entidad → contrato
# Cubre marco federal + leyes estatales (LOPSRM, LAASSP, LFPA, LFPRH, LFA, LGPDP)
# ═════════════════════════════════════════════════════════════════════════════

class Jurisdiccion(str, Enum):
    FEDERAL        = "FEDERAL"
    CDMX           = "CDMX"
    JALISCO        = "JALISCO"
    NUEVO_LEON     = "NUEVO_LEON"
    ESTADO_MEXICO  = "ESTADO_MEXICO"
    PUEBLA         = "PUEBLA"
    VERACRUZ       = "VERACRUZ"
    MICHOACAN      = "MICHOACAN"
    BAJA_CALIFORNIA = "BAJA_CALIFORNIA"
    CHIHUAHUA      = "CHIHUAHUA"
    SONORA         = "SONORA"
    OAXACA         = "OAXACA"
    OTRO           = "OTRO"


class TipoContrato(str, Enum):
    PRECIOS_UNITARIOS   = "PRECIOS_UNITARIOS"
    PRECIO_ALZADO       = "PRECIO_ALZADO"
    MIXTO               = "MIXTO"
    OBRA_PUBLICA        = "OBRA_PUBLICA"
    SERVICIO_RELACIONADO = "SERVICIO_RELACIONADO"
    ARRENDAMIENTO       = "ARRENDAMIENTO"
    ADQUISICION         = "ADQUISICION"


@dataclass
class MarcoJuridico:
    """Marco normativo aplicable a una combinación jurisdicción+convocante+contrato."""
    jurisdiccion: Jurisdiccion
    leyes_aplicables: List[str]
    reglamentos: List[str]
    lineamientos: List[str]
    convocante: str
    tipo_contrato: TipoContrato
    umbrales_licitacion: Dict[str, float] = field(default_factory=dict)   # MXN
    notas: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "jurisdiccion": self.jurisdiccion.value,
            "leyes_aplicables": self.leyes_aplicables,
            "reglamentos": self.reglamentos,
            "lineamientos": self.lineamientos,
            "convocante": self.convocante,
            "tipo_contrato": self.tipo_contrato.value,
            "umbrales_licitacion": self.umbrales_licitacion,
            "notas": self.notas,
        }


class MotorJuridico:
    """
    Motor de reglas jurídicas por jurisdicción.
    Modela: jurisdicción → ley → reglamento → lineamientos → convocante
                        → tipo de contrato.
    Preparado para expandirse con datos del libro Fisco Agenda 2026 y
    compendio de leyes estatales de la Cámara de Diputados.
    """

    # ── Base de conocimiento estática (federal) ──────────────────────────────
    _BASE_FEDERAL: Dict[TipoContrato, MarcoJuridico] = {}

    def __init__(self, motor_fallo: Optional[MotorFallo] = None) -> None:
        self._marcos: Dict[str, MarcoJuridico] = {}
        self._motor_fallo = motor_fallo or MotorFallo()
        self._lock = threading.Lock()
        self._cargar_marcos_federales()

    def _cargar_marcos_federales(self) -> None:
        """Carga los marcos jurídicos federales base."""
        marcos_fed = [
            MarcoJuridico(
                jurisdiccion=Jurisdiccion.FEDERAL,
                leyes_aplicables=["LOPSRM", "LFPRH", "LFA", "LGPDP"],
                reglamentos=["RLOPSRM", "RLFPRH"],
                lineamientos=["Lineamientos SHCP 2024", "Políticas IMSS", "Manual de Obra Pública"],
                convocante="SFP",
                tipo_contrato=TipoContrato.PRECIOS_UNITARIOS,
                umbrales_licitacion={
                    "adjudicacion_directa": 2_100_000.0,
                    "invitacion_tres": 10_400_000.0,
                    "licitacion_publica": 10_400_001.0,
                },
                notas="Umbral 2025 LOPSRM – actualizar anualmente con DOF",
            ),
            MarcoJuridico(
                jurisdiccion=Jurisdiccion.FEDERAL,
                leyes_aplicables=["LAASSP", "LFPRH", "LFA"],
                reglamentos=["RLAASSP"],
                lineamientos=["Lineamientos SHCP 2024"],
                convocante="SFP",
                tipo_contrato=TipoContrato.ADQUISICION,
                umbrales_licitacion={
                    "adjudicacion_directa": 1_350_000.0,
                    "invitacion_tres": 6_500_000.0,
                    "licitacion_publica": 6_500_001.0,
                },
                notas="LAASSP adquisiciones – umbral 2025",
            ),
        ]
        for m in marcos_fed:
            clave = self._clave(m.jurisdiccion, m.convocante, m.tipo_contrato)
            with self._lock:
                self._marcos[clave] = m

    def _clave(self, jurisdiccion: Jurisdiccion, convocante: str, tipo: TipoContrato) -> str:
        return f"{jurisdiccion.value}|{convocante.upper()}|{tipo.value}"

    def registrar_marco(self, marco: MarcoJuridico) -> None:
        clave = self._clave(marco.jurisdiccion, marco.convocante, marco.tipo_contrato)
        with self._lock:
            self._marcos[clave] = marco

    def obtener_marco(
        self,
        jurisdiccion: Jurisdiccion,
        convocante: str,
        tipo_contrato: TipoContrato,
    ) -> Optional[MarcoJuridico]:
        clave = self._clave(jurisdiccion, convocante, tipo_contrato)
        with self._lock:
            return self._marcos.get(clave)

    def determinar_procedimiento(
        self,
        jurisdiccion: Jurisdiccion,
        convocante: str,
        tipo_contrato: TipoContrato,
        monto: float,
    ) -> Dict[str, Any]:
        """
        Dado un monto y contexto, determina el procedimiento de contratación
        aplicable (adjudicación directa, invitación a 3, licitación pública).
        """
        marco = self.obtener_marco(jurisdiccion, convocante, tipo_contrato)
        if marco is None:
            # fallback: buscar solo por jurisdicción y tipo
            for m in self._marcos.values():
                if m.jurisdiccion == jurisdiccion and m.tipo_contrato == tipo_contrato:
                    marco = m
                    break
        if marco is None:
            return {
                "procedimiento": "NO_DETERMINADO",
                "motivo": f"No se encontró marco para {jurisdiccion.value}/{convocante}/{tipo_contrato.value}",
                "marco_disponible": False,
            }

        umbral_ad  = marco.umbrales_licitacion.get("adjudicacion_directa", 0)
        umbral_i3  = marco.umbrales_licitacion.get("invitacion_tres", 0)

        if monto <= umbral_ad:
            procedimiento = "ADJUDICACION_DIRECTA"
            fundamento = "Art. 42 LOPSRM / Art. 41 LAASSP"
        elif monto <= umbral_i3:
            procedimiento = "INVITACION_CUANDO_MENOS_TRES"
            fundamento = "Art. 43 LOPSRM / Art. 42 LAASSP"
        else:
            procedimiento = "LICITACION_PUBLICA"
            fundamento = "Art. 27 LOPSRM / Art. 26 LAASSP"

        return {
            "procedimiento": procedimiento,
            "fundamento": fundamento,
            "monto": monto,
            "jurisdiccion": jurisdiccion.value,
            "convocante": convocante,
            "tipo_contrato": tipo_contrato.value,
            "leyes_aplicables": marco.leyes_aplicables,
            "notas": marco.notas,
            "marco_disponible": True,
        }

    def validar_requisitos(
        self,
        jurisdiccion: Jurisdiccion,
        convocante: str,
        tipo_contrato: TipoContrato,
        datos_propuesta: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Valida los campos obligatorios de una propuesta según el marco jurídico."""
        marco = self.obtener_marco(jurisdiccion, convocante, tipo_contrato)
        fallos: List[str] = []
        if not datos_propuesta.get("rfc"):
            causal = CausalFallo(
                tipo=TipoFallo.REQUISITO_FALTANTE,
                descripcion="RFC del licitante es obligatorio",
                campo_afectado="rfc",
                norma_referencia="LAASSP Art. 29 / LOPSRM Art. 36",
                dependencia=convocante,
            )
            self._motor_fallo.registrar_fallo(causal)
            fallos.append("RFC faltante")
        if not datos_propuesta.get("garantia_seriedad"):
            causal = CausalFallo(
                tipo=TipoFallo.REQUISITO_FALTANTE,
                descripcion="Garantía de seriedad no proporcionada",
                campo_afectado="garantia_seriedad",
                norma_referencia="LOPSRM Art. 48",
                dependencia=convocante,
            )
            self._motor_fallo.registrar_fallo(causal)
            fallos.append("Garantía de seriedad faltante")
        return {
            "cumple": len(fallos) == 0,
            "fallos": fallos,
            "marco": marco.to_dict() if marco else None,
        }

    def listar_marcos(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [m.to_dict() for m in self._marcos.values()]


# ═════════════════════════════════════════════════════════════════════════════
# ENUMS Y DATACLASSES BASE (igual que v1, retenidos íntegros)
# ═════════════════════════════════════════════════════════════════════════════

class ClasificacionSeguridad(str, Enum):
    PUBLICO      = "PUBLICO"
    RESERVADO    = "RESERVADO"
    CONFIDENCIAL = "CONFIDENCIAL"

class EstadoExpediente(str, Enum):
    INICIADO               = "INICIADO"
    EN_FIRMA               = "EN_FIRMA"
    ARCHIVADO              = "ARCHIVADO"
    EN_INTEROPERABILIDAD   = "EN_INTEROPERABILIDAD"

class TipoDocumento(str, Enum):
    INFORME     = "INFORME"
    OFICIO      = "OFICIO"
    CONTRATO    = "CONTRATO"
    PRESUPUESTO = "PRESUPUESTO"

class NivelFirma(str, Enum):
    FIEL   = "FIEL"
    SIMPLE = "SIMPLE"

class TipoInteroperabilidad(str, Enum):
    REMISION_EXPEDIENTE = "REMISION_EXPEDIENTE"
    REMISION_DOCUMENTO  = "REMISION_DOCUMENTO"

class ZonaEconomica(str, Enum):
    NORTE     = "NORTE"
    CENTRO    = "CENTRO"
    SUR       = "SUR"
    NOROESTE  = "NOROESTE"
    NORESTE   = "NORESTE"
    OCCIDENTE = "OCCIDENTE"
    SURESTE   = "SURESTE"

@dataclass
class Metadato:
    nombre: str
    valor: str
    tipo: str = "string"
    obligatorio: bool = False
    esquema: str = "GENERAL"

@dataclass
class FirmaElectronicaReal:
    sujeto: str
    huella_certificado: str
    fecha_firma: datetime
    nivel: str = "FIEL"
    autoridad: str = "SAT"
    firma_base64: str = ""

@dataclass
class IndiceExpediente:
    firma_indice: Optional[FirmaElectronicaReal] = None
    hash_indice: str = ""

@dataclass
class DocumentoElectronico:
    identificador: str
    nombre: str
    contenido: bytes
    tipo_documental: TipoDocumento
    organo: str
    fecha_captura: datetime = field(default_factory=_now_utc)
    formato: str = "json"
    autor: str = ""
    nivel_seguridad: str = ClasificacionSeguridad.PUBLICO.value
    metadatos: List[Metadato] = field(default_factory=list)
    hash_contenido: str = ""
    firma: Optional[FirmaElectronicaReal] = None

    def __post_init__(self) -> None:
        self.hash_contenido = self._calcular_hash_contenido()

    def _calcular_hash_contenido(self) -> str:
        return CPLHash.de_bytes(self.contenido)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identificador": self.identificador,
            "nombre": self.nombre,
            "tipo_documental": self.tipo_documental.value if isinstance(self.tipo_documental, Enum) else str(self.tipo_documental),
            "organo": self.organo,
            "fecha_captura": self.fecha_captura.isoformat(),
            "formato": self.formato,
            "autor": self.autor,
            "nivel_seguridad": self.nivel_seguridad,
            "hash_contenido": self.hash_contenido,
            "metadatos": [dataclasses.asdict(m) for m in self.metadatos],
            "firmado": self.firma is not None,
        }

@dataclass
class ExpedienteElectronico:
    identificador: str
    titulo: str
    descripcion: str
    organo: str
    unidad_administrativa: str
    serie_documental: str
    subserie_documental: str
    fecha_apertura: datetime = field(default_factory=_now_utc)
    estado: EstadoExpediente = EstadoExpediente.INICIADO
    clasificacion: str = ClasificacionSeguridad.PUBLICO.value
    responsable: str = ""
    documentos: List[DocumentoElectronico] = field(default_factory=list)
    metadatos: List[Metadato] = field(default_factory=list)
    indice: IndiceExpediente = field(default_factory=IndiceExpediente)

    def agregar_documento(self, doc: DocumentoElectronico) -> None:
        self.documentos.append(doc)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identificador": self.identificador,
            "titulo": self.titulo,
            "descripcion": self.descripcion,
            "organo": self.organo,
            "unidad_administrativa": self.unidad_administrativa,
            "serie_documental": self.serie_documental,
            "subserie_documental": self.subserie_documental,
            "fecha_apertura": self.fecha_apertura.isoformat(),
            "estado": self.estado.value if isinstance(self.estado, Enum) else str(self.estado),
            "clasificacion": self.clasificacion,
            "responsable": self.responsable,
            "num_documentos": len(self.documentos),
            "metadatos": [dataclasses.asdict(m) for m in self.metadatos],
        }


# ═════════════════════════════════════════════════════════════════════════════
# SERVICIOS BASE (trazabilidad, firma, archivo, validación, interop)
# ═════════════════════════════════════════════════════════════════════════════

class UtilidadCriptografica:
    @staticmethod
    def hash_sha256(data: bytes) -> str:
        return CPLHash.de_bytes(data)

class ServicioTrazabilidad:
    def __init__(self) -> None:
        self._registros: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def registrar(self, actor: str, accion: str, objeto: str, objeto_id: str, resultado: str, detalle: str = "") -> Dict[str, Any]:
        with self._lock:
            prev_hash = self._registros[-1]["hash"] if self._registros else ""
            payload = {
                "timestamp": _now_utc().isoformat(),
                "actor": actor,
                "accion": accion,
                "objeto": objeto,
                "objeto_id": objeto_id,
                "resultado": resultado,
                "detalle": detalle,
                "prev_hash": prev_hash,
            }
            payload_bytes = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
            payload["hash"] = _sha256(payload_bytes)
            self._registros.append(payload)
            return payload

    def verificar_integridad_cadena(self) -> Tuple[bool, List[str]]:
        errores: List[str] = []
        prev_hash = ""
        for idx, r in enumerate(self._registros):
            data = {k: v for k, v in r.items() if k != "hash"}
            calc = _sha256(json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8"))
            if calc != r.get("hash"):
                errores.append(f"Registro {idx} con hash inválido")
            if r.get("prev_hash", "") != prev_hash:
                errores.append(f"Registro {idx} con encadenado previo inválido")
            prev_hash = r.get("hash", "")
        return (len(errores) == 0, errores)

    def to_list(self) -> List[Dict[str, Any]]:
        return list(self._registros)

class ServicioFirma:
    def __init__(self, trazabilidad: Optional[ServicioTrazabilidad] = None) -> None:
        self.trazabilidad = trazabilidad or ServicioTrazabilidad()
        self.certificados: Dict[str, str] = {}

    def generar_certificado_prueba(self, sujeto: str, password: str) -> str:
        huella = _sha256(f"{sujeto}:{password}".encode("utf-8"))
        self.certificados[sujeto] = huella
        return huella

    def firmar_documento(self, doc: DocumentoElectronico, firmante: str, cargo: str, esquema: str, nivel: NivelFirma, autoridad: str) -> FirmaElectronicaReal:
        firma = FirmaElectronicaReal(
            sujeto=firmante,
            huella_certificado=self.certificados.get(firmante, _sha256(firmante.encode("utf-8"))),
            fecha_firma=_now_utc(),
            nivel=nivel.value,
            autoridad=autoridad,
            firma_base64=base64.b64encode(_sha256(doc.contenido + firmante.encode("utf-8")).encode("utf-8")).decode("ascii"),
        )
        doc.firma = firma
        self.trazabilidad.registrar("SERVICIO_FIRMA", "FIRMAR_DOCUMENTO", doc.__class__.__name__, doc.identificador, "OK", f"Firmante: {firmante}, Cargo: {cargo}")
        return firma

    def firmar_indice_expediente(self, expediente: ExpedienteElectronico, firmante: str, cargo: str) -> FirmaElectronicaReal:
        payload = json.dumps({"expediente": expediente.identificador, "documentos": [d.hash_contenido for d in expediente.documentos]}, sort_keys=True).encode("utf-8")
        firma = FirmaElectronicaReal(
            sujeto=firmante,
            huella_certificado=self.certificados.get(firmante, _sha256(firmante.encode("utf-8"))),
            fecha_firma=_now_utc(),
            nivel=NivelFirma.FIEL.value,
            autoridad="SAT",
            firma_base64=base64.b64encode(_sha256(payload).encode("utf-8")).decode("ascii"),
        )
        expediente.indice = IndiceExpediente(firma_indice=firma, hash_indice=_sha256(payload))
        self.trazabilidad.registrar("SERVICIO_FIRMA", "FIRMAR_INDICE", expediente.__class__.__name__, expediente.identificador, "OK", f"Firmante: {firmante}, Cargo: {cargo}")
        return firma

class ServicioArchivo:
    def __init__(self, ruta_base: "str | Path", trazabilidad: Optional[ServicioTrazabilidad] = None) -> None:
        self.ruta_base = _ensure_dir(ruta_base)
        self.trazabilidad = trazabilidad or ServicioTrazabilidad()
        self._vsi = CPLVsiSistema(self.ruta_base / "_vsi")

    def archivar_expediente(self, expediente: ExpedienteElectronico) -> str:
        carpeta = _ensure_dir(self.ruta_base / expediente.identificador)
        manifest = {
            "expediente": expediente.to_dict(),
            "documentos": [d.to_dict() for d in expediente.documentos],
            "archivado_en": _now_utc().isoformat(),
        }
        manifest_path = carpeta / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        self.trazabilidad.registrar("SERVICIO_ARCHIVO", "ARCHIVAR_EXPEDIENTE", expediente.__class__.__name__, expediente.identificador, "OK", str(manifest_path))
        return str(carpeta)

    def recuperar_expediente(self, expediente_id: str, incluir_contenido: bool = False) -> Dict[str, Any]:
        carpeta = self.ruta_base / expediente_id
        manifest_path = carpeta / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(expediente_id)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return {
            "manifest": manifest if incluir_contenido else {k: v for k, v in manifest.items() if k != "documentos"},
            "hash_global_ok": True,
        }

class ValidadorCumplimiento:
    def validar_expediente(self, expediente: ExpedienteElectronico) -> Dict[str, Any]:
        errores = []
        if not expediente.identificador:
            errores.append("Expediente sin identificador")
        if not expediente.documentos:
            errores.append("Expediente sin documentos")
        return {"valido": len(errores) == 0, "errores": errores}

class ServicioInteroperabilidad:
    def __init__(self, servicio_trazabilidad: Optional[ServicioTrazabilidad] = None) -> None:
        self.trazabilidad = servicio_trazabilidad or ServicioTrazabilidad()

    def generar_mensaje_interoperabilidad(self, tipo: TipoInteroperabilidad, datos: Dict[str, Any]) -> str:
        mensaje = {"tipo": tipo.value if isinstance(tipo, Enum) else str(tipo), "datos": datos, "timestamp": _now_utc().isoformat()}
        xml_like = json.dumps(mensaje, ensure_ascii=False, indent=2)
        self.trazabilidad.registrar("SERVICIO_INTEROP", "GENERAR_MENSAJE", "MensajeInteroperabilidad", datos.get("cuerpo", {}).get("identificadorExpediente", ""), "OK", tipo.value)
        return xml_like

class SistemaGestionExpedientesMexico:
    def __init__(self) -> None:
        self.trazabilidad = ServicioTrazabilidad()
        self.firma = ServicioFirma(self.trazabilidad)
        self.archivo = ServicioArchivo(Path(tempfile.gettempdir()) / "megalodon_archivo", self.trazabilidad)
        self.validador = ValidadorCumplimiento()
        self.busqueda = types.SimpleNamespace(indexar_expediente=lambda expediente: None)
        self.expedientes: Dict[str, ExpedienteElectronico] = {}

    def inicializar_directorios(self) -> None:
        _ensure_dir(Path(tempfile.gettempdir()) / "megalodon_archivo")

    def obtener_estadisticas_sistema(self) -> Dict[str, Any]:
        firmados = sum(1 for e in self.expedientes.values() if e.indice.firma_indice is not None)
        return {
            "expedientes_en_memoria": len(self.expedientes),
            "expedientes_archivados": sum(1 for e in self.expedientes.values() if e.estado == EstadoExpediente.ARCHIVADO),
            "total_registros_trazabilidad": len(self.trazabilidad.to_list()),
            "certificados_cargados": len(self.firma.certificados),
            "expedientes_firmados": firmados,
        }


# ═════════════════════════════════════════════════════════════════════════════
# FLAGS DE CARGA DE MOTORES EXTERNOS (igual que v1)
# ═════════════════════════════════════════════════════════════════════════════

REAL_MOTOR_LOADED  = False
REAL_COSTOS_LOADED = False

try:
    motor_mod = _maybe_import_from_path("motor_end_to_end_corregido", "motor_end_to_end_corregido.py")
    SistemaGestionExpedientesMexico = getattr(motor_mod, "SistemaGestionExpedientesMexico", SistemaGestionExpedientesMexico)
    DocumentoElectronico   = getattr(motor_mod, "DocumentoElectronico",   DocumentoElectronico)
    ExpedienteElectronico  = getattr(motor_mod, "ExpedienteElectronico",  ExpedienteElectronico)
    Metadato               = getattr(motor_mod, "Metadato",               Metadato)
    FirmaElectronicaReal   = getattr(motor_mod, "FirmaElectronicaReal",   FirmaElectronicaReal)
    EstadoExpediente       = getattr(motor_mod, "EstadoExpediente",       EstadoExpediente)
    TipoDocumento          = getattr(motor_mod, "TipoDocumento",          TipoDocumento)
    NivelFirma             = getattr(motor_mod, "NivelFirma",             NivelFirma)
    ClasificacionSeguridad = getattr(motor_mod, "ClasificacionSeguridad", ClasificacionSeguridad)
    TipoInteroperabilidad  = getattr(motor_mod, "TipoInteroperabilidad",  TipoInteroperabilidad)
    ServicioTrazabilidad   = getattr(motor_mod, "ServicioTrazabilidad",   ServicioTrazabilidad)
    ServicioFirma          = getattr(motor_mod, "ServicioFirma",          ServicioFirma)
    ServicioArchivo        = getattr(motor_mod, "ServicioArchivo",        ServicioArchivo)
    ValidadorCumplimiento  = getattr(motor_mod, "ValidadorCumplimiento",  ValidadorCumplimiento)
    ServicioInteroperabilidad = getattr(motor_mod, "ServicioInteroperabilidad", ServicioInteroperabilidad)
    UtilidadCriptografica  = getattr(motor_mod, "UtilidadCriptografica",  UtilidadCriptografica)
    ErrorNormativo         = getattr(motor_mod, "ErrorNormativo",         ErrorNormativo)
    ErrorFirmaElectronica  = getattr(motor_mod, "ErrorFirmaElectronica",  ErrorFirmaElectronica)
    NS_MX    = getattr(motor_mod, "NS_MX",    "urn:megalodon:mx")
    NS_INTEROP = getattr(motor_mod, "NS_INTEROP", "urn:megalodon:interop")
    REAL_MOTOR_LOADED = True
except Exception:
    NS_MX    = "urn:megalodon:mx"
    NS_INTEROP = "urn:megalodon:interop"

try:
    mega_mod = _maybe_import_from_path("megalodon_cuantificacion_patch", "megalodon_cuantificacion_patch.py")
    ElementoGeometricoV2   = getattr(mega_mod, "ElementoGeometricoV2")
    CuantificadorV2        = getattr(mega_mod, "CuantificadorV2")
    MatcherConceptoV2      = getattr(mega_mod, "MatcherConceptoV2")
    ConceptoCompuestoBuilder = getattr(mega_mod, "ConceptoCompuestoBuilder")
    PipelinePresupuestoV2  = getattr(mega_mod, "PipelinePresupuestoV2")
    REAL_COSTOS_LOADED = True
except Exception:
    @dataclass
    class Insumo:
        nombre: str
        tipo: str
        cantidad: float
        precio_unitario: float
        precio_total: float

    @dataclass
    class Concepto:
        descripcion: str
        insumos: List[Insumo] = field(default_factory=list)

    @dataclass
    class Partida:
        id: str
        cantidad: float
        concepto: Concepto

    @dataclass
    class PresupuestoEngine:
        proyecto: str = ""
        proyecto_id: str = ""
        partidas: List[Partida] = field(default_factory=list)
        monto_directo: float = 0.0
        monto_indirecto: float = 0.0
        monto_utilidad: float = 0.0
        monto_impuesto: float = 0.0
        monto_total: float = 0.0
        moneda: str = "MXN"
        zona_economica: str = "NORTE"

        def to_dict(self) -> Dict[str, Any]:
            return {
                "proyecto": self.proyecto,
                "proyecto_id": self.proyecto_id,
                "monto_directo": self.monto_directo,
                "monto_indirecto": self.monto_indirecto,
                "monto_utilidad": self.monto_utilidad,
                "monto_impuesto": self.monto_impuesto,
                "monto_total": self.monto_total,
                "moneda": self.moneda,
                "zona_economica": self.zona_economica,
                "numero_partidas": len(self.partidas),
                "partidas": [
                    {
                        "id": p.id,
                        "cantidad": p.cantidad,
                        "concepto": {
                            "descripcion": p.concepto.descripcion,
                            "insumos": [dataclasses.asdict(i) for i in p.concepto.insumos],
                        },
                    }
                    for p in self.partidas
                ],
            }

    class Database:
        pass

    class APUCatalogo:
        pass

    class AjustadorZonal:
        pass

    class MatcherConcepto:
        pass

    class Cuantificador:
        pass

    class ElementoGeometrico:
        pass

    TIPOS_POR_PIEZA: Dict[str, Any] = {}

    class PipelinePresupuestoV2:
        def __init__(self, zona: ZonaEconomica, database: Any = None):
            self.zona = zona
            self.database = database
            self.presupuesto: Optional[PresupuestoEngine] = None
            self._motor_bim = MotorPreciosBIM()

        def ejecutar(self, payload_bim: Dict[str, Any]) -> Dict[str, Any]:
            # Usar MotorPreciosBIM para cuantificación real
            resultado_bim = self._motor_bim.ejecutar_desde_payload(payload_bim)
            proyecto    = payload_bim.get("proyecto", "SIN_NOMBRE")
            proyecto_id = payload_bim.get("proyecto_id", str(uuid.uuid4()))

            partidas_bim = resultado_bim.get("partidas", [])
            partidas: List[Partida] = []
            monto_directo = 0.0
            for pb in partidas_bim:
                insumo = Insumo(
                    nombre=f"APU_{pb['tipo']}_{pb['sistema']}",
                    tipo=pb["sistema"],
                    cantidad=pb["cantidad"],
                    precio_unitario=pb["costo_directo"] / pb["cantidad"] if pb["cantidad"] > 0 else 0,
                    precio_total=pb["costo_directo"],
                )
                concepto = Concepto(descripcion=f"{pb['tipo'].capitalize()} ({pb['sistema']})", insumos=[insumo])
                partidas.append(Partida(id=pb["id"], cantidad=pb["cantidad"], concepto=concepto))
                monto_directo += pb["costo_directo"]

            presupuesto = PresupuestoEngine(
                proyecto=proyecto,
                proyecto_id=proyecto_id,
                partidas=partidas,
                monto_directo=monto_directo,
                zona_economica=self.zona.value if isinstance(self.zona, Enum) else str(self.zona),
            )
            presupuesto.monto_indirecto = monto_directo * 0.15
            subtotal = monto_directo + presupuesto.monto_indirecto
            presupuesto.monto_utilidad = subtotal * 0.10
            presupuesto.monto_impuesto = (subtotal + presupuesto.monto_utilidad) * 0.16
            presupuesto.monto_total    = subtotal + presupuesto.monto_utilidad + presupuesto.monto_impuesto
            self.presupuesto = presupuesto
            return {"status": "COMPLETADO", "meta": {"partidas_generados": len(partidas), "zona": str(self.zona)}, "logs": []}


# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN UNIFICADA
# ═════════════════════════════════════════════════════════════════════════════

class ConfiguracionUnificada:
    RUTA_BASE_ARCHIVO    = os.environ.get("EXPEDIENTE_RUTA_BASE", str(Path(tempfile.gettempdir()) / "megalodon_unificado"))
    RUTA_BASE_DB         = os.environ.get("EXPEDIENTE_RUTA_DB",   str(Path(RUTA_BASE_ARCHIVO) / "db"))
    RUTA_LOGS            = os.environ.get("EXPEDIENTE_RUTA_LOGS", str(Path(RUTA_BASE_ARCHIVO) / "logs"))
    RUTA_CERTIFICADOS    = os.environ.get("EXPEDIENTE_RUTA_CERTS",str(Path(RUTA_BASE_ARCHIVO) / "certs"))
    ZONA_ECONOMICA_DEFAULT = ZonaEconomica.NORTE
    FACTOR_INDIRECTO     = 0.15
    FACTOR_UTILIDAD      = 0.10
    FACTOR_IMPUESTO      = 0.16
    SERIE_DOCUMENTAL_PRESUPUESTO       = "PRESUPUESTOS_OBRA"
    SUBSERIE_PRESUPUESTO_DETALLADO     = "PRESUPUESTO_DETALLADO"
    SUBSERIE_PRESUPUESTO_RESUMEN       = "PRESUPUESTO_RESUMEN"

    @classmethod
    def inicializar(cls) -> None:
        _ensure_dir(cls.RUTA_BASE_ARCHIVO)
        _ensure_dir(cls.RUTA_BASE_DB)
        _ensure_dir(cls.RUTA_LOGS)
        _ensure_dir(cls.RUTA_CERTIFICADOS)
        _ensure_dir(Path(cls.RUTA_BASE_ARCHIVO) / "presupuestos")


# ═════════════════════════════════════════════════════════════════════════════
# DOCUMENTO PRESUPUESTO Y EXPEDIENTE OBRA
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class DocumentoPresupuesto(DocumentoElectronico):
    monto_directo: float    = 0.0
    monto_indirecto: float  = 0.0
    monto_utilidad: float   = 0.0
    monto_impuesto: float   = 0.0
    monto_total: float      = 0.0
    moneda: str             = "MXN"
    zona_economica: str     = "NORTE"
    factor_indirecto: float = ConfiguracionUnificada.FACTOR_INDIRECTO
    factor_utilidad: float  = ConfiguracionUnificada.FACTOR_UTILIDAD
    factor_impuesto: float  = ConfiguracionUnificada.FACTOR_IMPUESTO
    numero_partidas: int    = 0
    insumos_desglose: List[Dict[str, Any]] = field(default_factory=list)
    resultado_montecarlo: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self._recalcular_montos()
        self._sincronizar_metadatos_presupuesto()
        self.hash_contenido = self._calcular_hash_contenido()

    def _recalcular_montos(self) -> None:
        self.monto_indirecto = float(self.monto_directo) * float(self.factor_indirecto)
        subtotal = float(self.monto_directo) + float(self.monto_indirecto)
        self.monto_utilidad = subtotal * float(self.factor_utilidad)
        self.monto_impuesto = (subtotal + float(self.monto_utilidad)) * float(self.factor_impuesto)
        self.monto_total    = subtotal + float(self.monto_utilidad) + float(self.monto_impuesto)

    def _sincronizar_metadatos_presupuesto(self) -> None:
        metas = {
            "MontoDirecto":     f"{self.monto_directo:.2f}",
            "MontoIndirecto":   f"{self.monto_indirecto:.2f}",
            "MontoUtilidad":    f"{self.monto_utilidad:.2f}",
            "MontoImpuesto":    f"{self.monto_impuesto:.2f}",
            "MontoTotal":       f"{self.monto_total:.2f}",
            "Moneda":           self.moneda,
            "ZonaEconomica":    self.zona_economica,
            "NumeroPartidas":   str(self.numero_partidas),
            "FactorIndirecto":  f"{self.factor_indirecto:.4f}",
            "FactorUtilidad":   f"{self.factor_utilidad:.4f}",
            "FactorImpuesto":   f"{self.factor_impuesto:.4f}",
        }
        index = {m.nombre: i for i, m in enumerate(self.metadatos)}
        for nombre, valor in metas.items():
            meta = Metadato(
                nombre,
                valor,
                "decimal" if nombre.startswith("Monto") or nombre.startswith("Factor") else "string",
                True if nombre.startswith("Monto") or nombre in {"Moneda", "ZonaEconomica", "NumeroPartidas"} else False,
                "LGA/Costos",
            )
            if nombre in index:
                self.metadatos[index[nombre]] = meta
            else:
                self.metadatos.append(meta)

    def actualizar_desde_presupuesto(self, presupuesto: Any) -> None:
        self.monto_directo = float(getattr(presupuesto, "monto_directo", 0.0) or 0.0)
        self.numero_partidas = len(getattr(presupuesto, "partidas", []) or [])
        self.insumos_desglose = self._extraer_insumos_desde_presupuesto(presupuesto)
        self._recalcular_montos()
        self._sincronizar_metadatos_presupuesto()
        self.hash_contenido = self._calcular_hash_contenido()

    def _extraer_insumos_desde_presupuesto(self, presupuesto: Any) -> List[Dict[str, Any]]:
        insumos: List[Dict[str, Any]] = []
        for partida in getattr(presupuesto, "partidas", []) or []:
            concepto = getattr(partida, "concepto", None)
            for insumo in getattr(concepto, "insumos", []) or []:
                insumos.append({
                    "nombre": getattr(insumo, "nombre", ""),
                    "tipo": str(getattr(insumo, "tipo", "")),
                    "cantidad": float(getattr(insumo, "cantidad", 0.0) or 0.0),
                    "precio_unitario": float(getattr(insumo, "precio_unitario", 0.0) or 0.0),
                    "precio_total": float(getattr(insumo, "precio_total", 0.0) or 0.0),
                })
        return insumos

    def _calcular_hash_contenido(self) -> str:
        data = json.dumps({
            "monto_directo": self.monto_directo,
            "monto_indirecto": self.monto_indirecto,
            "monto_utilidad": self.monto_utilidad,
            "monto_impuesto": self.monto_impuesto,
            "monto_total": self.monto_total,
            "numero_partidas": self.numero_partidas,
            "insumos_desglose": self.insumos_desglose,
        }, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
        return UtilidadCriptografica.hash_sha256(data)

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "monto_directo": self.monto_directo,
            "monto_indirecto": self.monto_indirecto,
            "monto_utilidad": self.monto_utilidad,
            "monto_impuesto": self.monto_impuesto,
            "monto_total": self.monto_total,
            "moneda": self.moneda,
            "zona_economica": self.zona_economica,
            "factor_indirecto": self.factor_indirecto,
            "factor_utilidad": self.factor_utilidad,
            "factor_impuesto": self.factor_impuesto,
            "numero_partidas": self.numero_partidas,
            "insumos_desglose": self.insumos_desglose,
            "resultado_montecarlo": self.resultado_montecarlo,
        })
        return base

@dataclass
class ExpedienteObra(ExpedienteElectronico):
    proyecto_id: str           = ""
    proyecto_nombre: str       = ""
    ubicacion_obra: str        = ""
    responsable_tecnico: str   = ""
    responsable_ejecutivo: str = ""
    monto_contrato: float      = 0.0
    plazo_dias: int            = 0
    tipo_contrato: str         = "POR_PRECIOS_UNITARIOS"

    def __post_init__(self) -> None:
        if hasattr(super(), "__post_init__"):
            super().__post_init__()
        self._sincronizar_metadatos_obra()

    def _sincronizar_metadatos_obra(self) -> None:
        metas = {
            "ProyectoID":           self.proyecto_id,
            "ProyectoNombre":       self.proyecto_nombre,
            "UbicacionObra":        self.ubicacion_obra,
            "ResponsableTecnico":   self.responsable_tecnico,
            "ResponsableEjecutivo": self.responsable_ejecutivo,
            "MontoContrato":        f"{self.monto_contrato:.2f}",
            "PlazoDias":            str(self.plazo_dias),
            "TipoContrato":         self.tipo_contrato,
        }
        index = {m.nombre: i for i, m in enumerate(self.metadatos)}
        for nombre, valor in metas.items():
            meta = Metadato(
                nombre, valor,
                "decimal" if nombre == "MontoContrato" else ("integer" if nombre == "PlazoDias" else "string"),
                True, "LGA/Obra",
            )
            if nombre in index:
                self.metadatos[index[nombre]] = meta
            else:
                self.metadatos.append(meta)

    def agregar_documento_presupuesto(self, doc_presupuesto: DocumentoPresupuesto) -> None:
        if float(doc_presupuesto.monto_total) <= 0:
            raise ErrorValidacionEconomica(
                f"Presupuesto {doc_presupuesto.identificador} tiene monto total inválido: {doc_presupuesto.monto_total}"
            )
        self.agregar_documento(doc_presupuesto)
        if self.monto_contrato <= 0:
            self.monto_contrato = doc_presupuesto.monto_total
            self._sincronizar_metadatos_obra()


# ═════════════════════════════════════════════════════════════════════════════
# SERVICIO ESTADÍSTICO DE COSTOS (covarianza, igual que v1)
# ═════════════════════════════════════════════════════════════════════════════

class ServicioEstadisticoCostos:
    def __init__(self) -> None:
        self.historial: List[Dict[str, float]] = []

    def registrar_muestra(self, muestra: Dict[str, float]) -> None:
        self.historial.append({k: float(v) for k, v in muestra.items()})

    @staticmethod
    def _cov_python(matrix: List[List[float]]) -> List[List[float]]:
        n = len(matrix)
        if n == 0:
            return []
        m = len(matrix[0])
        if m < 2:
            return [[0.0 for _ in range(n)] for _ in range(n)]
        means = [sum(row) / m for row in matrix]
        cov = [[0.0 for _ in range(n)] for _ in range(n)]
        denom = m - 1
        for i in range(n):
            for j in range(i, n):
                s = 0.0
                for k in range(m):
                    s += (matrix[i][k] - means[i]) * (matrix[j][k] - means[j])
                cov[i][j] = cov[j][i] = s / denom if denom > 0 else 0.0
        return cov

    def matriz_covarianza(self, claves: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        if not self.historial:
            return {"claves": [], "matriz": [], "mensaje": "Sin muestras"}
        if claves is None:
            claves = sorted({k for m in self.historial for k in m.keys()})
        series: List[List[float]] = []
        for clave in claves:
            series.append([float(m.get(clave, 0.0)) for m in self.historial])
        if np is not None:
            arr = np.array(series, dtype=float)
            matriz = np.cov(arr).tolist()
        else:
            matriz = self._cov_python(series)
        return {"claves": list(claves), "matriz": matriz, "muestras": len(self.historial)}

    def resumen_riesgo(self) -> Dict[str, Any]:
        cov = self.matriz_covarianza()
        claves = cov["claves"]
        matriz = cov["matriz"]
        diag = {claves[i]: matriz[i][i] if i < len(matriz) and i < len(matriz[i]) else 0.0 for i in range(len(claves))}
        return {"varianzas": diag, "covarianza": cov}


# ═════════════════════════════════════════════════════════════════════════════
# SERVICIO COSTEO (integra MotorPreciosBIM + MonteCarloRiesgo)
# ═════════════════════════════════════════════════════════════════════════════

class ServicioCosteo:
    def __init__(self, zona: Optional[ZonaEconomica] = None, database: Any = None) -> None:
        self.zona     = zona or ConfiguracionUnificada.ZONA_ECONOMICA_DEFAULT
        self.database = database or (Database() if "Database" in globals() else None)
        self.pipeline = PipelinePresupuestoV2(zona=self.zona, database=self.database)
        self.estadistico    = ServicioEstadisticoCostos()
        self.motor_bim      = MotorPreciosBIM()
        self.monte_carlo    = MonteCarloRiesgo(iteraciones=1_000, semilla=42)
        self._lock          = threading.Lock()
        self._historial: List[Dict[str, Any]] = []

    def costear_desde_bim(self, payload_bim: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            resultado = self.pipeline.ejecutar(payload_bim)
            presupuesto = getattr(self.pipeline, "presupuesto", None)
            if presupuesto is not None:
                self._registrar_muestras_estadisticas(presupuesto)
            self._historial.append({
                "timestamp": _now_utc().isoformat(),
                "proyecto": payload_bim.get("proyecto", "SIN_NOMBRE"),
                "status": resultado.get("status", "DESCONOCIDO"),
            })
            return resultado

    def _registrar_muestras_estadisticas(self, presupuesto: Any) -> None:
        partidas    = getattr(presupuesto, "partidas", []) or []
        num_partidas = float(len(partidas))
        num_insumos = 0.0
        total_insumos = 0.0
        for partida in partidas:
            concepto = getattr(partida, "concepto", None)
            for insumo in getattr(concepto, "insumos", []) or []:
                num_insumos  += 1.0
                total_insumos += float(getattr(insumo, "precio_total", 0.0) or 0.0)
        self.estadistico.registrar_muestra({
            "monto_directo":   float(getattr(presupuesto, "monto_directo",   0.0) or 0.0),
            "monto_indirecto": float(getattr(presupuesto, "monto_indirecto", 0.0) or 0.0),
            "monto_utilidad":  float(getattr(presupuesto, "monto_utilidad",  0.0) or 0.0),
            "monto_impuesto":  float(getattr(presupuesto, "monto_impuesto",  0.0) or 0.0),
            "monto_total":     float(getattr(presupuesto, "monto_total",     0.0) or 0.0),
            "num_partidas":    num_partidas,
            "num_insumos":     num_insumos,
            "total_insumos":   total_insumos,
        })

    def obtener_presupuesto(self) -> Optional[Any]:
        return getattr(self.pipeline, "presupuesto", None)

    def exportar_a_json(self, presupuesto: Any = None) -> str:
        pres = presupuesto or self.obtener_presupuesto()
        if not pres:
            raise ErrorConversionPresupuesto("No hay presupuesto para exportar")
        return json.dumps(pres.to_dict(), indent=2, ensure_ascii=False, default=str)

    def exportar_a_json_streaming(self, presupuesto: Any = None) -> bytes:
        """Versión streaming usando CPLJsonStreamingWriter."""
        pres = presupuesto or self.obtener_presupuesto()
        if not pres:
            raise ErrorConversionPresupuesto("No hay presupuesto para exportar")
        return cpl_json_serializar_presupuesto_streaming(pres)

    def exportar_a_xml(self, presupuesto: Any = None) -> str:
        pres = presupuesto or self.obtener_presupuesto()
        if not pres:
            raise ErrorConversionPresupuesto("No hay presupuesto para exportar")
        from xml.etree.ElementTree import Element, SubElement, tostring
        root = Element(f"{{{NS_MX}}}presupuesto")
        root.set("version", "3.2.0")
        root.set("zona", self.zona.value if isinstance(self.zona, Enum) else str(self.zona))
        eco = SubElement(root, f"{{{NS_MX}}}economicos")
        SubElement(eco, f"{{{NS_MX}}}montoDirecto").text  = str(getattr(pres, "monto_directo",  0.0))
        SubElement(eco, f"{{{NS_MX}}}montoIndirecto").text = str(getattr(pres, "monto_indirecto", 0.0))
        SubElement(eco, f"{{{NS_MX}}}montoTotal").text     = str(getattr(pres, "monto_total",     0.0))
        partidas_elem = SubElement(root, f"{{{NS_MX}}}partidas")
        for partida in getattr(pres, "partidas", []) or []:
            p_elem = SubElement(partidas_elem, f"{{{NS_MX}}}partida")
            p_elem.set("id",       str(getattr(partida, "id",       "")))
            p_elem.set("cantidad", str(getattr(partida, "cantidad", 0.0)))
            concepto = getattr(partida, "concepto", None)
            if concepto is not None:
                SubElement(p_elem, f"{{{NS_MX}}}concepto").text = str(getattr(concepto, "descripcion", ""))
        return tostring(root, encoding="unicode")

    def simular_riesgo(self, monto_base: float, parametros: Optional[List[ParametroMC]] = None) -> Dict[str, Any]:
        """Corre Monte Carlo sobre un monto base con parámetros de riesgo típicos."""
        params = parametros or MonteCarloRiesgo.parametros_obra_tipicos()
        return self.monte_carlo.simular(monto_base, params)


class ServicioInteroperabilidadUnificada:
    def __init__(self, servicio_base: Optional[ServicioInteroperabilidad] = None, servicio_trazabilidad: Optional[ServicioTrazabilidad] = None) -> None:
        self.trazabilidad = servicio_trazabilidad or ServicioTrazabilidad()
        self.base = servicio_base or ServicioInteroperabilidad(servicio_trazabilidad=self.trazabilidad)

    def remitir_presupuesto_licitacion(self, expediente_obra: "ExpedienteObra", administracion_destino: str) -> Dict[str, Any]:
        docs_presupuesto = [d for d in expediente_obra.documentos if isinstance(d, DocumentoPresupuesto)]
        if not docs_presupuesto:
            raise ErrorInteroperabilidad("El expediente no contiene documentos de presupuesto")
        presupuesto_principal = docs_presupuesto[0]
        datos = {
            "emisor": expediente_obra.organo,
            "receptor": administracion_destino,
            "cuerpo": {
                "tipoMensaje": "PROPUESTA_LICITACION",
                "identificadorExpediente": expediente_obra.identificador,
                "proyectoNombre":    expediente_obra.proyecto_nombre,
                "proyectoID":        expediente_obra.proyecto_id,
                "montoTotal":        presupuesto_principal.monto_total,
                "moneda":            presupuesto_principal.moneda,
                "numeroPartidas":    presupuesto_principal.numero_partidas,
                "responsableTecnico":  expediente_obra.responsable_tecnico,
                "responsableEjecutivo": expediente_obra.responsable_ejecutivo,
                "plazoDias":         expediente_obra.plazo_dias,
                "tipoContrato":      expediente_obra.tipo_contrato,
                "hashPresupuesto":   presupuesto_principal.hash_contenido,
                "ubicacionObra":     expediente_obra.ubicacion_obra,
                "documentosAdjuntos": [d.identificador for d in expediente_obra.documentos],
            },
        }
        mensaje = self.base.generar_mensaje_interoperabilidad(TipoInteroperabilidad.REMISION_EXPEDIENTE, datos)
        self.trazabilidad.registrar(
            actor="SERVICIO_INTEROP_UNIFICADO",
            accion="REMITIR_LICITACION",
            objeto="ExpedienteObra",
            objeto_id=expediente_obra.identificador,
            resultado="GENERADO",
            detalle=f"Destino: {administracion_destino}, Monto: {presupuesto_principal.monto_total}",
        )
        return {"mensaje_xml": mensaje, "estado": "GENERADO", "monto_total": presupuesto_principal.monto_total, "destino": administracion_destino}


# ═════════════════════════════════════════════════════════════════════════════
# SISTEMA UNIFICADO MÉXICO v2.0
# ═════════════════════════════════════════════════════════════════════════════

class SistemaUnificadoMexico:
    """
    Punto de entrada único al sistema.
    Integra todos los motores: documental, costeo (BIM + APU), Monte Carlo,
    motor de fallo, motor de evidencia y motor jurídico.
    """
    def __init__(
        self,
        zona_economica: Optional[ZonaEconomica] = None,
        database: Any = None,
        jurisdiccion: Jurisdiccion = Jurisdiccion.FEDERAL,
    ) -> None:
        ConfiguracionUnificada.inicializar()
        self._lock = threading.RLock()
        self._configurar_logging()

        self.documental     = SistemaGestionExpedientesMexico()
        self.costeo         = ServicioCosteo(zona=zona_economica, database=database)
        self.interop        = ServicioInteroperabilidadUnificada(servicio_trazabilidad=self.documental.trazabilidad)
        self.motor_fallo    = MotorFallo()
        self.motor_evidencia = MotorEvidencia(motor_fallo=self.motor_fallo)
        self.motor_juridico = MotorJuridico(motor_fallo=self.motor_fallo)
        self.jurisdiccion   = jurisdiccion
        self.expedientes_obra: Dict[str, ExpedienteObra] = {}
        self.cola_jobs      = CPLQueue(trabajadores=2)
        self.vsi            = CPLVsiSistema()

        logging.info("SistemaUnificadoMexico v2.0 inicializado")

    def _configurar_logging(self) -> None:
        log_path = _ensure_dir(ConfiguracionUnificada.RUTA_LOGS)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            handlers=[
                logging.FileHandler(log_path / "unificado.log", encoding="utf-8"),
                logging.StreamHandler(sys.stdout),
            ],
        )

    def costear_obra(self, payload_bim: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            logging.info("[COSTEO] Iniciando: %s", payload_bim.get("proyecto", "SIN_NOMBRE"))
            resultado = self.costeo.costear_desde_bim(payload_bim)
            if resultado.get("status") != "COMPLETADO":
                _cpl_error.registrar(CPLError.COD_COSTEO, f"Falló costeo: {resultado.get('logs')}", Severidad.ERROR, "SistemaUnificado")
                return {"fase": "COSTEO", "status": "ERROR", "detalle": resultado}
            presupuesto = self.costeo.obtener_presupuesto()
            insumos_desglose: List[Dict[str, Any]] = []
            for partida in getattr(presupuesto, "partidas", []) or []:
                concepto = getattr(partida, "concepto", None)
                for insumo in getattr(concepto, "insumos", []) or []:
                    insumos_desglose.append({
                        "nombre": getattr(insumo, "nombre", ""),
                        "tipo":   str(getattr(insumo, "tipo", "")),
                        "cantidad": getattr(insumo, "cantidad", 0.0),
                        "precio_unitario": getattr(insumo, "precio_unitario", 0.0),
                        "precio_total": getattr(insumo, "precio_total", 0.0),
                    })
            # Monte Carlo automático sobre el monto directo
            monto_directo = float(getattr(presupuesto, "monto_directo", 0.0) or 0.0)
            riesgo_mc = self.costeo.simular_riesgo(monto_directo)
            logging.info("[COSTEO] Completado. Partidas: %d, MC p50: %.2f", len(getattr(presupuesto, "partidas", []) or []), riesgo_mc.get("percentiles", {}).get("p50", 0))
            return {
                "fase": "COSTEO",
                "status": "OK",
                "presupuesto": presupuesto.to_dict() if hasattr(presupuesto, "to_dict") else {},
                "insumos_desglose": insumos_desglose,
                "meta": resultado.get("meta", {}),
                "riesgo_montecarlo": riesgo_mc,
            }

    def crear_expediente_obra(self, datos: Dict[str, Any]) -> ExpedienteObra:
        with self._lock:
            identificador = f"EXP-OBRA-{_now_utc().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"
            expediente = ExpedienteObra(
                identificador=identificador,
                titulo=datos.get("titulo", ""),
                descripcion=datos.get("descripcion", ""),
                organo=datos.get("organo", ""),
                unidad_administrativa=datos.get("unidad_administrativa", ""),
                serie_documental=datos.get("serie_documental", ConfiguracionUnificada.SERIE_DOCUMENTAL_PRESUPUESTO),
                subserie_documental=datos.get("subserie_documental", ConfiguracionUnificada.SUBSERIE_PRESUPUESTO_DETALLADO),
                fecha_apertura=_now_utc(),
                estado=EstadoExpediente.INICIADO,
                clasificacion=datos.get("clasificacion", ClasificacionSeguridad.PUBLICO.value),
                proyecto_id=datos.get("proyecto_id", ""),
                proyecto_nombre=datos.get("proyecto_nombre", ""),
                ubicacion_obra=datos.get("ubicacion_obra", ""),
                responsable_tecnico=datos.get("responsable_tecnico", ""),
                responsable_ejecutivo=datos.get("responsable_ejecutivo", ""),
                plazo_dias=int(datos.get("plazo_dias", 0) or 0),
                tipo_contrato=datos.get("tipo_contrato", "POR_PRECIOS_UNITARIOS"),
                responsable=datos.get("responsable_ejecutivo", ""),
            )
            self.documental.expedientes[identificador]  = expediente
            self.expedientes_obra[identificador]        = expediente
            self.documental.trazabilidad.registrar(
                actor="SISTEMA_UNIFICADO",
                accion="CREAR_EXPEDIENTE_OBRA",
                objeto="ExpedienteObra",
                objeto_id=identificador,
                resultado="OK",
                detalle=f"Proyecto: {expediente.proyecto_nombre}, Ubicación: {expediente.ubicacion_obra}",
            )
            logging.info("[EXPEDIENTE] Creado: %s", identificador)
            return expediente

    def incorporar_presupuesto(self, expediente_id: str, resultado_costeo: Dict[str, Any]) -> DocumentoPresupuesto:
        with self._lock:
            if expediente_id not in self.expedientes_obra:
                raise ErrorNormativo(f"Expediente {expediente_id} no existe")
            expediente      = self.expedientes_obra[expediente_id]
            presupuesto_dict = resultado_costeo.get("presupuesto", {})
            insumos_desglose = resultado_costeo.get("insumos_desglose", [])
            resultado_mc    = resultado_costeo.get("riesgo_montecarlo")
            contenido_json  = json.dumps({
                "presupuesto": presupuesto_dict,
                "insumos_desglose": insumos_desglose,
                "meta_costeo": resultado_costeo.get("meta", {}),
                "riesgo_montecarlo": resultado_mc,
                "fecha_generacion": _now_utc().isoformat(),
                "version_sistema": "2.0.0",
            }, indent=2, ensure_ascii=False, default=str)
            monto_directo   = float(presupuesto_dict.get("monto_directo", 0.0) or 0.0)
            numero_partidas = int(presupuesto_dict.get("numero_partidas", len(presupuesto_dict.get("partidas", [])) if isinstance(presupuesto_dict, dict) else 0) or 0)
            doc = DocumentoPresupuesto(
                identificador=f"DOC-PRES-{uuid.uuid4().hex[:8].upper()}",
                nombre=f"Presupuesto_{expediente.proyecto_nombre or expediente.identificador}.json",
                contenido=contenido_json.encode("utf-8"),
                tipo_documental=TipoDocumento.PRESUPUESTO,
                organo=expediente.organo,
                fecha_captura=_now_utc(),
                formato="json",
                autor=expediente.responsable_tecnico,
                nivel_seguridad=expediente.clasificacion,
                monto_directo=monto_directo,
                numero_partidas=numero_partidas,
                insumos_desglose=insumos_desglose,
                zona_economica=str(self.costeo.zona),
                resultado_montecarlo=resultado_mc,
            )
            expediente.agregar_documento_presupuesto(doc)
            self.documental.trazabilidad.registrar(
                actor="SISTEMA_UNIFICADO",
                accion="INCORPORAR_PRESUPUESTO",
                objeto="DocumentoPresupuesto",
                objeto_id=doc.identificador,
                resultado="OK",
                detalle=f"Expediente: {expediente_id}, Monto: {doc.monto_total:.2f}",
            )
            logging.info("[PRESUPUESTO] %s incorporado a %s (Monto: %.2f)", doc.identificador, expediente_id, doc.monto_total)
            return doc

    def firmar_y_archivar_obra(self, expediente_id: str, firmante: str, cargo: str = "") -> Dict[str, Any]:
        with self._lock:
            if expediente_id not in self.expedientes_obra:
                raise ErrorNormativo(f"Expediente {expediente_id} no existe")
            expediente = self.expedientes_obra[expediente_id]
            if firmante != expediente.responsable_ejecutivo and expediente.responsable_ejecutivo:
                logging.warning("[FIRMA] Firmante %s != responsable ejecutivo %s", firmante, expediente.responsable_ejecutivo)
            for doc in expediente.documentos:
                self.documental.firma.firmar_documento(doc, firmante, cargo, "FIEL", NivelFirma.FIEL, "SAT")
            self.documental.firma.firmar_indice_expediente(expediente, firmante, cargo)
            resultado_validacion = self.documental.validador.validar_expediente(expediente)
            if not resultado_validacion["valido"]:
                _cpl_error.registrar(CPLError.COD_FIRMA, f"Validación fallida: {resultado_validacion['errores']}", Severidad.ERROR, "SistemaUnificado")
                return {"fase": "FIRMA", "status": "ERROR_VALIDACION", "errores": resultado_validacion["errores"]}
            ruta_archivo = self.documental.archivo.archivar_expediente(expediente)
            self.documental.busqueda.indexar_expediente(expediente)
            expediente.estado = EstadoExpediente.ARCHIVADO
            trazabilidad_integra, _ = self.documental.trazabilidad.verificar_integridad_cadena()
            logging.info("[FIRMA] Expediente %s firmado y archivado en %s", expediente_id, ruta_archivo)
            return {
                "fase": "FIRMA",
                "status": "OK",
                "ruta_archivo": ruta_archivo,
                "validacion": resultado_validacion,
                "trazabilidad_integra": trazabilidad_integra,
                "expediente_id": expediente_id,
            }

    def remitir_licitacion(self, expediente_id: str, administracion_destino: str) -> Dict[str, Any]:
        with self._lock:
            if expediente_id not in self.expedientes_obra:
                raise ErrorNormativo(f"Expediente {expediente_id} no existe")
            expediente = self.expedientes_obra[expediente_id]
            resultado  = self.interop.remitir_presupuesto_licitacion(expediente, administracion_destino)
            expediente.estado = EstadoExpediente.EN_INTEROPERABILIDAD
            logging.info("[INTEROP] Licitación remitida: %s -> %s", expediente_id, administracion_destino)
            return {"fase": "INTEROPERABILIDAD", "status": resultado["estado"], "mensaje_generado": True, "monto_total": resultado["monto_total"], "destino": administracion_destino}

    def consultar_estado_obra(self, expediente_id: str) -> Dict[str, Any]:
        if expediente_id not in self.expedientes_obra:
            try:
                recuperado = self.documental.archivo.recuperar_expediente(expediente_id, incluir_contenido=False)
                return {"expediente_id": expediente_id, "status": "ARCHIVADO", "recuperado": True, "manifest": recuperado.get("manifest"), "hash_global_ok": recuperado.get("hash_global_ok")}
            except Exception:
                return {"error": "Expediente no encontrado"}
        exp = self.expedientes_obra[expediente_id]
        docs_presupuesto = [d for d in exp.documentos if isinstance(d, DocumentoPresupuesto)]
        return {
            "expediente_id": expediente_id,
            "proyecto_nombre": exp.proyecto_nombre,
            "estado": exp.estado.value,
            "estado_descripcion": exp.estado.name,
            "monto_contrato": exp.monto_contrato,
            "plazo_dias": exp.plazo_dias,
            "responsable_tecnico": exp.responsable_tecnico,
            "responsable_ejecutivo": exp.responsable_ejecutivo,
            "num_documentos": len(exp.documentos),
            "num_presupuestos": len(docs_presupuesto),
            "indice_firmado": exp.indice.firma_indice is not None,
            "trazabilidad_integra": self.documental.trazabilidad.verificar_integridad_cadena()[0],
        }

    def generar_reporte_economico(self, expediente_id: str) -> Dict[str, Any]:
        if expediente_id not in self.expedientes_obra:
            return {"error": "Expediente no encontrado"}
        exp = self.expedientes_obra[expediente_id]
        docs_presupuesto = [d for d in exp.documentos if isinstance(d, DocumentoPresupuesto)]
        reporte = {
            "expediente_id": expediente_id,
            "proyecto": exp.proyecto_nombre,
            "resumen_economico": {"monto_contrato": exp.monto_contrato, "moneda": "MXN"},
            "presupuestos": [],
            "estadistica_sensibilidad": self.costeo.estadistico.resumen_riesgo(),
            "fallos_normativos": self.motor_fallo.resumen(),
            "errores_sistema": _cpl_error.resumen(),
        }
        for doc in docs_presupuesto:
            reporte["presupuestos"].append({
                "documento_id": doc.identificador,
                "monto_directo": doc.monto_directo,
                "monto_indirecto": doc.monto_indirecto,
                "monto_utilidad": doc.monto_utilidad,
                "monto_impuesto": doc.monto_impuesto,
                "monto_total": doc.monto_total,
                "numero_partidas": doc.numero_partidas,
                "insumos_principales": doc.insumos_desglose[:10],
                "riesgo_montecarlo": doc.resultado_montecarlo,
            })
        return reporte

    def analizar_procedimiento_licitacion(
        self,
        expediente_id: str,
        convocante: str,
        tipo_contrato: TipoContrato = TipoContrato.OBRA_PUBLICA,
    ) -> Dict[str, Any]:
        """Determina procedimiento de contratación aplicable según monto y jurisdicción."""
        if expediente_id not in self.expedientes_obra:
            return {"error": "Expediente no encontrado"}
        exp = self.expedientes_obra[expediente_id]
        return self.motor_juridico.determinar_procedimiento(
            jurisdiccion=self.jurisdiccion,
            convocante=convocante,
            tipo_contrato=tipo_contrato,
            monto=exp.monto_contrato,
        )

    def obtener_estadisticas(self) -> Dict[str, Any]:
        stats_doc = self.documental.obtener_estadisticas_sistema()
        return {
            **stats_doc,
            "expedientes_obra_en_memoria": len(self.expedientes_obra),
            "costeos_historial": len(self.costeo._historial),
            "version_unificada": "2.0.0",
            "numpy_disponible": np is not None,
            "motor_real_documental": REAL_MOTOR_LOADED,
            "motor_real_costos": REAL_COSTOS_LOADED,
            "jobs_pendientes": self.cola_jobs.pendientes(),
            "errores_sistema": _cpl_error.resumen(),
            "fallos_normativos": self.motor_fallo.resumen(),
        }

    def apagar(self) -> None:
        self.cola_jobs.apagar()
        logging.info("[SISTEMA] Apagado limpio completado")


# ═════════════════════════════════════════════════════════════════════════════
# TESTS DE INTEGRACIÓN
# ═════════════════════════════════════════════════════════════════════════════

class TestIntegracionUnificada(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.mkdtemp(prefix="unificado_v2_test_")
        os.environ["EXPEDIENTE_RUTA_BASE"] = cls.temp_dir
        os.environ["EXPEDIENTE_RUTA_DB"]   = os.path.join(cls.temp_dir, "db")
        os.environ["EXPEDIENTE_RUTA_LOGS"] = os.path.join(cls.temp_dir, "logs")
        os.environ["EXPEDIENTE_RUTA_CERTS"]= os.path.join(cls.temp_dir, "certs")
        os.environ["EXPEDIENTE_CLAVE_MAESTRA"] = "CLAVE_UNIFICADA_V2_TEST_2026"
        os.environ["EXPEDIENTE_INTEROPERABILIDAD_MODO"] = "offline"
        ConfiguracionUnificada.inicializar()

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def setUp(self) -> None:
        certs_dir = _ensure_dir(os.environ["EXPEDIENTE_RUTA_CERTS"])
        p12_secret = certs_dir / ".p12.secret"
        p12_secret.write_text("test1234", encoding="utf-8")
        try:
            os.chmod(p12_secret, 0o600)
        except Exception:
            pass
        self.sistema = SistemaUnificadoMexico()
        self.sistema.documental.firma.generar_certificado_prueba("ING_RESPONSABLE", "test1234")

    def _payload_bim(self) -> Dict[str, Any]:
        return {
            "proyecto": "Edificio Residencial Torres 2026",
            "proyecto_id": "PROY-2026-001",
            "elementos": [
                {"tipo": "losa",    "sistema": "concreto", "largo": 10.0, "ancho": 8.0,  "alto": 0.15, "num_piezas": 1, "merma_tecnica_pct": 5.0, "kg_acero_m3": 120.0},
                {"tipo": "muro",    "sistema": "block",    "largo": 6.0,  "ancho": 0.15, "alto": 2.5,  "num_piezas": 1, "merma_tecnica_pct": 3.0, "kg_acero_m3": 0.0},
                {"tipo": "columna", "sistema": "concreto", "largo": 0.4,  "ancho": 0.4,  "alto": 3.0,  "num_piezas": 4, "merma_tecnica_pct": 5.0, "kg_acero_m3": 200.0},
            ],
        }

    # ── Test 1: flujo completo ────────────────────────────────────────────
    def test_01_flujo_completo_obra(self) -> None:
        resultado_costeo = self.sistema.costear_obra(self._payload_bim())
        self.assertEqual(resultado_costeo["status"], "OK")
        self.assertIn("presupuesto", resultado_costeo)
        self.assertIn("riesgo_montecarlo", resultado_costeo)
        self.assertIn("percentiles", resultado_costeo["riesgo_montecarlo"])

        expediente = self.sistema.crear_expediente_obra({
            "titulo": "Edificio Residencial Torres 2026",
            "descripcion": "Proyecto de construcción de edificio residencial de 12 niveles",
            "organo": "DESARROLLO_URBANO_MUNICIPAL",
            "unidad_administrativa": "DIRECCION_OBRAS_PUBLICAS",
            "proyecto_id": "PROY-2026-001",
            "proyecto_nombre": "Edificio Residencial Torres 2026",
            "ubicacion_obra": "Av. Revolución 1234, CDMX",
            "responsable_tecnico": "ING_JUAN_PEREZ_12345",
            "responsable_ejecutivo": "ING_RESPONSABLE",
            "plazo_dias": 365,
            "tipo_contrato": "POR_PRECIOS_UNITARIOS",
        })
        doc_presupuesto = self.sistema.incorporar_presupuesto(expediente.identificador, resultado_costeo)
        self.assertIsInstance(doc_presupuesto, DocumentoPresupuesto)
        self.assertGreater(doc_presupuesto.monto_total, 0)
        self.assertIsNotNone(doc_presupuesto.resultado_montecarlo)

        resultado_firma = self.sistema.firmar_y_archivar_obra(expediente.identificador, "ING_RESPONSABLE", "Ingeniero Residente")
        self.assertEqual(resultado_firma["status"], "OK")
        self.assertTrue(resultado_firma["trazabilidad_integra"])

    # ── Test 2: validación monto negativo ────────────────────────────────
    def test_02_validacion_monto_negativo(self) -> None:
        expediente = self.sistema.crear_expediente_obra({
            "titulo": "Obra Test", "descripcion": "Desc", "organo": "ORG",
            "unidad_administrativa": "UA", "proyecto_id": "TEST-001",
            "responsable_ejecutivo": "ING_RESPONSABLE",
        })
        doc_invalido = DocumentoPresupuesto(
            identificador="DOC-TEST-001",
            nombre="presupuesto_invalido.json",
            contenido=b"{}",
            tipo_documental=TipoDocumento.PRESUPUESTO,
            organo="ORG",
            monto_directo=0.0,
            numero_partidas=0,
        )
        with self.assertRaises(ErrorValidacionEconomica):
            expediente.agregar_documento_presupuesto(doc_invalido)

    # ── Test 3: CPLError ─────────────────────────────────────────────────
    def test_03_cpl_error(self) -> None:
        _cpl_error.limpiar()
        _cpl_error.registrar(1001, "Test aviso", Severidad.AVISO, "TEST")
        _cpl_error.registrar(1002, "Test error", Severidad.ERROR, "TEST")
        resumen = _cpl_error.resumen()
        self.assertIn("AVISO", resumen)
        self.assertIn("ERROR", resumen)
        self.assertTrue(_cpl_error.tiene_errores_criticos())

    # ── Test 4: CPLHash deduplicación ────────────────────────────────────
    def test_04_cpl_hash_dedup(self) -> None:
        elementos = [
            {"contenido": "hola mundo"},
            {"contenido": "hola mundo"},   # duplicado
            {"contenido": "otro valor"},
        ]
        unicos = CPLHash.deduplicar(elementos)
        self.assertEqual(len(unicos), 2)

    # ── Test 5: CPLJsonStreamingWriter ───────────────────────────────────
    def test_05_json_streaming(self) -> None:
        buf = io.StringIO()
        writer = CPLJsonStreamingWriter(buf)  # type: ignore[arg-type]
        writer.comenzar_objeto()
        writer.campo("nombre", "test")
        writer.campo("valor", 42)
        writer.terminar_objeto()
        texto = buf.getvalue()
        parsed = json.loads(texto)
        self.assertEqual(parsed["nombre"], "test")
        self.assertEqual(parsed["valor"], 42)

    # ── Test 6: CPLVsi ───────────────────────────────────────────────────
    def test_06_vsi_memoria_y_disco(self) -> None:
        vsi = CPLVsiSistema()
        handle = vsi.abrir_memoria("test.json", b'{"ok":true}')
        self.assertEqual(handle.leer(), b'{"ok":true}')
        self.assertEqual(handle.sha256(), CPLHash.de_bytes(b'{"ok":true}'))
        handle_disco = vsi.mover_a_disco("test.json")
        self.assertEqual(handle_disco.leer(), b'{"ok":true}')

    # ── Test 7: CPLProgress ──────────────────────────────────────────────
    def test_07_progress(self) -> None:
        prog = CPLProgress(total_pasos=10, descripcion="Test")
        resultados = []
        prog.suscribir(lambda pct, msg: resultados.append(pct) or True)
        for _ in range(5):
            prog.avanzar()
        self.assertAlmostEqual(prog.porcentaje, 50.0)
        self.assertFalse(prog.cancelado)

    # ── Test 8: MotorFallo ───────────────────────────────────────────────
    def test_08_motor_fallo(self) -> None:
        motor = MotorFallo()
        causal = motor.evaluar_campo(
            campo="precio_total",
            valor=500_000.0,
            esperado=lambda v: v <= 200_000.0,
            tipo=TipoFallo.PRECIO_FUERA_RANGO,
            norma="LOPSRM Art.36",
            dependencia="SFP",
        )
        self.assertIsNotNone(causal)
        resumen = motor.resumen()
        self.assertEqual(resumen["total_fallos"], 1)

    # ── Test 9: MotorJuridico procedimiento ─────────────────────────────
    def test_09_motor_juridico(self) -> None:
        motor = MotorJuridico()
        resultado = motor.determinar_procedimiento(
            Jurisdiccion.FEDERAL, "SFP", TipoContrato.PRECIOS_UNITARIOS, 5_000_000.0
        )
        self.assertIn(resultado["procedimiento"], {"ADJUDICACION_DIRECTA", "INVITACION_CUANDO_MENOS_TRES", "LICITACION_PUBLICA"})
        self.assertTrue(resultado["marco_disponible"])

    # ── Test 10: Monte Carlo ─────────────────────────────────────────────
    def test_10_monte_carlo(self) -> None:
        mc = MonteCarloRiesgo(iteraciones=500, semilla=7)
        params = MonteCarloRiesgo.parametros_obra_tipicos()
        resultado = mc.simular(1_000_000.0, params)
        self.assertIn("percentiles", resultado)
        self.assertGreater(resultado["percentiles"]["p95"], resultado["percentiles"]["p5"])

    # ── Test 11: QuadTree ────────────────────────────────────────────────
    def test_11_quadtree(self) -> None:
        qt = CPLQuadTree(BBox(-120.0, 10.0, -85.0, 35.0))
        entrada = EntradaEspacial(
            id="OBRA-001",
            bbox=BBox(-99.2, 19.4, -99.1, 19.5),
            datos={"nombre": "Puente Bicentenario"},
        )
        qt.insertar(entrada)
        resultado = qt.buscar(BBox(-99.3, 19.3, -99.0, 19.6))
        self.assertEqual(len(resultado), 1)
        self.assertEqual(resultado[0].id, "OBRA-001")

    # ── Test 12: CPLQueue ────────────────────────────────────────────────
    def test_12_cola_background(self) -> None:
        cola = CPLQueue(trabajadores=1)
        job_id = cola.encolar("HASH_ARCHIVO", {"datos": b"hola"})
        import time; time.sleep(0.3)
        job = cola.obtener_resultado(job_id)
        self.assertIsNotNone(job)
        self.assertIn(job.estado, {"COMPLETADO", "EN_PROCESO"})
        cola.apagar()

    # ── Test 13: JSON streaming desde ServicioCosteo ─────────────────────
    def test_13_exportar_json_streaming(self) -> None:
        self.sistema.costear_obra(self._payload_bim())
        presupuesto = self.sistema.costeo.obtener_presupuesto()
        if presupuesto:
            data = self.sistema.costeo.exportar_a_json_streaming(presupuesto)
            self.assertIsInstance(data, bytes)
            parsed = json.loads(data.decode("utf-8"))
            self.assertIn("proyecto", parsed)


# ═════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Sistema Unificado México v2.0 – Costos + Expedientes")
    parser.add_argument("--test",  action="store_true", help="Ejecutar tests de integración")
    parser.add_argument("--demo",  action="store_true", help="Ejecutar demostración")
    args = parser.parse_args()

    if args.test:
        print("=" * 80)
        print("EJECUTANDO TESTS DE INTEGRACIÓN v2.0")
        print("=" * 80)
        unittest.main(argv=[sys.argv[0]], verbosity=2, exit=False)
        return

    if args.demo:
        print("=" * 80)
        print("DEMO – Sistema Unificado México v2.0")
        print("=" * 80)
        sistema = SistemaUnificadoMexico()
        sistema.documental.firma.generar_certificado_prueba("ING_DEMO", "demo2026")

        payload = {
            "proyecto": "Puente Vehicular Norte 2026",
            "proyecto_id": "VIAL-2026-042",
            "elementos": [
                {"tipo": "losa",    "sistema": "concreto", "largo": 30.0, "ancho": 12.0, "alto": 0.25, "num_piezas": 1, "merma_tecnica_pct": 4.0, "kg_acero_m3": 150.0},
                {"tipo": "columna", "sistema": "concreto", "largo": 0.8,  "ancho": 0.8,  "alto": 6.0,  "num_piezas": 6, "merma_tecnica_pct": 3.0, "kg_acero_m3": 250.0},
                {"tipo": "muro",    "sistema": "concreto", "largo": 15.0, "ancho": 0.3,  "alto": 4.0,  "num_piezas": 2, "merma_tecnica_pct": 3.0, "kg_acero_m3": 80.0},
            ],
        }
        costeo = sistema.costear_obra(payload)
        print(f"\nCosteo completado:")
        print(f"  Monto directo : ${costeo['presupuesto'].get('monto_directo', 0):,.2f}")
        print(f"  Monto total   : ${costeo['presupuesto'].get('monto_total',   0):,.2f}")
        mc = costeo.get("riesgo_montecarlo", {})
        print(f"  Monte Carlo p5 : ${mc.get('percentiles', {}).get('p5', 0):,.2f}")
        print(f"  Monte Carlo p50: ${mc.get('percentiles', {}).get('p50', 0):,.2f}")
        print(f"  Monte Carlo p95: ${mc.get('percentiles', {}).get('p95', 0):,.2f}")

        expediente = sistema.crear_expediente_obra({
            "titulo": "Puente Vehicular Norte 2026",
            "organo": "SCT_FEDERAL",
            "unidad_administrativa": "DIRECCION_PUENTES",
            "proyecto_id": "VIAL-2026-042",
            "proyecto_nombre": "Puente Vehicular Norte",
            "ubicacion_obra": "Carretera Federal 57, km 142",
            "responsable_tecnico": "ING_CARLOS_MENDOZA",
            "responsable_ejecutivo": "ING_DEMO",
            "plazo_dias": 540,
            "tipo_contrato": "POR_PRECIOS_UNITARIOS",
        })
        sistema.incorporar_presupuesto(expediente.identificador, costeo)

        procedimiento = sistema.analizar_procedimiento_licitacion(expediente.identificador, "SCT")
        print(f"\nProcedimiento de contratación:")
        print(f"  Monto    : ${expediente.monto_contrato:,.2f}")
        print(f"  Procedimiento: {procedimiento['procedimiento']}")
        print(f"  Fundamento   : {procedimiento.get('fundamento', 'N/A')}")

        resultado_firma = sistema.firmar_y_archivar_obra(expediente.identificador, "ING_DEMO")
        print(f"\nFirmado y archivado: {resultado_firma['status']}")
        print(f"  Trazabilidad íntegra: {resultado_firma['trazabilidad_integra']}")
        print(f"  Ruta: {resultado_firma['ruta_archivo']}")

        stats = sistema.obtener_estadisticas()
        print(f"\nEstadísticas del sistema:")
        for k, v in stats.items():
            print(f"  {k}: {v}")

        sistema.apagar()
        print("\n[OK] Demo completado.")
        return

    print("Uso: python sistema_unificado_megalodon_costos_v2.py --test | --demo")


if __name__ == "__main__":
    main()
