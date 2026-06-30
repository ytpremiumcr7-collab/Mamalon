-- =============================================================================
-- MEGALODON COSTOS — ESQUEMA DEFINITIVO v4.0
-- Compatible 100% con megalodon_costos_v3_3.py (no se renombra ninguna tabla
-- ni columna de la v3.3; solo se añaden tablas/columnas nuevas).
-- Incluye: núcleo documental, costeo/BIM/APU/riesgo, marco jurídico y reglas
-- deterministas, topografía PostGIS, jobs, marco legal estructurado (leyes y
-- artículos con FTS), base de conocimiento técnico, catálogos genéricos y
-- trazabilidad/idempotencia del pipeline ETL.
-- Generado: 2026-06-18
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS megalodon;
SET search_path TO megalodon, public, extensions;

-- =============================================================================
-- MEGALODON COSTOS v3.3
-- Esquema completo para PostgreSQL + PostGIS
-- Fecha: 2026-06-18
-- =============================================================================


-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Esquema propio

-- =============================================================================
-- FUNCIONES Y TRIGGERS AUXILIARES
-- =============================================================================

CREATE OR REPLACE FUNCTION megalodon.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION megalodon.ensure_jsonb_object(value jsonb, default_value jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(value, default_value);
$$;

-- =============================================================================
-- 1. NÚCLEO DOCUMENTAL Y EXPEDIENTES
-- =============================================================================

CREATE TABLE IF NOT EXISTS expedientes (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    identificador           text NOT NULL UNIQUE,
    titulo                  text NOT NULL,
    descripcion             text NOT NULL DEFAULT '',
    organo                  text NOT NULL,
    unidad_administrativa   text NOT NULL,
    serie_documental        text NOT NULL DEFAULT '',
    subserie_documental     text NOT NULL DEFAULT '',
    fecha_apertura          timestamptz NOT NULL DEFAULT NOW(),
    estado                  text NOT NULL CHECK (estado IN (
                                'INICIADO', 'EN_FIRMA', 'ARCHIVADO', 'EN_INTEROPERABILIDAD', 'EN_PROGRESO'
                            )),
    clasificacion           text NOT NULL CHECK (clasificacion IN ('PUBLICO', 'RESERVADO', 'CONFIDENCIAL')),
    responsable             text NOT NULL DEFAULT '',
    metadatos_jsonb         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_expedientes_updated_at
BEFORE UPDATE ON expedientes
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS expedientes_obra (
    expediente_id           uuid PRIMARY KEY REFERENCES expedientes(id) ON DELETE CASCADE,
    proyecto_id             text NOT NULL UNIQUE,
    proyecto_nombre         text NOT NULL,
    ubicacion_obra          text NOT NULL DEFAULT '',
    responsable_tecnico     text NOT NULL DEFAULT '',
    responsable_ejecutivo   text NOT NULL DEFAULT '',
    monto_contrato          numeric(18,2) NOT NULL DEFAULT 0 CHECK (monto_contrato >= 0),
    plazo_dias              integer NOT NULL DEFAULT 0 CHECK (plazo_dias >= 0),
    tipo_contrato           text NOT NULL CHECK (tipo_contrato IN (
                                'PRECIOS_UNITARIOS', 'PRECIO_ALZADO', 'MIXTO',
                                'OBRA_PUBLICA', 'SERVICIO_RELACIONADO', 'ARRENDAMIENTO', 'ADQUISICION'
                            )),
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_expedientes_obra_updated_at
BEFORE UPDATE ON expedientes_obra
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS documentos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    expediente_id           uuid NOT NULL REFERENCES expedientes(id) ON DELETE CASCADE,
    identificador           text NOT NULL UNIQUE,
    nombre                  text NOT NULL,
    contenido               bytea NULL,
    storage_uri             text NULL,
    tipo_documental         text NOT NULL CHECK (tipo_documental IN ('INFORME', 'OFICIO', 'CONTRATO', 'PRESUPUESTO')),
    organo                  text NOT NULL,
    fecha_captura           timestamptz NOT NULL DEFAULT NOW(),
    formato                 text NOT NULL DEFAULT 'json',
    autor                   text NOT NULL DEFAULT '',
    nivel_seguridad         text NOT NULL CHECK (nivel_seguridad IN ('PUBLICO', 'RESERVADO', 'CONFIDENCIAL', 'FIEL', 'SIMPLE')),
    hash_contenido          text NOT NULL DEFAULT '',
    metadatos_jsonb         jsonb NOT NULL DEFAULT '{}'::jsonb,
    firmado                 boolean NOT NULL DEFAULT FALSE,
    firma_id                uuid NULL,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_documentos_updated_at
BEFORE UPDATE ON documentos
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_documentos_expediente_id ON documentos (expediente_id);
CREATE INDEX IF NOT EXISTS idx_documentos_hash_contenido ON documentos (hash_contenido);

CREATE TABLE IF NOT EXISTS documento_metadatos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    documento_id            uuid NOT NULL REFERENCES documentos(id) ON DELETE CASCADE,
    nombre                  text NOT NULL,
    valor                   text NOT NULL,
    tipo                    text NOT NULL DEFAULT 'string',
    obligatorio             boolean NOT NULL DEFAULT FALSE,
    esquema                 text NOT NULL DEFAULT 'GENERAL',
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (documento_id, nombre)
);

CREATE TABLE IF NOT EXISTS documento_firmas (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    documento_id            uuid NOT NULL UNIQUE REFERENCES documentos(id) ON DELETE CASCADE,
    sujeto                  text NOT NULL,
    huella_certificado      text NOT NULL,
    fecha_firma             timestamptz NOT NULL,
    nivel                   text NOT NULL CHECK (nivel IN ('FIEL', 'SIMPLE')),
    autoridad               text NOT NULL DEFAULT 'SAT',
    firma_base64            text NOT NULL DEFAULT '',
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

ALTER TABLE documentos
    ADD CONSTRAINT fk_documentos_firma
    FOREIGN KEY (firma_id) REFERENCES documento_firmas(id)
    ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS indice_expediente (
    expediente_id           uuid PRIMARY KEY REFERENCES expedientes(id) ON DELETE CASCADE,
    hash_indice             text NOT NULL DEFAULT '',
    firma_indice_jsonb      jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_indice_expediente_updated_at
BEFORE UPDATE ON indice_expediente
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS certificados_firma (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sujeto                  text NOT NULL,
    huella_certificado      text NOT NULL UNIQUE,
    autoridad               text NOT NULL DEFAULT 'SAT',
    vigencia_desde          date NOT NULL DEFAULT CURRENT_DATE,
    vigencia_hasta          date NULL,
    vigente                 boolean NOT NULL DEFAULT TRUE,
    archivo_certificado_path text NULL,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_certificados_firma_updated_at
BEFORE UPDATE ON certificados_firma
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS trazabilidad (
    id                      bigserial PRIMARY KEY,
    timestamp               timestamptz NOT NULL DEFAULT NOW(),
    actor                   text NOT NULL DEFAULT '',
    accion                  text NOT NULL,
    objeto                  text NOT NULL DEFAULT '',
    objeto_id               text NOT NULL DEFAULT '',
    resultado               text NOT NULL DEFAULT '',
    detalle_jsonb           jsonb NOT NULL DEFAULT '{}'::jsonb,
    prev_hash               text NOT NULL DEFAULT '',
    hash                    text NOT NULL DEFAULT '',
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trazabilidad_objeto ON trazabilidad (objeto, objeto_id);
CREATE INDEX IF NOT EXISTS idx_trazabilidad_timestamp ON trazabilidad (timestamp);

CREATE TABLE IF NOT EXISTS archivos_manifest (
    expediente_id           uuid PRIMARY KEY REFERENCES expedientes(id) ON DELETE CASCADE,
    ruta_manifest           text NOT NULL,
    hash_manifest           text NOT NULL DEFAULT '',
    peso_bytes              bigint NOT NULL DEFAULT 0 CHECK (peso_bytes >= 0),
    manifest_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    creado_en               timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_archivos_manifest_updated_at
BEFORE UPDATE ON archivos_manifest
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

-- =============================================================================
-- 2. COSTEO, BIM, APU, RIESGO
-- =============================================================================

CREATE TABLE IF NOT EXISTS catalogos_fuente (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre_catalogo         text NOT NULL,
    fuente                  text NOT NULL,
    version                 text NOT NULL DEFAULT '',
    tipo_catalogo           text NOT NULL CHECK (tipo_catalogo IN (
                                'APU', 'LEGAL', 'SECTORIAL', 'INDIRECTOS', 'SALARIOS',
                                'MAQUINARIA', 'REFERENCIA_TECNICA', 'OTRO'
                            )),
    zona_economica          text NULL CHECK (zona_economica IN (
                                'NORTE','CENTRO','SUR','NOROESTE','NORESTE','OCCIDENTE','SURESTE'
                            )),
    vigencia_desde          date NULL,
    vigencia_hasta          date NULL,
    hash_archivo            text NOT NULL DEFAULT '',
    ocr_status              text NOT NULL DEFAULT 'NO_REQUIERE' CHECK (ocr_status IN ('NO_REQUIERE', 'PENDIENTE', 'APLICADO', 'ERROR')),
    storage_uri             text NULL,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_catalogos_fuente_updated_at
BEFORE UPDATE ON catalogos_fuente
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_catalogos_fuente_tipo ON catalogos_fuente (tipo_catalogo);
CREATE INDEX IF NOT EXISTS idx_catalogos_fuente_hash ON catalogos_fuente (hash_archivo);

CREATE TABLE IF NOT EXISTS catalogo_insumos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_fuente_id      uuid NOT NULL REFERENCES catalogos_fuente(id) ON DELETE CASCADE,
    clave                   text NOT NULL,
    descripcion             text NOT NULL,
    unidad                  text NOT NULL,
    precio                  numeric(18,6) NOT NULL DEFAULT 0 CHECK (precio >= 0),
    fuente_catalogo         text NOT NULL DEFAULT '',
    rendimiento             numeric(18,6) NOT NULL DEFAULT 1 CHECK (rendimiento > 0),
    activo                  boolean NOT NULL DEFAULT TRUE,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (catalogo_fuente_id, clave)
);

CREATE TRIGGER trg_catalogo_insumos_updated_at
BEFORE UPDATE ON catalogo_insumos
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_catalogo_insumos_clave ON catalogo_insumos (clave);
CREATE INDEX IF NOT EXISTS idx_catalogo_insumos_catalogo_fuente_id ON catalogo_insumos (catalogo_fuente_id);

CREATE TABLE IF NOT EXISTS apu_conceptos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_fuente_id      uuid NOT NULL REFERENCES catalogos_fuente(id) ON DELETE CASCADE,
    clave                   text NOT NULL,
    descripcion             text NOT NULL,
    unidad                  text NOT NULL,
    activo                  boolean NOT NULL DEFAULT TRUE,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (catalogo_fuente_id, clave)
);

CREATE TRIGGER trg_apu_conceptos_updated_at
BEFORE UPDATE ON apu_conceptos
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS apu_concepto_insumos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    concepto_id             uuid NOT NULL REFERENCES apu_conceptos(id) ON DELETE CASCADE,
    insumo_id               uuid NOT NULL REFERENCES catalogo_insumos(id) ON DELETE RESTRICT,
    componente              text NOT NULL CHECK (componente IN ('material', 'mano_obra', 'equipo', 'otro')),
    cantidad                numeric(18,6) NOT NULL DEFAULT 0 CHECK (cantidad >= 0),
    orden                   integer NOT NULL DEFAULT 0 CHECK (orden >= 0),
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (concepto_id, insumo_id, componente)
);

CREATE TRIGGER trg_apu_concepto_insumos_updated_at
BEFORE UPDATE ON apu_concepto_insumos
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_apu_concepto_insumos_concepto ON apu_concepto_insumos (concepto_id);
CREATE INDEX IF NOT EXISTS idx_apu_concepto_insumos_insumo ON apu_concepto_insumos (insumo_id);

CREATE TABLE IF NOT EXISTS ajustes_zonales (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    zona_economica          text NOT NULL CHECK (zona_economica IN (
                                'NORTE','CENTRO','SUR','NOROESTE','NORESTE','OCCIDENTE','SURESTE'
                            )),
    factor_indirecto        numeric(18,6) NOT NULL DEFAULT 0.15 CHECK (factor_indirecto >= 0),
    factor_utilidad         numeric(18,6) NOT NULL DEFAULT 0.10 CHECK (factor_utilidad >= 0),
    factor_impuesto         numeric(18,6) NOT NULL DEFAULT 0.16 CHECK (factor_impuesto >= 0),
    factor_riesgo           numeric(18,6) NOT NULL DEFAULT 0.02 CHECK (factor_riesgo >= 0),
    vigencia_desde          date NOT NULL DEFAULT CURRENT_DATE,
    vigencia_hasta          date NULL,
    fuente                  text NOT NULL DEFAULT '',
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_ajustes_zonales_updated_at
BEFORE UPDATE ON ajustes_zonales
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_ajustes_zonales_zona_vigencia ON ajustes_zonales (zona_economica, vigencia_desde, vigencia_hasta);

CREATE TABLE IF NOT EXISTS documentos_presupuesto (
    documento_id            uuid PRIMARY KEY REFERENCES documentos(id) ON DELETE CASCADE,
    monto_directo           numeric(18,2) NOT NULL DEFAULT 0 CHECK (monto_directo >= 0),
    monto_indirecto         numeric(18,2) NOT NULL DEFAULT 0 CHECK (monto_indirecto >= 0),
    monto_utilidad          numeric(18,2) NOT NULL DEFAULT 0 CHECK (monto_utilidad >= 0),
    monto_impuesto          numeric(18,2) NOT NULL DEFAULT 0 CHECK (monto_impuesto >= 0),
    monto_total             numeric(18,2) NOT NULL DEFAULT 0 CHECK (monto_total >= 0),
    moneda                  text NOT NULL DEFAULT 'MXN',
    zona_economica          text NOT NULL CHECK (zona_economica IN (
                                'NORTE','CENTRO','SUR','NOROESTE','NORESTE','OCCIDENTE','SURESTE'
                            )),
    factor_indirecto        numeric(18,6) NOT NULL DEFAULT 0.15 CHECK (factor_indirecto >= 0),
    factor_utilidad         numeric(18,6) NOT NULL DEFAULT 0.10 CHECK (factor_utilidad >= 0),
    factor_impuesto         numeric(18,6) NOT NULL DEFAULT 0.16 CHECK (factor_impuesto >= 0),
    numero_partidas         integer NOT NULL DEFAULT 0 CHECK (numero_partidas >= 0),
    resultado_montecarlo_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
    resultado_determinista_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
    insumos_desglose_jsonb  jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_documentos_presupuesto_updated_at
BEFORE UPDATE ON documentos_presupuesto
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS presupuesto_partidas (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    documento_presupuesto_id uuid NOT NULL REFERENCES documentos_presupuesto(documento_id) ON DELETE CASCADE,
    partida_id              text NOT NULL,
    tipo                    text NOT NULL DEFAULT 'generico',
    sistema                 text NOT NULL DEFAULT 'concreto',
    cantidad                numeric(18,6) NOT NULL DEFAULT 0 CHECK (cantidad >= 0),
    unidad                  text NOT NULL,
    costo_directo           numeric(18,2) NOT NULL DEFAULT 0 CHECK (costo_directo >= 0),
    merma_pct               numeric(18,6) NOT NULL DEFAULT 0 CHECK (merma_pct >= 0),
    factor_desperdicio      numeric(18,6) NOT NULL DEFAULT 1 CHECK (factor_desperdicio > 0),
    elemento_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (documento_presupuesto_id, partida_id)
);

CREATE TRIGGER trg_presupuesto_partidas_updated_at
BEFORE UPDATE ON presupuesto_partidas
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_presupuesto_partidas_doc ON presupuesto_partidas (documento_presupuesto_id);

CREATE TABLE IF NOT EXISTS presupuesto_partida_insumos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    partida_id              uuid NOT NULL REFERENCES presupuesto_partidas(id) ON DELETE CASCADE,
    componente              text NOT NULL CHECK (componente IN ('material', 'mano_obra', 'equipo', 'otro')),
    insumo_clave            text NOT NULL DEFAULT '',
    nombre                  text NOT NULL,
    tipo                    text NOT NULL DEFAULT '',
    cantidad                numeric(18,6) NOT NULL DEFAULT 0 CHECK (cantidad >= 0),
    precio_unitario         numeric(18,6) NOT NULL DEFAULT 0 CHECK (precio_unitario >= 0),
    precio_total            numeric(18,2) NOT NULL DEFAULT 0 CHECK (precio_total >= 0),
    fuente_catalogo_id      uuid NULL REFERENCES catalogos_fuente(id) ON DELETE SET NULL,
    raw_jsonb               jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_presupuesto_partida_insumos_updated_at
BEFORE UPDATE ON presupuesto_partida_insumos
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_presupuesto_partida_insumos_partida ON presupuesto_partida_insumos (partida_id);

CREATE TABLE IF NOT EXISTS simulaciones_riesgo (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    documento_presupuesto_id uuid NOT NULL REFERENCES documentos_presupuesto(documento_id) ON DELETE CASCADE,
    costo_base              numeric(18,2) NOT NULL DEFAULT 0 CHECK (costo_base >= 0),
    iteraciones             integer NOT NULL DEFAULT 1000 CHECK (iteraciones > 0),
    media                   numeric(18,2) NOT NULL DEFAULT 0 CHECK (media >= 0),
    desviacion_std          numeric(18,2) NOT NULL DEFAULT 0 CHECK (desviacion_std >= 0),
    cv_pct                  numeric(18,4) NOT NULL DEFAULT 0 CHECK (cv_pct >= 0),
    p5                      numeric(18,2) NOT NULL DEFAULT 0 CHECK (p5 >= 0),
    p25                     numeric(18,2) NOT NULL DEFAULT 0 CHECK (p25 >= 0),
    p50                     numeric(18,2) NOT NULL DEFAULT 0 CHECK (p50 >= 0),
    p75                     numeric(18,2) NOT NULL DEFAULT 0 CHECK (p75 >= 0),
    p95                     numeric(18,2) NOT NULL DEFAULT 0 CHECK (p95 >= 0),
    rango_minimo            numeric(18,2) NOT NULL DEFAULT 0 CHECK (rango_minimo >= 0),
    rango_maximo            numeric(18,2) NOT NULL DEFAULT 0 CHECK (rango_maximo >= 0),
    parametro_hash          text NOT NULL DEFAULT '',
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_simulaciones_riesgo_updated_at
BEFORE UPDATE ON simulaciones_riesgo
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS simulacion_riesgo_parametros (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    simulacion_id           uuid NOT NULL REFERENCES simulaciones_riesgo(id) ON DELETE CASCADE,
    nombre                  text NOT NULL,
    media                   numeric(18,6) NOT NULL DEFAULT 1 CHECK (media > 0),
    desviacion_std          numeric(18,6) NOT NULL DEFAULT 0 CHECK (desviacion_std >= 0),
    minimo                  numeric(18,6) NULL,
    maximo                  numeric(18,6) NULL,
    orden                   integer NOT NULL DEFAULT 0 CHECK (orden >= 0),
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (simulacion_id, nombre)
);

CREATE TRIGGER trg_simulacion_riesgo_parametros_updated_at
BEFORE UPDATE ON simulacion_riesgo_parametros
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS costeo_historial (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    expediente_id           uuid NULL REFERENCES expedientes(id) ON DELETE SET NULL,
    documento_presupuesto_id uuid NULL REFERENCES documentos_presupuesto(documento_id) ON DELETE SET NULL,
    proyecto                text NOT NULL DEFAULT '',
    proyecto_id             text NOT NULL DEFAULT '',
    zona_economica          text NOT NULL CHECK (zona_economica IN (
                                'NORTE','CENTRO','SUR','NOROESTE','NORESTE','OCCIDENTE','SURESTE'
                            )),
    status                  text NOT NULL DEFAULT 'COMPLETADO' CHECK (status IN ('PENDIENTE','EN_PROCESO','COMPLETADO','FALLIDO')),
    payload_jsonb           jsonb NOT NULL DEFAULT '{}'::jsonb,
    resultado_jsonb         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_costeo_historial_expediente ON costeo_historial (expediente_id);
CREATE INDEX IF NOT EXISTS idx_costeo_historial_documento ON costeo_historial (documento_presupuesto_id);
CREATE INDEX IF NOT EXISTS idx_costeo_historial_created_at ON costeo_historial (created_at);

-- Vista de desglose de insumos (equivalente a la salida del motor)
CREATE OR REPLACE VIEW presupuesto_insumos_desglose AS
SELECT
    ppi.id,
    pp.documento_presupuesto_id,
    ppi.partida_id,
    pp.partida_id AS partida_codigo,
    ppi.componente,
    ppi.insumo_clave,
    ppi.nombre,
    ppi.tipo,
    ppi.cantidad,
    ppi.precio_unitario,
    ppi.precio_total,
    ppi.fuente_catalogo_id,
    ppi.raw_jsonb,
    ppi.created_at,
    ppi.updated_at
FROM presupuesto_partida_insumos ppi
JOIN presupuesto_partidas pp ON pp.id = ppi.partida_id;

-- =============================================================================
-- 3. MARCO JURÍDICO, EVIDENCIA, FALLAS Y REGLAS DETERMINISTAS
-- =============================================================================

CREATE TABLE IF NOT EXISTS marcos_juridicos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    jurisdiccion            text NOT NULL CHECK (jurisdiccion IN (
                                'FEDERAL','CDMX','JALISCO','NUEVO_LEON','ESTADO_MEXICO','PUEBLA',
                                'VERACRUZ','MICHOACAN','BAJA_CALIFORNIA','CHIHUAHUA','SONORA',
                                'OAXACA','OTRO'
                            )),
    convocante              text NOT NULL,
    tipo_contrato           text NOT NULL CHECK (tipo_contrato IN (
                                'PRECIOS_UNITARIOS', 'PRECIO_ALZADO', 'MIXTO',
                                'OBRA_PUBLICA', 'SERVICIO_RELACIONADO', 'ARRENDAMIENTO', 'ADQUISICION'
                            )),
    notas                   text NOT NULL DEFAULT '',
    marco_jsonb             jsonb NOT NULL DEFAULT '{}'::jsonb,
    activo                  boolean NOT NULL DEFAULT TRUE,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (jurisdiccion, convocante, tipo_contrato)
);

CREATE TRIGGER trg_marcos_juridicos_updated_at
BEFORE UPDATE ON marcos_juridicos
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS marco_juridico_leyes (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    marco_juridico_id       uuid NOT NULL REFERENCES marcos_juridicos(id) ON DELETE CASCADE,
    ley                     text NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (marco_juridico_id, ley)
);

CREATE TABLE IF NOT EXISTS marco_juridico_reglamentos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    marco_juridico_id       uuid NOT NULL REFERENCES marcos_juridicos(id) ON DELETE CASCADE,
    reglamento              text NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (marco_juridico_id, reglamento)
);

CREATE TABLE IF NOT EXISTS marco_juridico_lineamientos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    marco_juridico_id       uuid NOT NULL REFERENCES marcos_juridicos(id) ON DELETE CASCADE,
    lineamiento             text NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (marco_juridico_id, lineamiento)
);

CREATE TABLE IF NOT EXISTS marco_juridico_umbrales (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    marco_juridico_id       uuid NOT NULL REFERENCES marcos_juridicos(id) ON DELETE CASCADE,
    tipo_umbral             text NOT NULL CHECK (tipo_umbral IN ('adjudicacion_directa', 'invitacion_tres', 'licitacion_publica')),
    monto                   numeric(18,2) NOT NULL DEFAULT 0 CHECK (monto >= 0),
    moneda                  text NOT NULL DEFAULT 'MXN',
    orden                   integer NOT NULL DEFAULT 0 CHECK (orden >= 0),
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (marco_juridico_id, tipo_umbral)
);

CREATE TABLE IF NOT EXISTS reglas_validacion (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    descripcion             text NOT NULL,
    norma                   text NOT NULL,
    campo                   text NOT NULL,
    activa                  boolean NOT NULL DEFAULT TRUE,
    condicion_tipo          text NOT NULL DEFAULT 'FUNCION' CHECK (condicion_tipo IN ('FUNCION', 'JSONPATH', 'SQL', 'REGEX', 'RANGO', 'LISTA')),
    condicion_jsonb         jsonb NOT NULL DEFAULT '{}'::jsonb,
    severidad_default       text NOT NULL DEFAULT 'ERROR' CHECK (severidad_default IN ('DEBUG','INFO','AVISO','ERROR','FATAL')),
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_reglas_validacion_updated_at
BEFORE UPDATE ON reglas_validacion
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS causales_fallo (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tipo                    text NOT NULL CHECK (tipo IN (
                                'REQUISITO_FALTANTE', 'INCONSISTENCIA_TECNICA', 'INCUMPLIMIENTO_DOCUMENTAL',
                                'PRECIO_FUERA_RANGO', 'ERROR_FIRMA', 'FORMATO_INVALIDO', 'VIGENCIA_VENCIDA',
                                'CAPACIDAD_INSUFICIENTE', 'INCUMPLIMIENTO_NORMATIVO', 'ERROR_CALCULO'
                            )),
    descripcion             text NOT NULL,
    campo_afectado          text NOT NULL DEFAULT '',
    valor_obtenido_jsonb    jsonb NULL,
    valor_esperado_jsonb    jsonb NULL,
    norma_referencia        text NOT NULL DEFAULT '',
    dependencia             text NOT NULL DEFAULT '',
    estado                  text NOT NULL DEFAULT '',
    tipo_obra               text NOT NULL DEFAULT '',
    subsanable              boolean NOT NULL DEFAULT TRUE,
    evidencia_id            uuid NULL,
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS evidencias (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tipo                    text NOT NULL CHECK (tipo IN (
                                'PAGINA_BASES', 'PARRAFO', 'PLANO', 'CATALOGO', 'ARCHIVO',
                                'CALCULO', 'METRADO', 'FICHA_TECNICA', 'CAPTURA_BIM', 'NORMA'
                            )),
    descripcion             text NOT NULL DEFAULT '',
    referencia              text NOT NULL DEFAULT '',
    contenido_hash          text NOT NULL DEFAULT '',
    nombre_archivo          text NULL,
    url_o_ruta              text NULL,
    regla_id                uuid NULL REFERENCES reglas_validacion(id) ON DELETE SET NULL,
    causal_id               uuid NULL REFERENCES causales_fallo(id) ON DELETE SET NULL,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

ALTER TABLE causales_fallo
    ADD CONSTRAINT fk_causales_fallo_evidencia
    FOREIGN KEY (evidencia_id) REFERENCES evidencias(id)
    ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_causales_fallo_tipo ON causales_fallo (tipo);
CREATE INDEX IF NOT EXISTS idx_evidencias_regla_id ON evidencias (regla_id);
CREATE INDEX IF NOT EXISTS idx_evidencias_causal_id ON evidencias (causal_id);

CREATE TABLE IF NOT EXISTS reglas_deterministas (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    codigo                  text NOT NULL UNIQUE,
    seccion                 text NOT NULL,
    descripcion             text NOT NULL,
    es_critica              boolean NOT NULL DEFAULT TRUE,
    orden                   integer NOT NULL DEFAULT 0 CHECK (orden >= 0),
    activa                  boolean NOT NULL DEFAULT TRUE,
    norma_referencia        text NOT NULL DEFAULT '',
    regla_jsonb             jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_reglas_deterministas_updated_at
BEFORE UPDATE ON reglas_deterministas
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE TABLE IF NOT EXISTS evaluaciones_deterministas (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    expediente_id           uuid NULL REFERENCES expedientes(id) ON DELETE SET NULL,
    rfc_empresa             text NOT NULL DEFAULT '',
    estatus_final           text NOT NULL CHECK (estatus_final IN ('SOLVENTE', 'DESCALIFICADO', 'PENDIENTE')),
    payload_hash            text NOT NULL DEFAULT '',
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS evaluacion_determinista_bitacora (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    evaluacion_id           uuid NOT NULL REFERENCES evaluaciones_deterministas(id) ON DELETE CASCADE,
    id_regla                text NOT NULL,
    seccion                 text NOT NULL,
    estatus                 text NOT NULL CHECK (estatus IN ('PASA', 'FALLA', 'NO_APLICA')),
    valor_detectado         text NOT NULL DEFAULT '',
    valor_esperado          text NOT NULL DEFAULT '',
    evidencia               text NOT NULL DEFAULT '',
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_evaluacion_bitacora_eval ON evaluacion_determinista_bitacora (evaluacion_id);
CREATE INDEX IF NOT EXISTS idx_evaluaciones_deterministas_expediente ON evaluaciones_deterministas (expediente_id);

-- =============================================================================
-- 4. TOPOGRAFÍA (PostGIS)
-- =============================================================================

CREATE TABLE IF NOT EXISTS topo_levantamientos (
    id                      text PRIMARY KEY,
    nombre                  text NOT NULL,
    crs                     text DEFAULT 'LOCAL',
    srid                    integer DEFAULT 0,
    metadatos               jsonb DEFAULT '{}'::jsonb,
    created_at              timestamptz DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS topo_puntos (
    id                      text PRIMARY KEY,
    levantamiento_id        text NOT NULL REFERENCES topo_levantamientos(id) ON DELETE CASCADE,
    x                       numeric,
    y                       numeric,
    z                       numeric,
    precision_xy            numeric DEFAULT 0.02,
    precision_z             numeric DEFAULT 0.05,
    etiqueta                text,
    descripcion             text,
    fuente                  text,
    metadatos               jsonb DEFAULT '{}'::jsonb,
    srid                    integer DEFAULT 0,
    geom                    geometry(POINTZ, 0) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(x, y, z), srid)) STORED,
    created_at              timestamptz DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_topo_puntos_geom ON topo_puntos USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_topo_puntos_lid ON topo_puntos (levantamiento_id);

-- =============================================================================
-- 5. JOBS ASÍNCRONOS
-- =============================================================================

CREATE TABLE IF NOT EXISTS jobs_background (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tipo                    text NOT NULL,
    payload_jsonb           jsonb NOT NULL DEFAULT '{}'::jsonb,
    callback_name           text NOT NULL DEFAULT '',
    creado_en               timestamptz NOT NULL DEFAULT NOW(),
    started_at              timestamptz NULL,
    finished_at             timestamptz NULL,
    resultado_jsonb         jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_text              text NOT NULL DEFAULT '',
    estado                  text NOT NULL DEFAULT 'PENDIENTE' CHECK (estado IN ('PENDIENTE', 'EN_PROCESO', 'COMPLETADO', 'FALLIDO')),
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_jobs_background_estado ON jobs_background (estado);
CREATE INDEX IF NOT EXISTS idx_jobs_background_creado_en ON jobs_background (creado_en);

-- =============================================================================
-- 6. ÍNDICES ADICIONALES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_documento_metadatos_documento_id ON documento_metadatos (documento_id);
CREATE INDEX IF NOT EXISTS idx_documento_firmas_documento_id ON documento_firmas (documento_id);
CREATE INDEX IF NOT EXISTS idx_catalogo_insumos_activo ON catalogo_insumos (activo);
CREATE INDEX IF NOT EXISTS idx_apu_conceptos_activo ON apu_conceptos (activo);
CREATE INDEX IF NOT EXISTS idx_reglas_validacion_activa ON reglas_validacion (activa);
CREATE INDEX IF NOT EXISTS idx_reglas_deterministas_activa ON reglas_deterministas (activa);
CREATE INDEX IF NOT EXISTS idx_presupuesto_partidas_doc ON presupuesto_partidas (documento_presupuesto_id);
CREATE INDEX IF NOT EXISTS idx_presupuesto_partida_insumos_partida ON presupuesto_partida_insumos (partida_id);
CREATE INDEX IF NOT EXISTS idx_evaluacion_bitacora_eval ON evaluacion_determinista_bitacora (evaluacion_id);
CREATE INDEX IF NOT EXISTS idx_evaluaciones_deterministas_expediente ON evaluaciones_deterministas (expediente_id);

-- =============================================================================
-- COMENTARIOS DESCRIPTIVOS (opcional)
-- =============================================================================

COMMENT ON TABLE expedientes IS 'Expedientes electrónicos, núcleo documental';
COMMENT ON TABLE documentos IS 'Documentos asociados a expedientes';
COMMENT ON TABLE trazabilidad IS 'Bitácora inmutable con cadena de Merkle para auditoría';
COMMENT ON TABLE documentos_presupuesto IS 'Datos específicos de presupuestos, derivados de documentos';
COMMENT ON TABLE presupuesto_partidas IS 'Partidas de un presupuesto';
COMMENT ON TABLE presupuesto_partida_insumos IS 'Insumos (materiales, mano de obra, equipo) de cada partida';
COMMENT ON TABLE simulaciones_riesgo IS 'Resultados de Monte Carlo para análisis de riesgos';
COMMENT ON TABLE reglas_deterministas IS 'Reglas de validación para el motor determinista de licitaciones';
COMMENT ON TABLE evaluaciones_deterministas IS 'Resultados de validación de propuestas';
COMMENT ON TABLE topo_levantamientos IS 'Levantamientos topográficos (PostGIS)';
COMMENT ON TABLE jobs_background IS 'Cola de trabajos asíncronos (background jobs)';


-- =============================================================================
-- MEGALODON COSTOS v3.3 -> AMPLIACIÓN v4.0
-- Módulo: Marco jurídico estructurado, base de conocimiento técnico,
--         catálogos genéricos y trazabilidad del pipeline ETL.
-- No modifica ni renombra ninguna tabla/columna de la sección 1 (v3.3):
-- el backend megalodon_costos_v3_3.py sigue funcionando sin cambios.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 7. MARCO LEGAL ESTRUCTURADO (LOPSRM, LAASSP, LGA, LGPDPPSO, reglamentos, etc.)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS leyes (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    clave                   text NOT NULL UNIQUE,              -- ej. 'LOPSRM', 'LAASSP', 'LGA'
    nombre_completo         text NOT NULL,
    jurisdiccion            text NOT NULL DEFAULT 'FEDERAL',   -- FEDERAL, CDMX, ESTADO_MEXICO, TLAXCALA, etc.
    tipo_norma              text NOT NULL DEFAULT 'LEY' CHECK (tipo_norma IN (
                                'LEY','REGLAMENTO','LINEAMIENTO','NOM','CRITERIO','DECRETO','OTRO'
                            )),
    ultima_reforma_dof      date NULL,
    fecha_publicacion       date NULL,
    archivo_origen          text NOT NULL,
    hash_archivo            text NOT NULL DEFAULT '',
    texto_completo          text NOT NULL DEFAULT '',
    total_articulos         integer NOT NULL DEFAULT 0,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    tsv                     tsvector GENERATED ALWAYS AS (
                                to_tsvector('spanish', coalesce(nombre_completo,'') || ' ' || coalesce(texto_completo,''))
                            ) STORED,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_leyes_updated_at
BEFORE UPDATE ON leyes
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_leyes_tsv ON leyes USING GIN (tsv);
CREATE INDEX IF NOT EXISTS idx_leyes_jurisdiccion ON leyes (jurisdiccion);

CREATE TABLE IF NOT EXISTS ley_articulos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ley_id                  uuid NOT NULL REFERENCES leyes(id) ON DELETE CASCADE,
    numero_articulo         text NOT NULL,          -- '1', '45 Bis', 'Transitorio Cuarto'
    titulo                  text NOT NULL DEFAULT '',
    capitulo                text NOT NULL DEFAULT '',
    titulo_seccion          text NOT NULL DEFAULT '',
    texto                   text NOT NULL,
    orden                   integer NOT NULL DEFAULT 0,
    es_transitorio          boolean NOT NULL DEFAULT FALSE,
    reformas_dof            text[] NOT NULL DEFAULT '{}',
    tsv                     tsvector GENERATED ALWAYS AS (to_tsvector('spanish', coalesce(texto,''))) STORED,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (ley_id, numero_articulo, orden)
);

CREATE INDEX IF NOT EXISTS idx_ley_articulos_ley ON ley_articulos (ley_id);
CREATE INDEX IF NOT EXISTS idx_ley_articulos_tsv ON ley_articulos USING GIN (tsv);
CREATE INDEX IF NOT EXISTS idx_ley_articulos_numero ON ley_articulos (numero_articulo);

-- Vincula reglas de validación / deterministas a artículos concretos (trazabilidad jurídica)
CREATE TABLE IF NOT EXISTS regla_articulo_referencia (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    regla_id                uuid NULL REFERENCES reglas_validacion(id) ON DELETE CASCADE,
    regla_determinista_id   uuid NULL REFERENCES reglas_deterministas(id) ON DELETE CASCADE,
    articulo_id             uuid NOT NULL REFERENCES ley_articulos(id) ON DELETE CASCADE,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    CHECK (regla_id IS NOT NULL OR regla_determinista_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_regla_articulo_articulo ON regla_articulo_referencia (articulo_id);

-- -----------------------------------------------------------------------------
-- 8. BASE DE CONOCIMIENTO TÉCNICO (libros, manuales, informes, PDFs no tabulares)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS documentos_referencia (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    titulo                  text NOT NULL,
    categoria               text NOT NULL CHECK (categoria IN (
                                'GEOTECNIA','TOPOGRAFIA','ESTRUCTURAS','COSTOS','NORMATIVA',
                                'LICITACIONES','FORMULARIOS','FISCAL','SECTORIAL','MATEMATICAS','OTRO'
                            )),
    archivo_origen          text NOT NULL,
    formato_origen          text NOT NULL CHECK (formato_origen IN ('pdf','docx','doc','md','txt','xls','xlsx')),
    hash_archivo            text NOT NULL DEFAULT '',
    paginas                 integer NULL,
    texto_extraido          text NOT NULL DEFAULT '',
    resumen                 text NOT NULL DEFAULT '',
    calidad_extraccion      text NOT NULL DEFAULT 'ALTA' CHECK (calidad_extraccion IN ('ALTA','MEDIA','BAJA','OCR_DEGRADADO')),
    catalogo_fuente_id      uuid NULL REFERENCES catalogos_fuente(id) ON DELETE SET NULL,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    tsv                     tsvector GENERATED ALWAYS AS (
                                to_tsvector('spanish', coalesce(titulo,'') || ' ' || coalesce(texto_extraido,''))
                            ) STORED,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (hash_archivo)
);

CREATE TRIGGER trg_documentos_referencia_updated_at
BEFORE UPDATE ON documentos_referencia
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_documentos_referencia_tsv ON documentos_referencia USING GIN (tsv);
CREATE INDEX IF NOT EXISTS idx_documentos_referencia_categoria ON documentos_referencia (categoria);

-- Tablas/párrafos relevantes extraídos de documentos de referencia (granularidad fina para RAG/búsqueda)
CREATE TABLE IF NOT EXISTS documento_referencia_fragmentos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    documento_id            uuid NOT NULL REFERENCES documentos_referencia(id) ON DELETE CASCADE,
    pagina                  integer NULL,
    orden                   integer NOT NULL DEFAULT 0,
    contenido               text NOT NULL,
    tipo_fragmento          text NOT NULL DEFAULT 'PARRAFO' CHECK (tipo_fragmento IN ('PARRAFO','TABLA','TITULO','NOTA')),
    tsv                     tsvector GENERATED ALWAYS AS (to_tsvector('spanish', coalesce(contenido,''))) STORED,
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_doc_ref_fragmentos_doc ON documento_referencia_fragmentos (documento_id);
CREATE INDEX IF NOT EXISTS idx_doc_ref_fragmentos_tsv ON documento_referencia_fragmentos USING GIN (tsv);

-- -----------------------------------------------------------------------------
-- 9. CATÁLOGOS GENÉRICOS CLAVE/DESCRIPCIÓN (CFDI, Carta Porte, NOM, PACs, etc.)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS catalogos_referencia_generica (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_nombre         text NOT NULL,            -- 'c_SubTipoRem', 'PACs_Autorizados_2026', 'NOM_012_SCT'
    clave                   text NOT NULL,
    descripcion             text NOT NULL DEFAULT '',
    valor_adicional         text NOT NULL DEFAULT '', -- vigencia, monto, unidad, lo que aplique
    archivo_origen          text NOT NULL DEFAULT '',
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (catalogo_nombre, clave)
);

CREATE TRIGGER trg_catalogos_referencia_generica_updated_at
BEFORE UPDATE ON catalogos_referencia_generica
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_catref_generica_nombre ON catalogos_referencia_generica (catalogo_nombre);
CREATE INDEX IF NOT EXISTS idx_catref_generica_clave ON catalogos_referencia_generica (clave);

-- -----------------------------------------------------------------------------
-- 10. COSTOS PARAMÉTRICOS POR M2 Y PESOS DE MATERIALES (tablas de ingeniería)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS costos_parametricos_m2 (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tipo_obra               text NOT NULL,
    zona                    text NOT NULL DEFAULT 'NACIONAL',
    costo_min_mxn_m2        numeric(18,2) NOT NULL DEFAULT 0 CHECK (costo_min_mxn_m2 >= 0),
    costo_max_mxn_m2        numeric(18,2) NOT NULL DEFAULT 0 CHECK (costo_max_mxn_m2 >= 0),
    caracteristicas         text NOT NULL DEFAULT '',
    tiempo_estimado         text NOT NULL DEFAULT '',
    vigencia                text NOT NULL DEFAULT '',
    documento_referencia_id uuid NULL REFERENCES documentos_referencia(id) ON DELETE SET NULL,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_costos_parametricos_tipo ON costos_parametricos_m2 (tipo_obra);
CREATE INDEX IF NOT EXISTS idx_costos_parametricos_zona ON costos_parametricos_m2 (zona);

CREATE TABLE IF NOT EXISTS materiales_pesos_referencia (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    categoria               text NOT NULL,           -- 'Angulo','PTR','Varilla Corrugada','IPR', etc.
    producto                text NOT NULL,
    especificacion_jsonb    jsonb NOT NULL DEFAULT '{}'::jsonb,  -- dimensiones, calibre, diámetro, etc.
    peso_kg_pza             numeric(18,4) NULL,
    peso_kg_ml              numeric(18,4) NULL,
    unidad_referencia       text NOT NULL DEFAULT 'kg',
    archivo_origen          text NOT NULL DEFAULT '',
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (categoria, producto)
);

CREATE INDEX IF NOT EXISTS idx_materiales_pesos_categoria ON materiales_pesos_referencia (categoria);

-- -----------------------------------------------------------------------------
-- 11. TRAZABILIDAD Y CONTROL DEL PIPELINE ETL (idempotencia, auditoría, errores)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_archivos_procesados (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ruta_relativa           text NOT NULL,
    nombre_archivo          text NOT NULL,
    extension               text NOT NULL,
    hash_sha256             text NOT NULL,
    tamano_bytes            bigint NOT NULL DEFAULT 0,
    tipo_detectado          text NOT NULL,    -- LEY_TXT, CATALOGO_XLSX_LIMPIO, CATALOGO_OCR_TXT, REFERENCIA_PDF, ...
    estrategia_aplicada     text NOT NULL DEFAULT '',
    estado                  text NOT NULL DEFAULT 'PENDIENTE' CHECK (estado IN ('PENDIENTE','PROCESADO','ERROR','OMITIDO','PARCIAL')),
    registros_generados     integer NOT NULL DEFAULT 0,
    confianza_promedio      text NOT NULL DEFAULT '' CHECK (confianza_promedio IN ('','ALTA','MEDIA','BAJA')),
    error_text              text NOT NULL DEFAULT '',
    duracion_ms             integer NOT NULL DEFAULT 0,
    procesado_en            timestamptz NOT NULL DEFAULT NOW(),
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (hash_sha256)
);

CREATE INDEX IF NOT EXISTS idx_etl_archivos_estado ON etl_archivos_procesados (estado);
CREATE INDEX IF NOT EXISTS idx_etl_archivos_tipo ON etl_archivos_procesados (tipo_detectado);

CREATE TABLE IF NOT EXISTS etl_runs (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    iniciado_en             timestamptz NOT NULL DEFAULT NOW(),
    finalizado_en           timestamptz NULL,
    total_archivos          integer NOT NULL DEFAULT 0,
    procesados_ok           integer NOT NULL DEFAULT 0,
    procesados_error        integer NOT NULL DEFAULT 0,
    omitidos                integer NOT NULL DEFAULT 0,
    registros_totales        integer NOT NULL DEFAULT 0,
    parametros_jsonb        jsonb NOT NULL DEFAULT '{}'::jsonb,
    resumen_jsonb           jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- -----------------------------------------------------------------------------
-- 12. AMPLIACIÓN A catalogo_insumos PARA SOPORTAR EXTRACCIÓN OCR DE BAJA CONFIANZA
--     (no se renombra ni se elimina ninguna columna existente del backend)
-- -----------------------------------------------------------------------------

ALTER TABLE costos_parametricos_m2 ADD COLUMN IF NOT EXISTS catalogo_fuente_id uuid NULL
    REFERENCES catalogos_fuente(id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_costos_parametricos_m2_catalogo_fuente ON costos_parametricos_m2 (catalogo_fuente_id);

ALTER TABLE catalogo_insumos ADD COLUMN IF NOT EXISTS zona text NOT NULL DEFAULT '';
ALTER TABLE catalogo_insumos ADD COLUMN IF NOT EXISTS tipo_recurso text NOT NULL DEFAULT 'material'
    CHECK (tipo_recurso IN ('material','mano_obra','equipo','otro'));
CREATE INDEX IF NOT EXISTS idx_catalogo_insumos_zona ON catalogo_insumos (zona);

ALTER TABLE catalogo_insumos ADD COLUMN IF NOT EXISTS confianza_extraccion text NOT NULL DEFAULT 'ALTA'
    CHECK (confianza_extraccion IN ('ALTA','MEDIA','BAJA'));
ALTER TABLE catalogo_insumos ADD COLUMN IF NOT EXISTS pagina_origen integer NULL;
ALTER TABLE catalogo_insumos ADD COLUMN IF NOT EXISTS seccion text NOT NULL DEFAULT '';

ALTER TABLE catalogos_fuente ADD COLUMN IF NOT EXISTS archivo_origen text NOT NULL DEFAULT '';
ALTER TABLE catalogos_fuente ADD COLUMN IF NOT EXISTS hash_sha256 text NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_catalogos_fuente_hash_archivo_uq
    ON catalogos_fuente (hash_archivo) WHERE hash_archivo <> '';

CREATE INDEX IF NOT EXISTS idx_catalogo_insumos_descripcion_trgm ON catalogo_insumos USING GIN (descripcion gin_trgm_ops);

-- -----------------------------------------------------------------------------
-- 13. VISTAS DE APOYO PARA EL ETL / BÚSQUEDA UNIFICADA
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_busqueda_unificada AS
SELECT 'LEY_ARTICULO'::text AS origen, la.id, l.clave || ' Art. ' || la.numero_articulo AS titulo,
       la.texto AS contenido, la.tsv
FROM ley_articulos la JOIN leyes l ON l.id = la.ley_id
UNION ALL
SELECT 'CATALOGO_INSUMO'::text AS origen, ci.id, ci.clave || ' - ' || ci.descripcion AS titulo,
       ci.descripcion AS contenido, to_tsvector('spanish', ci.descripcion)
FROM catalogo_insumos ci
UNION ALL
SELECT 'DOCUMENTO_REFERENCIA'::text AS origen, dr.id, dr.titulo,
       left(dr.texto_extraido, 5000) AS contenido, dr.tsv
FROM documentos_referencia dr;

COMMENT ON TABLE leyes IS 'Leyes y reglamentos completos con texto íntegro indexado (FTS español)';
COMMENT ON TABLE ley_articulos IS 'Artículos individuales extraídos de cada ley, con su capítulo/título y número';
COMMENT ON TABLE documentos_referencia IS 'Base de conocimiento técnico: libros, manuales, informes (PDF/DOCX/MD)';
COMMENT ON TABLE catalogos_referencia_generica IS 'Catálogos clave/descripción genéricos (CFDI, NOM, PACs, etc.)';
COMMENT ON TABLE costos_parametricos_m2 IS 'Referencias de costo por m2 extraídas de reportes/documentos paramétricos';
COMMENT ON TABLE materiales_pesos_referencia IS 'Tablas de pesos de perfiles y materiales de construcción';
COMMENT ON TABLE etl_archivos_procesados IS 'Bitácora de idempotencia del ETL: 1 fila por archivo fuente (hash único)';
COMMENT ON TABLE etl_runs IS 'Bitácora de corridas completas del pipeline ETL';
-- =============================================================================
-- MEGALODON COSTOS — AMPLIACIÓN v5.0
-- Motor de costos real (alimenta MotorPreciosBIM / AnalisisPreciosUnitarios /
-- MotorDeterministaLicitaciones en piedra_angular_megalodon.py) + soporte de
-- jurisdicción estado/municipio para el motor jurídico de 12 reglas.
--
-- Sigue la misma filosofía que v4.0: NO renombra ni elimina nada de v3.3/v4.0.
-- Las tablas aquí mirroran los conceptos del ETL de referencia
-- (megalodon_etl_cmic_2_.py, esquema costos.*) pero viven dentro del esquema
-- `megalodon` real que exige RepositorioPostgres._validate_connection().
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 14.1 Ampliación de catalogos_fuente: sector + año de vigencia (para que el
--      frontend pueda filtrar "CMIC Vivienda 2026" / "CFE 2025" / etc.)
-- -----------------------------------------------------------------------------

ALTER TABLE catalogos_fuente DROP CONSTRAINT IF EXISTS catalogos_fuente_tipo_catalogo_check;
ALTER TABLE catalogos_fuente ADD CONSTRAINT catalogos_fuente_tipo_catalogo_check
    CHECK (tipo_catalogo IN (
        'APU','PRECIOS_UNITARIOS','PARAMETRICO','MATERIALES','MAQUINARIA',
        'SALARIOS','INDICES','INDIRECTOS','LEGAL','SECTORIAL','REFERENCIA_TECNICA','OTRO'
    ));

ALTER TABLE catalogos_fuente ADD COLUMN IF NOT EXISTS categoria_sector text NOT NULL DEFAULT '';
    -- VIVIENDA, CARRETERAS, SALUD, EDUCATIVA, CIMENTACIONES, CIMENTACIONES_PROFUNDAS,
    -- POZOS, MAQUINARIA_GENERAL, ECOTECNOLOGIAS, MATERIALES_RECICLADOS, HIDRAULICA,
    -- ELECTRICO, ALBANILERIA, CARPINTERIA, TERRACERIAS, ALCANTARILLADO, GENERAL, ...
ALTER TABLE catalogos_fuente ADD COLUMN IF NOT EXISTS codigo_fuente text NOT NULL DEFAULT '';
    -- código corto único por publicación, ej. 'CMIC-VIVIENDA-2026', 'SICT-TCD-CARRETERAS-2026'
ALTER TABLE catalogos_fuente ADD COLUMN IF NOT EXISTS anio_vigencia integer
    GENERATED ALWAYS AS (CASE WHEN vigencia_desde IS NOT NULL THEN EXTRACT(YEAR FROM vigencia_desde)::integer END) STORED;
ALTER TABLE catalogos_fuente ADD COLUMN IF NOT EXISTS es_historico boolean NOT NULL DEFAULT FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_catalogos_fuente_codigo_uq
    ON catalogos_fuente (codigo_fuente) WHERE codigo_fuente <> '';
CREATE INDEX IF NOT EXISTS idx_catalogos_fuente_categoria_sector ON catalogos_fuente (categoria_sector);
CREATE INDEX IF NOT EXISTS idx_catalogos_fuente_anio_vigencia ON catalogos_fuente (anio_vigencia);
CREATE INDEX IF NOT EXISTS idx_catalogos_fuente_fuente ON catalogos_fuente (fuente);

-- Vista lista para el selector del frontend: "2026 | 2025 | 2024 -> Block -> [catálogos]"
CREATE OR REPLACE VIEW v_explorador_catalogos AS
SELECT cf.id AS catalogo_fuente_id, cf.anio_vigencia, cf.fuente AS organismo,
       cf.categoria_sector, cf.nombre_catalogo, cf.tipo_catalogo, cf.zona_economica,
       cf.es_historico, cf.archivo_origen,
       COUNT(DISTINCT ci.id) AS total_insumos, COUNT(DISTINCT ac.id) AS total_conceptos_apu
FROM catalogos_fuente cf
LEFT JOIN catalogo_insumos ci ON ci.catalogo_fuente_id = cf.id
LEFT JOIN apu_conceptos ac ON ac.catalogo_fuente_id = cf.id
GROUP BY cf.id;

-- -----------------------------------------------------------------------------
-- 14.2 Maquinaria (costo horario) — alimenta MotorPreciosBIM / AnalisisPreciosUnitarios
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS maquinaria_catalogo (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_fuente_id      uuid NOT NULL REFERENCES catalogos_fuente(id) ON DELETE CASCADE,
    clave                   text NOT NULL,
    descripcion             text NOT NULL,
    tipo                    text NOT NULL DEFAULT '',
    marca                   text NOT NULL DEFAULT '',
    modelo                  text NOT NULL DEFAULT '',
    hp                      numeric(10,2) NULL,
    capacidad               text NOT NULL DEFAULT '',
    unidad                  text NOT NULL DEFAULT 'hora',
    costo_horario           numeric(18,4) NOT NULL DEFAULT 0 CHECK (costo_horario >= 0),
    activo                  boolean NOT NULL DEFAULT TRUE,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (catalogo_fuente_id, clave)
);
CREATE TRIGGER trg_maquinaria_catalogo_updated_at
BEFORE UPDATE ON maquinaria_catalogo
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();
CREATE INDEX IF NOT EXISTS idx_maquinaria_catalogo_tipo ON maquinaria_catalogo (tipo);
CREATE INDEX IF NOT EXISTS idx_maquinaria_catalogo_descripcion_trgm ON maquinaria_catalogo USING GIN (descripcion gin_trgm_ops);

-- -----------------------------------------------------------------------------
-- 14.3 Salarios de mano de obra y personal profesional
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mano_obra_salarios (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_fuente_id      uuid NOT NULL REFERENCES catalogos_fuente(id) ON DELETE CASCADE,
    categoria               text NOT NULL,
    nivel                   text NOT NULL DEFAULT '',
    tipo_personal           text NOT NULL DEFAULT 'EJECUCION' CHECK (tipo_personal IN (
                                'EJECUCION','ESTUDIOS','SUPERVISION','PROFESIONALES'
                            )),
    unidad                  text NOT NULL DEFAULT 'jornal',
    salario_base_diario     numeric(18,4) NOT NULL DEFAULT 0 CHECK (salario_base_diario >= 0),
    factor_imss             numeric(10,6) NULL,
    factor_imss_total       numeric(10,6) NULL,
    salario_real_diario     numeric(18,4) NULL,
    activo                  boolean NOT NULL DEFAULT TRUE,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (catalogo_fuente_id, categoria, tipo_personal)
);
CREATE TRIGGER trg_mano_obra_salarios_updated_at
BEFORE UPDATE ON mano_obra_salarios
FOR EACH ROW EXECUTE FUNCTION megalodon.set_updated_at();
CREATE INDEX IF NOT EXISTS idx_mano_obra_salarios_tipo ON mano_obra_salarios (tipo_personal);

-- -----------------------------------------------------------------------------
-- 14.4 Índices de variación de precios (CEICO) y factores de indirecto
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS indices_precios_materiales (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_fuente_id      uuid NOT NULL REFERENCES catalogos_fuente(id) ON DELETE CASCADE,
    insumo                  text NOT NULL,
    familia                 text NOT NULL DEFAULT '',
    variacion_pct           numeric(10,4) NOT NULL,
    periodo_inicio          date NOT NULL,
    periodo_fin             date NOT NULL,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (catalogo_fuente_id, insumo, periodo_inicio)
);
CREATE INDEX IF NOT EXISTS idx_indices_precios_familia ON indices_precios_materiales (familia);
CREATE INDEX IF NOT EXISTS idx_indices_precios_periodo ON indices_precios_materiales (periodo_inicio, periodo_fin);

CREATE TABLE IF NOT EXISTS factores_indirectos (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_fuente_id      uuid NOT NULL REFERENCES catalogos_fuente(id) ON DELETE CASCADE,
    convocante              text NOT NULL,
    tipo_obra               text NOT NULL DEFAULT 'GENERAL',
    factor_indirecto        numeric(10,6) NOT NULL CHECK (factor_indirecto >= 0),
    factor_utilidad         numeric(10,6) NULL,
    mes_anio                date NOT NULL,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (convocante, tipo_obra, mes_anio, catalogo_fuente_id)
);
CREATE INDEX IF NOT EXISTS idx_factores_indirectos_convocante ON factores_indirectos (convocante, mes_anio);

-- -----------------------------------------------------------------------------
-- 14.5 Costos paramétricos genéricos (km, aula, cama, m³…) y rendimientos
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS costos_parametricos_obra (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_fuente_id      uuid NOT NULL REFERENCES catalogos_fuente(id) ON DELETE CASCADE,
    tipo_obra               text NOT NULL,
    descripcion             text NOT NULL,
    unidad_parametro        text NOT NULL,          -- km, m², m³, aula, cama, ml, ha, lt, kg...
    costo                   numeric(18,4) NOT NULL DEFAULT 0,
    costo_min               numeric(18,4) NULL,
    costo_max               numeric(18,4) NULL,
    clasificacion           text NOT NULL DEFAULT '',
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_costos_parametricos_obra_tipo ON costos_parametricos_obra (tipo_obra);
CREATE INDEX IF NOT EXISTS idx_costos_parametricos_obra_unidad ON costos_parametricos_obra (unidad_parametro);

CREATE TABLE IF NOT EXISTS rendimientos_mano_obra (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    catalogo_fuente_id      uuid NOT NULL REFERENCES catalogos_fuente(id) ON DELETE CASCADE,
    actividad               text NOT NULL,
    tipo_recurso            text NOT NULL DEFAULT 'MANO_OBRA' CHECK (tipo_recurso IN ('MANO_OBRA','MAQUINARIA')),
    unidad_trabajo          text NOT NULL,
    rendimiento_minimo      numeric(18,4) NOT NULL,
    rendimiento_medio       numeric(18,4) NULL,
    rendimiento_maximo      numeric(18,4) NULL,
    metadata_jsonb          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rendimientos_actividad_trgm ON rendimientos_mano_obra USING GIN (actividad gin_trgm_ops);

-- -----------------------------------------------------------------------------
-- 14.6 Jurisdicción estado/municipio en `leyes` — para el motor jurídico de
--      12 reglas: leyes federales SIEMPRE aplican + ley estatal/municipal de
--      contratación pública según la ubicación de la licitación.
-- -----------------------------------------------------------------------------

ALTER TABLE leyes ADD COLUMN IF NOT EXISTS ambito text NOT NULL DEFAULT 'FEDERAL'
    CHECK (ambito IN ('FEDERAL','ESTATAL','MUNICIPAL'));
ALTER TABLE leyes ADD COLUMN IF NOT EXISTS estado text NULL;       -- ej. 'JALISCO', 'TLAXCALA' (NULL si es FEDERAL)
ALTER TABLE leyes ADD COLUMN IF NOT EXISTS municipio text NULL;    -- NULL si es FEDERAL o ESTATAL

CREATE INDEX IF NOT EXISTS idx_leyes_ambito ON leyes (ambito);
CREATE INDEX IF NOT EXISTS idx_leyes_estado_municipio ON leyes (estado, municipio);

-- Vista de apoyo para el motor jurídico: dado un estado (+ municipio opcional),
-- devuelve TODAS las leyes que aplican (federales + estatales + municipales).
CREATE OR REPLACE VIEW v_marco_juridico_aplicable AS
SELECT l.*, 0 AS prioridad FROM leyes l WHERE l.ambito = 'FEDERAL'
UNION ALL
SELECT l.*, 1 AS prioridad FROM leyes l WHERE l.ambito = 'ESTATAL'
UNION ALL
SELECT l.*, 2 AS prioridad FROM leyes l WHERE l.ambito = 'MUNICIPAL';

COMMENT ON TABLE maquinaria_catalogo IS 'Costos horarios de maquinaria por catálogo fuente (CMIC, BIMSA, etc.)';
COMMENT ON TABLE mano_obra_salarios IS 'Tabuladores de salarios base/reales por categoría (ejecución, estudios, supervisión, profesionales)';
COMMENT ON TABLE indices_precios_materiales IS 'Variación porcentual de precios de insumos (informes CEICO)';
COMMENT ON TABLE factores_indirectos IS 'Factor de indirecto/utilidad integrado por convocante y mes (ej. CDMX-SOBSE)';
COMMENT ON TABLE costos_parametricos_obra IS 'Costos paramétricos genéricos por unidad de obra (km, aula, cama, etc.)';
COMMENT ON TABLE rendimientos_mano_obra IS 'Rendimientos mínimos/medios/máximos de mano de obra y maquinaria';
COMMENT ON VIEW v_marco_juridico_aplicable IS 'Resuelve qué leyes aplican (federal+estatal+municipal) para el motor jurídico de 12 reglas, según ubicación de la licitación';

COMMIT;
