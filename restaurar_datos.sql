-- ================================================================
-- VEF ERP — SCRIPT DE RESTAURACIÓN DE DATOS
-- Basado en FAC-FAC-2026-001.pdf
-- Schema: emp_vef (empresa VEF Automatización)
-- ================================================================

SET search_path TO "emp_vef", public;

-- ── 1. EMPRESA CONFIG ─────────────────────────────────────────
INSERT INTO empresa_config (
  nombre, razon_social, rfc, regimen_fiscal,
  telefono, email, direccion, ciudad, estado, cp, pais,
  moneda_default, iva_default
)
SELECT
  'VEF Automatización',
  'VEF Automatización',
  'GOBE840604JLA',
  'Régimen Simplificado de Confianza',
  '+52 (722) 115-7792',
  'soporte.ventas@vef-automatizacion.com',
  'Privada Rio Panuco, Manzana 5 Lote 10',
  'Toluca',
  'Estado de México',
  '50227',
  'México',
  'MXN',
  16.00
WHERE NOT EXISTS (SELECT 1 FROM empresa_config LIMIT 1);

-- Si ya existe, actualizar
UPDATE empresa_config SET
  nombre          = 'VEF Automatización',
  rfc             = 'GOBE840604JLA',
  regimen_fiscal  = 'Régimen Simplificado de Confianza',
  telefono        = '+52 (722) 115-7792',
  email           = 'soporte.ventas@vef-automatizacion.com',
  direccion       = 'Privada Rio Panuco, Manzana 5 Lote 10',
  ciudad          = 'Toluca',
  estado          = 'Estado de México',
  cp              = '50227',
  pais            = 'México',
  moneda_default  = 'MXN',
  iva_default     = 16.00,
  updated_at      = NOW()
WHERE id = (SELECT id FROM empresa_config LIMIT 1);

-- ── 2. CLIENTE: HMO AUTOMATIZACION ───────────────────────────
INSERT INTO clientes (
  nombre, rfc, regimen_fiscal, tipo_persona,
  email, telefono, direccion, cp, ciudad,
  uso_cfdi, activo
)
SELECT
  'HMO AUTOMATIZACION Y COMERCIALIZACION INDUSTRIAL',
  'HAC190729242',
  'Régimen Simplificado de Confianza',
  'moral',
  'hmo.venta1@gmail.com',
  '7223087247',
  'Melero y Piña 511-interior 102, San Sebastian',
  '50150',
  'Toluca de Lerdo, Méx.',
  'G03',
  true
WHERE NOT EXISTS (
  SELECT 1 FROM clientes WHERE rfc = 'HAC190729242'
);

-- ── 3. PROYECTO PARA LA FACTURA ───────────────────────────────
INSERT INTO proyectos (nombre, cliente_id, estatus, responsable)
SELECT
  'Servicio de Programación HMO',
  (SELECT id FROM clientes WHERE rfc = 'HAC190729242' LIMIT 1),
  'activo',
  'VEF Automatización'
WHERE NOT EXISTS (
  SELECT 1 FROM proyectos WHERE nombre = 'Servicio de Programación HMO'
);

-- ── 4. COTIZACIÓN BASE ────────────────────────────────────────
INSERT INTO cotizaciones (
  numero_cotizacion, proyecto_id, fecha_emision, validez_hasta,
  alcance_tecnico, moneda, subtotal, iva, total, estatus
)
SELECT
  'COT-2026-001',
  (SELECT id FROM proyectos WHERE nombre = 'Servicio de Programación HMO' LIMIT 1),
  '2026-04-10',
  '2026-04-10',
  'Servicio de programación industrial',
  'MXN',
  35750.00,
  5720.00,
  37208.59,
  'aprobada'
WHERE NOT EXISTS (
  SELECT 1 FROM cotizaciones WHERE numero_cotizacion = 'COT-2026-001'
);

-- Items de la cotización
INSERT INTO items_cotizacion (
  cotizacion_id, descripcion, cantidad, precio_unitario, total,
  clave_prod_serv, clave_unidad, objeto_imp
)
SELECT
  (SELECT id FROM cotizaciones WHERE numero_cotizacion = 'COT-2026-001' LIMIT 1),
  'Servicio de programacion',
  1,
  35750.00,
  35750.00,
  '81111600',
  'H87',
  '02'
WHERE NOT EXISTS (
  SELECT 1 FROM items_cotizacion
  WHERE cotizacion_id = (SELECT id FROM cotizaciones WHERE numero_cotizacion = 'COT-2026-001' LIMIT 1)
);

-- ── 5. FACTURA FAC-2026-001 ───────────────────────────────────
INSERT INTO facturas (
  numero_factura,
  cotizacion_id,
  cliente_id,
  moneda,
  subtotal,
  iva,
  retencion_isr,
  retencion_iva,
  total,
  monto,
  fecha_emision,
  fecha_vencimiento,
  estatus,
  estatus_pago,
  notas
)
SELECT
  'FAC-2026-001',
  (SELECT id FROM cotizaciones WHERE numero_cotizacion = 'COT-2026-001' LIMIT 1),
  (SELECT id FROM clientes WHERE rfc = 'HAC190729242' LIMIT 1),
  'MXN',
  35750.00,
  5720.00,
  446.88,
  3814.53,
  37208.59,
  37208.59,
  '2026-04-10',
  '2026-04-10',
  'pendiente',
  'pendiente',
  'Método de Pago: PPD — Pago en Parcialidades o Diferido | Forma de Pago: 03 — Transferencia electrónica'
WHERE NOT EXISTS (
  SELECT 1 FROM facturas WHERE numero_factura = 'FAC-2026-001'
);

-- ── 6. VERIFICACIÓN FINAL ─────────────────────────────────────
SELECT 'empresa_config' AS tabla, COUNT(*) AS registros FROM empresa_config
UNION ALL
SELECT 'clientes',    COUNT(*) FROM clientes
UNION ALL
SELECT 'proyectos',   COUNT(*) FROM proyectos
UNION ALL
SELECT 'cotizaciones',COUNT(*) FROM cotizaciones
UNION ALL
SELECT 'items_cotizacion', COUNT(*) FROM items_cotizacion
UNION ALL
SELECT 'facturas',    COUNT(*) FROM facturas
ORDER BY tabla;

