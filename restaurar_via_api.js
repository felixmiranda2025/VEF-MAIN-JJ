/**
 * VEF ERP — Script de restauración vía API
 * Ejecutar DESPUÉS de que el servidor esté corriendo
 * Uso: node restaurar_via_api.js [URL] [TOKEN]
 * 
 * Restaura los datos del PDF FAC-FAC-2026-001:
 *   Empresa:  VEF Automatización (GOBE840604JLA)
 *   Cliente:  HMO AUTOMATIZACION Y COMERCIALIZACION INDUSTRIAL
 *   Factura:  FAC-2026-001 — $37,208.59 MXN
 */

const BASE = process.argv[2] || 'http://localhost:3000';
const TOKEN = process.argv[3] || '';

const h = {
  'Content-Type': 'application/json',
  ...(TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {})
};

async function post(path, body) {
  const r = await fetch(`${BASE}${path}`, {
    method: 'POST', headers: h, body: JSON.stringify(body)
  });
  const d = await r.json();
  if (!r.ok) throw new Error(`${path}: ${d.error || r.status}`);
  return d;
}
async function put(path, body) {
  const r = await fetch(`${BASE}${path}`, {
    method: 'PUT', headers: h, body: JSON.stringify(body)
  });
  const d = await r.json();
  if (!r.ok) throw new Error(`${path}: ${d.error || r.status}`);
  return d;
}
async function get(path) {
  const r = await fetch(`${BASE}${path}`, { headers: h });
  return r.json();
}

(async () => {
  console.log('🔄 VEF ERP — Restaurando datos FAC-2026-001...\n');

  // 1. Empresa
  console.log('1️⃣  Configurando empresa...');
  try {
    await put('/api/empresa', {
      nombre: 'VEF Automatización',
      razon_social: 'VEF Automatización',
      rfc: 'GOBE840604JLA',
      regimen_fiscal: 'Régimen Simplificado de Confianza',
      telefono: '+52 (722) 115-7792',
      email: 'soporte.ventas@vef-automatizacion.com',
      direccion: 'Privada Rio Panuco, Manzana 5 Lote 10',
      ciudad: 'Toluca',
      estado: 'Estado de México',
      cp: '50227',
      pais: 'México',
      moneda_default: 'MXN',
    });
    console.log('   ✅ Empresa configurada');
  } catch(e) { console.log('   ⚠️ Empresa:', e.message); }

  // 2. Cliente
  console.log('2️⃣  Creando/verificando cliente...');
  let clienteId;
  try {
    const clientes = await get('/api/clientes');
    const existing = (clientes || []).find(c => c.rfc === 'HAC190729242');
    if (existing) {
      clienteId = existing.id;
      console.log(`   ✅ Cliente existente ID=${clienteId}`);
    } else {
      const cli = await post('/api/clientes', {
        nombre: 'HMO AUTOMATIZACION Y COMERCIALIZACION INDUSTRIAL',
        rfc: 'HAC190729242',
        regimen_fiscal: 'Régimen Simplificado de Confianza',
        tipo_persona: 'moral',
        email: 'hmo.venta1@gmail.com',
        telefono: '7223087247',
        direccion: 'Melero y Piña 511-interior 102, San Sebastian',
        cp: '50150',
        ciudad: 'Toluca de Lerdo, Méx.',
        uso_cfdi: 'G03',
      });
      clienteId = cli.id;
      console.log(`   ✅ Cliente creado ID=${clienteId}`);
    }
  } catch(e) { console.log('   ⚠️ Cliente:', e.message); }

  // 3. Proyecto
  console.log('3️⃣  Creando proyecto...');
  let proyectoId;
  try {
    const proyectos = await get('/api/proyectos');
    const existing = (proyectos || []).find(p => p.nombre === 'Servicio de Programación HMO');
    if (existing) {
      proyectoId = existing.id;
      console.log(`   ✅ Proyecto existente ID=${proyectoId}`);
    } else {
      const proy = await post('/api/proyectos', {
        nombre: 'Servicio de Programación HMO',
        cliente_id: clienteId,
        estatus: 'activo',
        responsable: 'VEF Automatización',
      });
      proyectoId = proy.id;
      console.log(`   ✅ Proyecto creado ID=${proyectoId}`);
    }
  } catch(e) { console.log('   ⚠️ Proyecto:', e.message); }

  // 4. Cotización
  console.log('4️⃣  Creando cotización...');
  let cotizacionId;
  try {
    const cots = await get('/api/cotizaciones');
    const existing = (cots || []).find(c => c.numero_cotizacion === 'COT-2026-001');
    if (existing) {
      cotizacionId = existing.id;
      console.log(`   ✅ Cotización existente ID=${cotizacionId}`);
    } else {
      const cot = await post('/api/cotizaciones', {
        proyecto_id: proyectoId,
        moneda: 'MXN',
        alcance_tecnico: 'Servicio de programación industrial',
        condiciones_pago: 'PPD — Pago en Parcialidades o Diferido',
        validez_hasta: '2026-04-10',
        items: [{
          descripcion: 'Servicio de programacion',
          cantidad: 1,
          precio_unitario: 35750.00,
          total: 35750.00,
          clave_prod_serv: '81111600',
          clave_unidad: 'H87',
        }],
      });
      cotizacionId = cot.id;
      // Marcar como aprobada
      try { await put(`/api/cotizaciones/${cotizacionId}`, { estatus: 'aprobada' }); } catch{}
      console.log(`   ✅ Cotización creada ID=${cotizacionId}`);
    }
  } catch(e) { console.log('   ⚠️ Cotización:', e.message); }

  // 5. Factura
  console.log('5️⃣  Creando factura FAC-2026-001...');
  try {
    const facts = await get('/api/facturas');
    const existing = (facts || []).find(f => f.numero_factura === 'FAC-2026-001');
    if (existing) {
      console.log(`   ✅ Factura ya existe ID=${existing.id}`);
    } else {
      const fac = await post('/api/facturas', {
        cotizacion_id: cotizacionId,
        cliente_id: clienteId,
        moneda: 'MXN',
        subtotal: 35750.00,
        iva: 5720.00,
        retencion_isr: 446.88,
        retencion_iva: 3814.53,
        total: 37208.59,
        fecha_vencimiento: '2026-04-10',
        notas: 'PPD — Pago en Parcialidades o Diferido | Forma de Pago: 03 — Transferencia electrónica',
      });
      console.log(`   ✅ Factura creada: ${fac.numero_factura} ID=${fac.id}`);
    }
  } catch(e) { console.log('   ⚠️ Factura:', e.message); }

  console.log('\n✅ Restauración completada. Verifica en el ERP:');
  console.log(`   ${BASE}/app → Finanzas → Órdenes y Facturas → FAC-2026-001`);
  console.log(`   Subtotal: $35,750.00 | IVA: $5,720.00 | ISR: -$446.88 | RetIVA: -$3,814.53 | Total: $37,208.59`);
})().catch(e => console.error('❌ Error:', e.message));
