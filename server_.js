// ================================================================
// VEF AUTOMATIZACIÓN — ERP Industrial
// server.js — Compatible con esquema existente en BD
// ================================================================
const express   = require('express');
const cors      = require('cors');
const helmet    = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool }  = require('pg');
const jwt       = require('jsonwebtoken');
const bcrypt    = require('bcryptjs');
const nodemailer= require('nodemailer');
const PDFKit    = require('pdfkit');
const path      = require('path');
const fs        = require('fs');
require('dotenv').config();

const app  = express();
const PORT = process.env.PORT || 3000;

const VEF_NOMBRE   = 'VEF Automatización';
const VEF_TELEFONO = '+52 (722) 115-7792';
const VEF_CORREO   = 'soporte.ventas@vef-automatizacion.com';

// Logo: buscar en carpeta raíz del proyecto
// Logo path — prioridad: 1) upload en caliente, 2) .env LOGO_FILE, 3) auto-búsqueda
function getLogoPath() {
  // 1. Upload realizado desde la pantalla de Configuración (sin reiniciar)
  if (global._logoPathOverride && fs.existsSync(global._logoPathOverride)) return global._logoPathOverride;
  // 2. Variable de entorno LOGO_FILE (puede ser ruta absoluta o relativa)
  if (process.env.LOGO_FILE) {
    const envPath = path.isAbsolute(process.env.LOGO_FILE)
      ? process.env.LOGO_FILE
      : path.join(__dirname, process.env.LOGO_FILE);
    if (fs.existsSync(envPath)) return envPath;
  }
  // 3. Auto-búsqueda en carpeta raíz y frontend/
  for (const n of ['logo.png','logo.PNG','logo.jpg','logo.JPG','logo.jpeg','Logo.png','Logo.jpg']) {
    const p = path.join(__dirname, n);
    if (fs.existsSync(p)) return p;
  }
  for (const n of ['logo.png','logo.PNG','logo.jpg','logo.JPG','logo.jpeg']) {
    const p = path.join(__dirname, 'frontend', n);
    if (fs.existsSync(p)) return p;
  }
  return '';
}
const LOGO_PATH = getLogoPath();

// ── DB ───────────────────────────────────────────────────────────
const pool = new Pool({
  host    : process.env.DB_HOST,
  port    : parseInt(process.env.DB_PORT) || 5432,
  database: process.env.DB_NAME || 'postgres',
  user    : process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl     : { rejectUnauthorized: false },
  max: 10,                    // Suficiente para uso normal
  idleTimeoutMillis: 10000,   // Cerrar conexiones inactivas más rápido
  connectionTimeoutMillis: 8000,
  allowExitOnIdle: true,
});
pool.on('error', e => console.error('DB pool error:', e.message));

// ── Helper: conexión con search_path de empresa ──────────
async function getSchemaClient(schema) {
  const client = await pool.connect();
  if (schema && schema !== 'public') {
    await client.query(`SET search_path TO "${schema}", public`);
  }
  return client;
}

// ── Crear schema completo para empresa nueva ─────────────
async function crearSchemaEmpresa(slug, nombreEmpresa) {
  const schema = 'emp_' + slug.toLowerCase().replace(/[^a-z0-9]/g,'_');
  const client = await pool.connect();
  try {
    await client.query(`CREATE SCHEMA IF NOT EXISTS "${schema}"`);
    await client.query(`SET search_path TO "${schema}", public`);
    const tablas = [
      `CREATE TABLE IF NOT EXISTS clientes (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,contacto TEXT,direccion TEXT,telefono TEXT,email TEXT,rfc TEXT,regimen_fiscal TEXT,cp TEXT,ciudad TEXT,activo BOOLEAN DEFAULT true,constancia_pdf BYTEA,constancia_nombre TEXT,constancia_fecha TIMESTAMP,estado_cuenta_pdf BYTEA,estado_cuenta_nombre TEXT,estado_cuenta_fecha TIMESTAMP,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS proveedores (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,contacto TEXT,direccion TEXT,telefono TEXT,email TEXT,rfc TEXT,condiciones_pago TEXT,activo BOOLEAN DEFAULT true,constancia_pdf BYTEA,constancia_nombre TEXT,constancia_fecha TIMESTAMP,estado_cuenta_pdf BYTEA,estado_cuenta_nombre TEXT,estado_cuenta_fecha TIMESTAMP,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS proyectos (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,cliente_id INTEGER,responsable TEXT,fecha_creacion DATE DEFAULT CURRENT_DATE,estatus TEXT DEFAULT 'activo',created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS cotizaciones (id SERIAL PRIMARY KEY,proyecto_id INTEGER,numero_cotizacion TEXT,fecha_emision DATE DEFAULT CURRENT_DATE,validez_hasta DATE,alcance_tecnico TEXT,notas_importantes TEXT,comentarios_generales TEXT,condiciones_entrega TEXT,condiciones_pago TEXT,garantia TEXT,total NUMERIC(15,2) DEFAULT 0,moneda TEXT DEFAULT 'USD',estatus TEXT DEFAULT 'borrador',created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS items_cotizacion (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,descripcion TEXT,cantidad NUMERIC(10,2),precio_unitario NUMERIC(15,2),total NUMERIC(15,2))`,
      `CREATE TABLE IF NOT EXISTS seguimientos (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,notas TEXT,proxima_accion TEXT)`,
      `CREATE TABLE IF NOT EXISTS ordenes_proveedor (id SERIAL PRIMARY KEY,proveedor_id INTEGER,numero_op TEXT,fecha_emision DATE DEFAULT CURRENT_DATE,fecha_entrega DATE,condiciones_pago TEXT,lugar_entrega TEXT,notas TEXT,total NUMERIC(15,2) DEFAULT 0,moneda TEXT DEFAULT 'USD',estatus TEXT DEFAULT 'borrador',factura_pdf BYTEA,factura_nombre TEXT,cotizacion_pdf BYTEA,cotizacion_nombre TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS items_orden_proveedor (id SERIAL PRIMARY KEY,orden_id INTEGER,descripcion TEXT,cantidad NUMERIC(10,2),precio_unitario NUMERIC(15,2),total NUMERIC(15,2))`,
      `CREATE TABLE IF NOT EXISTS seguimientos_oc (id SERIAL PRIMARY KEY,orden_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,notas TEXT,proxima_accion TEXT)`,
      `CREATE TABLE IF NOT EXISTS facturas (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,numero_factura TEXT,cliente_id INTEGER,moneda TEXT DEFAULT 'USD',subtotal NUMERIC(15,2) DEFAULT 0,iva NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,fecha_emision DATE DEFAULT CURRENT_DATE,fecha_vencimiento DATE,estatus TEXT DEFAULT 'pendiente',notas TEXT,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS pagos (id SERIAL PRIMARY KEY,factura_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),monto NUMERIC(15,2),metodo TEXT,referencia TEXT,notas TEXT,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS inventario (id SERIAL PRIMARY KEY,codigo TEXT,nombre TEXT NOT NULL,descripcion TEXT,categoria TEXT,unidad TEXT DEFAULT 'pza',cantidad_actual NUMERIC(10,2) DEFAULT 0,cantidad_minima NUMERIC(10,2) DEFAULT 0,precio_costo NUMERIC(15,2) DEFAULT 0,precio_venta NUMERIC(15,2) DEFAULT 0,ubicacion TEXT,proveedor_id INTEGER,foto TEXT,notas TEXT,activo BOOLEAN DEFAULT true,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS movimientos_inventario (id SERIAL PRIMARY KEY,producto_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,cantidad NUMERIC(10,2),stock_anterior NUMERIC(10,2) DEFAULT 0,stock_nuevo NUMERIC(10,2) DEFAULT 0,referencia TEXT,notas TEXT,created_by INTEGER)`,
      `CREATE TABLE IF NOT EXISTS tareas (id SERIAL PRIMARY KEY,titulo VARCHAR(300) NOT NULL,descripcion TEXT,proyecto_id INTEGER,asignado_a INTEGER,creado_por INTEGER,prioridad VARCHAR(20) DEFAULT 'normal',estatus VARCHAR(30) DEFAULT 'pendiente',fecha_inicio DATE,fecha_vencimiento DATE,notas TEXT,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS egresos (id SERIAL PRIMARY KEY,fecha DATE NOT NULL DEFAULT CURRENT_DATE,proveedor_nombre VARCHAR(200),categoria VARCHAR(100),descripcion TEXT,subtotal NUMERIC(15,2) DEFAULT 0,iva NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,metodo VARCHAR(50) DEFAULT 'Transferencia',referencia VARCHAR(100),numero_factura VARCHAR(100),factura_pdf BYTEA,factura_nombre TEXT,notas TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS pdfs_guardados (id SERIAL PRIMARY KEY,tipo VARCHAR(30) NOT NULL,referencia_id INTEGER NOT NULL,numero_doc VARCHAR(100),cliente_proveedor VARCHAR(200),nombre_archivo VARCHAR(200),tamanio_bytes INTEGER,pdf_data BYTEA,generado_por INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS empresa_config (id SERIAL PRIMARY KEY,nombre VARCHAR(200) NOT NULL DEFAULT 'Mi Empresa',razon_social VARCHAR(200),rfc VARCHAR(30),regimen_fiscal VARCHAR(100),contacto VARCHAR(100),telefono VARCHAR(50),email VARCHAR(100),direccion TEXT,ciudad VARCHAR(100),estado VARCHAR(100),cp VARCHAR(10),pais VARCHAR(50) DEFAULT 'México',moneda_default VARCHAR(10) DEFAULT 'USD',iva_default NUMERIC(5,2) DEFAULT 16.00,margen_ganancia NUMERIC(5,2) DEFAULT 0,smtp_host VARCHAR(100),smtp_port INTEGER DEFAULT 465,smtp_user VARCHAR(100),smtp_pass VARCHAR(200),notas_factura TEXT,notas_cotizacion TEXT,updated_at TIMESTAMP DEFAULT NOW())`,
    ];
    for (const sql of tablas) await client.query(sql);
    await client.query(`INSERT INTO empresa_config (nombre) VALUES ($1)`,[nombreEmpresa||'Mi Empresa']);
    console.log('✅ Schema creado:', schema);
    return schema;
  } finally { client.release(); }
}

// Esquema real de la BD (se llena en autoSetup)
let DB = {};  // DB['tabla'] = ['col1','col2',...]

const has = (table, col) => (DB[table] || []).includes(col);

// ── Cache de columnas por schema (evita queries repetidas a information_schema) ──
const _colCache = {};  // { 'schema.tabla': Set<string> }

async function getCols(schema, table) {
  const key = `${schema}.${table}`;
  if (_colCache[key] && _colCache[key].size > 0) return _colCache[key];
  try {
    const {rows} = await pool.query(
      `SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`,
      [schema, table]);
    if(rows.length > 0) {
      _colCache[key] = new Set(rows.map(r => r.column_name));
      return _colCache[key];
    }
    // Si no hay columnas, no cachear — puede ser que la BD aún no está lista
    return new Set();
  } catch(e) {
    console.warn('getCols error:', schema, table, e.message);
    return new Set();
  }
}

// Invalidar cache cuando autoSetup agrega columnas
function clearColCache() { Object.keys(_colCache).forEach(k => delete _colCache[k]); }

// Query seguro — nunca rompe el servidor
// Q(sql, params, schema) — ejecuta con search_path del schema de la empresa
const Q = async (sql, p=[], schema=null) => {
  const s = schema || global._defaultSchema;
  if(s && s !== 'public'){
    const client = await pool.connect();
    try {
      // Sin comillas — los schemas en minúsculas no las necesitan
      await client.query(`SET search_path TO ${s},public`);
      return (await client.query(sql, p)).rows;
    } catch(e){ console.error('Query error:', e.message, '\n  SQL:', sql.slice(0,120)); return []; }
    finally { client.release(); }
  }
  try { return (await pool.query(sql, p)).rows; }
  catch(e) { console.error('Query error:', e.message, '\n  SQL:', sql.slice(0,120)); return []; }
};

// QR(req, sql, params) — usa schema del usuario autenticado — NUNCA usa schema de otra empresa
const QR = async (req, sql, p=[]) => {
  const schema = req.user?.schema || req.user?.schema_name;
  if(!schema) {
    // Sin schema = sin empresa → error de aislamiento
    console.error('QR: usuario sin schema asignado', req.user?.id, req.user?.username);
    throw new Error('Usuario sin empresa asignada. Contacta al administrador.');
  }
  return Q(sql, p, schema);
};

// ── MIDDLEWARE ───────────────────────────────────────────────────
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors({ origin: '*', credentials: true }));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(express.static(path.join(__dirname, 'frontend')));
app.use('/api', rateLimit({ windowMs:15*60*1000, max:2000 }));

// ── AUTH ─────────────────────────────────────────────────────────
const JWT_SECRET = process.env.JWT_SECRET || 'vef_secret_2025';
function auth(req, res, next) {
  // Accept token from header OR ?token= query param (for PDF window.open)
  const t = req.headers.authorization?.split(' ')[1] || req.query.token;
  if (!t) return res.status(401).json({ error: 'Token requerido' });
  try { req.user = jwt.verify(t, JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'Token inválido' }); }
}
function adminOnly(req, res, next) {
  if (req.user?.rol !== 'admin') return res.status(403).json({ error: 'Solo admin' });
  next();
}

// ── EMAIL ────────────────────────────────────────────────────────
// Mailer estático del .env (fallback)
const mailer = nodemailer.createTransport({
  host: process.env.SMTP_HOST || 'smtp.zoho.com',
  port: parseInt(process.env.SMTP_PORT || '465'),
  secure: parseInt(process.env.SMTP_PORT || '465') === 465,
  auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  tls: { rejectUnauthorized: false, ciphers: 'SSLv3' }
});

// Obtener transporter dinámico desde empresa_config del schema del usuario
async function getMailer(schema) {
  try {
    const sch = schema || global._defaultSchema || 'emp_vef';
    const rows = await Q('SELECT smtp_host,smtp_port,smtp_user,smtp_pass,email FROM empresa_config LIMIT 1', [], sch);
    const cfg = rows[0];
    if(cfg?.smtp_host && cfg?.smtp_user && cfg?.smtp_pass) {
      const port = parseInt(cfg.smtp_port)||465;
      const isGmail = cfg.smtp_host?.includes('gmail.com');
      const isZoho  = cfg.smtp_host?.includes('zoho.com');
      const secure  = port === 465; // 465=SSL, 587=STARTTLS
      return nodemailer.createTransport({
        host: cfg.smtp_host,
        port,
        secure,
        auth: {
          user: cfg.smtp_user,
          pass: cfg.smtp_pass,
          // Gmail con OAuth no necesita type especial
        },
        connectionTimeout: 30000,
        greetingTimeout: 15000,
        socketTimeout: 30000,
        tls: {
          rejectUnauthorized: false,
          // Gmail requiere SNI
          servername: cfg.smtp_host,
        },
        requireTLS: port === 587,
      });
    }
    // Fallback a .env
    if(process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS){
      return mailer;
    }
  } catch(e) { console.warn('getMailer error:', e.message); }
  return mailer;
}

async function getFromEmail(schema) {
  try {
    const sch = schema || global._defaultSchema || 'emp_vef';
    const rows = await Q('SELECT smtp_user,email,nombre FROM empresa_config LIMIT 1', [], sch);
    const cfg = rows[0];
    return cfg?.smtp_user || cfg?.email || process.env.SMTP_USER || 'noreply@erp.local';
  } catch(e) { return process.env.SMTP_USER || 'noreply@erp.local'; }
}

// ================================================================
// PDF — con logo VEF si existe logo.png en la carpeta del proyecto
// ================================================================
const C = { AZUL:'#0D2B55', AZUL_MED:'#1A4A8A', AZUL_SUV:'#D6E4F7',
            GRIS:'#F4F6FA', GRIS_B:'#CCCCCC', BLANCO:'#FFFFFF', TEXTO:'#333333' };

function pdfHeader(doc, titulo, subs=[], emp={}) {
  const M=28, W=539, H=96;
  const _lp=getLogoPath();
  const hasLogo = !!_lp;
  const LW = 130;

  doc.rect(M, 14, W, H).fill(C.AZUL);

  if (hasLogo) {
    doc.rect(M, 14, LW, H).fill(C.BLANCO);
    try { doc.image(_lp, M+6, 18, { fit:[LW-12, H-8], align:'center', valign:'center' }); } catch(e){}
  }

  const tx = hasLogo ? M+LW+10 : M+14;
  const tw = hasLogo ? W-LW-14 : W-28;
  const ta = hasLogo ? 'left' : 'center';

  // Nombre empresa
  const empNom = emp.nombre || emp.razon_social || VEF_NOMBRE;
  doc.fillColor(C.BLANCO).fontSize(14).font('Helvetica-Bold')
     .text(empNom, tx, 20, { width:tw, align:ta });

  // RFC + Régimen fiscal
  let infoY = 37;
  if (emp.rfc || emp.regimen_fiscal) {
    const rfcLine = [emp.rfc?'RFC: '+emp.rfc:'', emp.regimen_fiscal?emp.regimen_fiscal:''].filter(Boolean).join('  |  ');
    doc.fillColor('#A8C5F0').fontSize(8).font('Helvetica')
       .text(rfcLine, tx, infoY, { width:tw, align:ta });
    infoY += 11;
  }
  // Dirección
  if (emp.direccion || emp.ciudad) {
    const dir = [emp.direccion, emp.ciudad, emp.estado, emp.cp].filter(Boolean).join(', ');
    doc.fillColor('#A8C5F0').fontSize(8).font('Helvetica')
       .text(dir, tx, infoY, { width:tw, align:ta });
    infoY += 11;
  }

  // Separador y título del documento
  doc.moveTo(tx, infoY+2).lineTo(tx+tw, infoY+2).lineWidth(0.5).strokeColor('#A8C5F0').stroke();

  // Título del documento (COTIZACIÓN, ORDEN DE COMPRA, etc.)
  doc.fillColor(C.BLANCO).fontSize(15).font('Helvetica-Bold')
     .text(titulo, tx, infoY+6, { width:tw, align:ta });
  let ty = infoY+24;
  doc.fontSize(8).font('Helvetica');
  for (const s of subs) {
    doc.fillColor('#A8C5F0').text(s, tx, ty, { width:tw, align:ta });
    ty += 11;
  }
  doc.y = 14 + H + 10;
}

function pdfWatermark(doc) {
  const _lp=getLogoPath(); if (!_lp) return;
  try { doc.save(); doc.opacity(0.07); doc.image(_lp, 158, 270, { fit:[280,280] }); doc.restore(); }
  catch(e){}
}

function pdfPie(doc, emp={}) {
  const M=28, W=539;
  doc.moveDown(0.8);
  const y = Math.min(doc.y, 750);
  doc.moveTo(M,y).lineTo(M+W,y).lineWidth(1).strokeColor(C.AZUL_MED).stroke();
  const py = y+6;
  doc.rect(M,py,W,36).fill(C.AZUL);
  const nom = emp.nombre||VEF_NOMBRE;
  const tel = emp.telefono||VEF_TELEFONO;
  const mail= emp.email||VEF_CORREO;
  const rfc = emp.rfc ? '  |  RFC: '+emp.rfc : '';
  doc.fillColor(C.BLANCO).fontSize(8.5).font('Helvetica-Bold')
     .text(`${nom}${rfc}`, M, py+8, {width:W, align:'center'});
  doc.fillColor('#A8C5F0').fontSize(8).font('Helvetica')
     .text(`Tel: ${tel}   |   ${mail}`, M, py+20, {width:W, align:'center'});
  doc.fillColor('#888').fontSize(7.5).font('Helvetica')
     .text(`Generado el ${new Date().toLocaleDateString('es-MX')}`, M, py+50, {width:W, align:'center'});
}

function pdfSec(doc, titulo) {
  const M=28, W=539;
  doc.moveDown(0.5);
  doc.fillColor(C.AZUL).fontSize(11).font('Helvetica-Bold').text(titulo, M);
  doc.moveDown(0.2);
  doc.moveTo(M,doc.y).lineTo(M+W,doc.y).lineWidth(1.5).strokeColor(C.AZUL_MED).stroke();
  doc.moveDown(0.4);
}

function pdfGrid(doc, filas) {
  const M=28, COLS=[84,163,84,163];
  let y=doc.y;
  for (const f of filas) {
    // Calcular altura dinámica basada en el texto más largo
    let maxH=20;
    let cx=M;
    for (let i=0;i<4;i++) {
      const txt=String(f[i]||'');
      const linesEst=Math.ceil(txt.length/(COLS[i]/6.5));
      maxH=Math.max(maxH, linesEst*13+8);
    }
    const H=maxH;
    doc.rect(M,y,COLS[0]+COLS[1]+COLS[2]+COLS[3],H).fill(C.GRIS);
    doc.rect(M,y,COLS[0]+COLS[1]+COLS[2]+COLS[3],H).lineWidth(0.3).strokeColor(C.GRIS_B).stroke();
    cx=M;
    for (let i=0;i<4;i++) {
      doc.fillColor(i%2===0?C.AZUL:C.TEXTO).fontSize(9)
         .font(i%2===0?'Helvetica-Bold':'Helvetica')
         .text(String(f[i]||''), cx+5, y+5, {width:COLS[i]-8, lineBreak:true});
      cx+=COLS[i];
    }
    y+=H;
    doc.y=y;
  }
  doc.y=y+6;
}

function pdfItems(doc, items, moneda='USD') {
  const M=28,W=539,COLS=[280,56,98,105],SYM=moneda==='USD'?'$':'MX$';
  let y=doc.y;
  // Header
  doc.rect(M,y,W,22).fill(C.AZUL_MED);
  let cx=M;
  for (const [h,i] of [['Descripción',0],['Cant.',1],['P. Unitario',2],['Total '+moneda,3]]) {
    doc.fillColor(C.BLANCO).fontSize(9).font('Helvetica-Bold')
       .text(h, cx+5, y+6, {width:COLS[i]-8, align:i>0?'right':'left', lineBreak:false});
    cx+=COLS[i];
  }
  y+=22;
  if (!items.length) {
    doc.rect(M,y,W,20).fill(C.BLANCO);
    doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text('Sin partidas', M+6, y+5);
    y+=20;
  }
  for (let idx=0;idx<items.length;idx++) {
    const it=items[idx];
    const cant=parseFloat(it.cantidad||0), pu=parseFloat(it.precio_unitario||0);
    const tot=parseFloat(it.total||0)||cant*pu;
    cx=M;
    const vals=[it.descripcion||'', String(cant%1===0?cant:cant.toFixed(2)),
      SYM+pu.toLocaleString('es-MX',{minimumFractionDigits:2}),
      SYM+tot.toLocaleString('es-MX',{minimumFractionDigits:2})];
    // Altura dinámica según largo de descripción
    const descLines=Math.max(1,Math.ceil((vals[0]||'').length/42));
    const rowH=Math.max(20, descLines*13+6);
    doc.rect(M,y,W,rowH).fill(idx%2===0?C.AZUL_SUV:C.BLANCO);
    doc.rect(M,y,W,rowH).lineWidth(0.3).strokeColor(C.GRIS_B).stroke();
    for (let i=0;i<4;i++) {
      doc.fillColor(C.TEXTO).fontSize(9).font(i===3?'Helvetica-Bold':'Helvetica')
         .text(vals[i], cx+5, y+5, {width:COLS[i]-8, align:i>0?'right':'left', lineBreak:i===0});
      cx+=COLS[i];
    }
    y+=rowH;
  }
  doc.y=y+6;
}

function pdfTotal(doc, label, total, moneda='USD') {
  const M=28,W=539,SYM=moneda==='USD'?'$':'MX$';
  const y=doc.y;
  doc.rect(M,y,W,28).fill(C.AZUL);
  doc.fillColor(C.BLANCO).fontSize(13).font('Helvetica-Bold')
     .text(`${label}:  ${SYM}${parseFloat(total||0).toLocaleString('es-MX',{minimumFractionDigits:2})} ${moneda}`,
       M+10, y+7, {width:W-20, align:'right'});
  doc.y=y+40;
}

function pdfCondiciones(doc, conds) {
  const M=28,W=539,LW=130;
  let y=doc.y;
  for (const [lbl,val] of conds) {
    if (!val||!String(val).trim()) continue;
    const txt=String(val).trim();
    const h=Math.max(20, Math.ceil(txt.length/85)*13 + txt.split('\n').length*13);
    doc.rect(M,y,LW,h).fill(C.AZUL_SUV);
    doc.rect(M+LW,y,W-LW,h).fill(C.BLANCO);
    doc.rect(M,y,W,h).lineWidth(0.3).strokeColor(C.GRIS_B).stroke();
    doc.fillColor(C.AZUL).fontSize(9).font('Helvetica-Bold').text(lbl, M+5, y+5, {width:LW-8,lineBreak:false});
    doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text(txt, M+LW+5, y+5, {width:W-LW-8});
    y+=h; doc.y=y;
  }
  doc.y=y+6;
}

// Obtener empresa_config del schema del usuario
async function getEmpConfig(schema) {
  try {
    const rows = await Q('SELECT * FROM empresa_config ORDER BY id LIMIT 1', [], schema||global._defaultSchema);
    return rows[0] || {};
  } catch(e) { return {}; }
}

async function buildPDFCotizacion(cot, items, schema=null) {
  const emp = await getEmpConfig(schema||cot._schema);
  return new Promise((res,rej)=>{
    const doc=new PDFKit({margin:28,size:'A4'});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>res(Buffer.concat(ch))); doc.on('error',rej);
    pdfWatermark(doc);
    pdfHeader(doc,'COTIZACIÓN COMERCIAL',[
      `No. ${cot.numero_cotizacion||'—'}  |  Fecha: ${fmt(cot.fecha_emision||cot.created_at)}  |  Válida hasta: ${fmt(cot.validez_hasta)||'N/A'}`,
      `Proyecto: ${cot.proyecto_nombre||'—'}`
    ], emp);
    pdfSec(doc,'Información del Cliente');
    pdfGrid(doc,[
      ['Empresa:', cot.cliente_nombre||'—', 'Contacto:', cot.cliente_contacto||'—'],
      ['Dirección:',cot.cliente_dir||'—',   'Email:',    cot.cliente_email||'—'],
      ['Teléfono:', cot.cliente_tel||'—',   'RFC:',      cot.cliente_rfc||'—'],
    ]);
    if (cot.alcance_tecnico) {
      pdfSec(doc,'Alcance Técnico');
      doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text(cot.alcance_tecnico,28,doc.y,{width:539});
      doc.moveDown(0.5);
    }
    pdfSec(doc,'Detalle de Partidas / Precios');
    pdfItems(doc, items, cot.moneda||'USD');
    pdfTotal(doc,'TOTAL GENERAL', cot.total, cot.moneda||'USD');
    const conds=[
      ['Condiciones de Entrega y Pago', cot.condiciones_pago||cot.condiciones_entrega],
      ['Garantía y Responsabilidad',    cot.garantia],
      ['Servicio Postventa',            cot.servicio_postventa],
      ['Notas Importantes',             cot.notas_importantes],
      ['Comentarios Generales',         cot.comentarios_generales],
      ['Validez',                       cot.validez],
      ['Fuerza Mayor',                  cot.fuerza_mayor],
      ['Ley Aplicable',                 cot.ley_aplicable],
    ];
    if (conds.some(([,v])=>v)) { pdfSec(doc,'Términos y Condiciones'); pdfCondiciones(doc,conds); }
    pdfPie(doc,emp); doc.end();
  });
}

async function buildPDFOrden(oc, items, schema=null) {
  const emp = await getEmpConfig(schema||oc._schema);
  return new Promise((res,rej)=>{
    const doc=new PDFKit({margin:28,size:'A4'});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>res(Buffer.concat(ch))); doc.on('error',rej);
    pdfWatermark(doc);
    pdfHeader(doc,'ORDEN DE COMPRA',[
      `No. ${oc.numero_op||oc.numero_oc||'—'}  |  Emisión: ${fmt(oc.fecha_emision||oc.created_at)}  |  Entrega: ${fmt(oc.fecha_entrega)||'Por definir'}`,
    ], emp);
    pdfSec(doc,'Datos del Proveedor');
    pdfGrid(doc,[
      ['Proveedor:', oc.proveedor_nombre||'—', 'Contacto:', oc.proveedor_contacto||'—'],
      ['Dirección:', oc.proveedor_dir||'—',    'Email:',    oc.proveedor_email||'—'],
      ['Teléfono:',  oc.proveedor_tel||'—',    'RFC:',      oc.proveedor_rfc||'—'],
    ]);
    pdfSec(doc,'Condiciones');
    pdfGrid(doc,[['Cond. Pago:', oc.condiciones_pago||'—','Lugar de Entrega:',oc.lugar_entrega||'—']]);
    pdfSec(doc,'Partidas / Materiales');
    pdfItems(doc, items, oc.moneda||'USD');
    // Subtotal + IVA + Total
    const M2=28, W2=539, mon2=oc.moneda||'USD', SYM2=mon2==='USD'?'$':'MX$';
    const sub2=parseFloat(oc.subtotal)||items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0);
    const iva2=parseFloat(oc.iva)||0;
    const tot2=parseFloat(oc.total)||(sub2+iva2);
    if(iva2>0){
      // Subtotal row
      let ry=doc.y;
      doc.rect(M2,ry,W2,22).fill(C.GRIS);
      doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica')
         .text('Subtotal:',M2+10,ry+6,{width:W2-100,align:'right'});
      doc.font('Helvetica-Bold')
         .text(SYM2+sub2.toLocaleString('es-MX',{minimumFractionDigits:2})+' '+mon2,M2+10,ry+6,{width:W2-14,align:'right'});
      ry+=22;
      // IVA row
      doc.rect(M2,ry,W2,22).fill(C.GRIS);
      doc.fillColor(C.AZUL).fontSize(9).font('Helvetica-Bold')
         .text('IVA (16%):',M2+10,ry+6,{width:W2-100,align:'right'});
      doc.fillColor(C.TEXTO)
         .text(SYM2+iva2.toLocaleString('es-MX',{minimumFractionDigits:2})+' '+mon2,M2+10,ry+6,{width:W2-14,align:'right'});
      doc.y=ry+22;
    }
    pdfTotal(doc,'TOTAL ORDEN', tot2, mon2);
    if (oc.notas) { pdfSec(doc,'Notas'); doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text(oc.notas,28,doc.y,{width:539}); doc.moveDown(0.5); }
    // Firmas
    doc.moveDown(1.2);
    const fy=doc.y;
    doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica')
       .text('_______________________________',28,fy,{width:240,align:'center'})
       .text('_______________________________',299,fy,{width:240,align:'center'});
    doc.moveDown(0.3);
    doc.font('Helvetica-Bold')
       .text(`Autorizado: ${emp.nombre||VEF_NOMBRE}`,28,doc.y,{width:240,align:'center'})
       .text(`Proveedor: ${oc.proveedor_nombre||'—'}`,299,doc.y,{width:240,align:'center'});
    pdfPie(doc,emp); doc.end();
  });
}

async function buildPDFFactura(f, items=[], schema=null) {
  const emp = await getEmpConfig(schema||f._schema);
  return new Promise((res,rej)=>{
    const doc=new PDFKit({margin:28,size:'A4'});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>res(Buffer.concat(ch))); doc.on('error',rej);
    pdfWatermark(doc);
    pdfHeader(doc,'FACTURA',[
      `No. ${f.numero_factura||'—'}  |  Fecha: ${fmt(f.fecha_emision)}  |  Estatus: ${(f.estatus||'pendiente').toUpperCase()}`,
    ], emp);
    pdfSec(doc,'Datos del Cliente');
    pdfGrid(doc,[
      ['Cliente:', f.cliente_nombre||'—', 'RFC:', f.cliente_rfc||'—'],
      ['Email:',   f.cliente_email||'—',  'Tel:', f.cliente_tel||'—'],
    ]);
    if (items.length) { pdfSec(doc,'Detalle'); pdfItems(doc,items,f.moneda||'USD'); }
    const M=28,W=539,SYM=(f.moneda||'USD')==='USD'?'$':'MX$';
    const sub=parseFloat(f.subtotal||f.monto||f.total||0), iva=parseFloat(f.iva||0);
    doc.fillColor(C.TEXTO).fontSize(10).font('Helvetica')
       .text(`Subtotal: ${SYM}${sub.toLocaleString('es-MX',{minimumFractionDigits:2})}`,M,doc.y,{width:W,align:'right'})
       .text(`IVA: ${SYM}${iva.toLocaleString('es-MX',{minimumFractionDigits:2})}`,M,doc.y,{width:W,align:'right'});
    doc.moveDown(0.3);
    pdfTotal(doc,'TOTAL FACTURA', f.total||f.monto, f.moneda||'USD');
    if (f.fecha_vencimiento) doc.fillColor(C.AZUL).fontSize(9).font('Helvetica-Bold')
       .text(`Vencimiento: ${fmt(f.fecha_vencimiento)}`,M,doc.y,{width:W});
    if (f.notas) { doc.moveDown(0.3); doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text(f.notas,M,doc.y,{width:W}); }
    pdfPie(doc,emp); doc.end();
  });
}

function fmt(v) {
  if (!v) return '—';
  try { return new Date(v).toLocaleDateString('es-MX',{day:'2-digit',month:'2-digit',year:'numeric'}); }
  catch { return String(v).slice(0,10); }
}

// ================================================================
// HEALTH
// ================================================================
app.get('/api/health', async (req,res) => {
  const t=Date.now();
  try {
    const [{db,u,ts}] = (await pool.query(`SELECT current_database() db,current_user u,NOW() ts`)).rows;
    const tabsRes = await pool.query(`SELECT COUNT(*) cnt FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog','pg_toast')`);
    const tabs = parseInt(tabsRes.rows[0]?.cnt||0);
    res.json({status:'ok',connected:true,latency_ms:Date.now()-t,database:db,server_time:ts,
      total_tables:tabs, logo:LOGO_PATH?'✅ '+path.basename(LOGO_PATH):'❌ no encontrado',
      default_schema:global._defaultSchema, empresa_id:global._defaultEmpresaId});
  } catch(e){ res.status(503).json({status:'error',connected:false,error:e.message}); }
});

// Lista de tablas por schema — para el admin de BD
app.get('/api/health/tables', async (req,res) => {
  try {
    const schemas = await pool.query(`
      SELECT schema_name FROM information_schema.schemata
      WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast','pg_temp_1','pg_toast_temp_1')
      ORDER BY schema_name`);
    
    const result = {};
    for(const {schema_name} of schemas.rows){
      const tbls = await pool.query(`
        SELECT t.table_name,
          (SELECT COUNT(*) FROM information_schema.columns c 
           WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name) col_count
        FROM information_schema.tables t
        WHERE t.table_schema=$1 AND t.table_type='BASE TABLE'
        ORDER BY t.table_name`,[schema_name]);
      if(tbls.rows.length > 0)
        result[schema_name] = tbls.rows.map(r=>({name:r.table_name, cols:parseInt(r.col_count)}));
    }
    res.json({schemas:result, default_schema:global._defaultSchema});
  } catch(e){ res.status(500).json({error:e.message}); }
});

// Columnas de una tabla específica
app.get('/api/health/columns/:schema/:table', async (req,res) => {
  try {
    const cols = await pool.query(`
      SELECT column_name,data_type,column_default,is_nullable
      FROM information_schema.columns
      WHERE table_schema=$1 AND table_name=$2
      ORDER BY ordinal_position`,[req.params.schema, req.params.table]);
    res.json(cols.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

// Conteo de registros en una tabla
app.get('/api/health/count/:schema/:table', async (req,res) => {
  try {
    const sc = await pool.connect();
    try {
      await sc.query(`SET search_path TO "${req.params.schema}",public`);
      const r = await sc.query(`SELECT COUNT(*) cnt FROM ${req.params.table}`);
      res.json({count: parseInt((r[0]||{}).cnt||0)});
    } finally { sc.release(); }
  } catch(e){ res.status(500).json({error:e.message}); }
});

// Test rápido del dashboard — sin auth
app.get('/api/test-dash', async (req,res)=>{
  const schema = global._defaultSchema || 'emp_vef';
  let client;
  const result = {schema, steps:[]};
  try {
    result.steps.push('connecting...');
    client = await pool.connect();
    result.steps.push('connected');
    await client.query('SET search_path TO '+schema+',public');
    result.steps.push('search_path set');
    const r = await client.query('SELECT COUNT(*) val FROM empresa_config');
    result.steps.push('empresa_config count='+r.rows[0].val);
    const r2 = await client.query('SELECT COUNT(*) val FROM clientes');
    result.steps.push('clientes count='+r2.rows[0].val);
    result.ok = true;
  } catch(e) {
    result.error = e.message;
    result.steps.push('ERROR: '+e.message);
  } finally {
    try{client?.release();}catch{}
  }
  res.json(result);
});

app.get('/api/setup', async (req,res)=>{ await autoSetup(); res.json({ok:true}); });

// ── Limpiar datos cruzados entre empresas ─────────────────────
// GET /api/fix-schemas?key=vef2025
app.get('/api/fix-schemas', async (req,res)=>{
  if(req.query.key!=='vef2025') return res.status(403).json({error:'Clave incorrecta'});
  const log = [];
  try {
    // 1. Obtener todas las empresas
    const emps = await pool.query('SELECT id,nombre,slug FROM public.empresas ORDER BY id');
    log.push(`Empresas: ${emps.rows.map(e=>e.nombre+' ('+e.slug+')').join(', ')}`);
    
    // 2. Para cada empresa, limpiar empresa_config con datos de VEF
    for(const emp of emps.rows){
      const schema = 'emp_'+emp.slug.replace(/[^a-z0-9]/g,'_');
      const client = await pool.connect();
      try {
        await client.query(`SET search_path TO ${schema},public`);
        // Verificar si existe empresa_config
        const cfgCheck = await client.query(`SELECT COUNT(*) cnt FROM empresa_config`);
        if(!cfgCheck.rows[0]?.cnt) { log.push(`${schema}: sin empresa_config`); continue; }
        
        // Obtener config actual
        const cfg = await client.query(`SELECT nombre,email,smtp_user FROM empresa_config LIMIT 1`);
        const cfgRow = cfg.rows[0];
        log.push(`${schema}: nombre="${cfgRow?.nombre}" email="${cfgRow?.email}" smtp="${cfgRow?.smtp_user}"`);
        
        // Si el nombre es VEF y el schema NO es emp_vef, limpiar SMTP de VEF
        if(schema !== 'emp_vef' && (cfgRow?.smtp_user||'').includes('vef-automatizacion')){
          await client.query(`UPDATE empresa_config SET smtp_host=NULL,smtp_port=465,smtp_user=NULL,smtp_pass=NULL WHERE id=(SELECT id FROM empresa_config LIMIT 1)`);
          log.push(`  → SMTP de VEF borrado de ${schema}`);
        }
        // Actualizar nombre si todavía dice "VEF Automatización" y no es emp_vef
        if(schema !== 'emp_vef' && (cfgRow?.nombre||'').includes('VEF Automatización')){
          await client.query(`UPDATE empresa_config SET nombre=$1 WHERE id=(SELECT id FROM empresa_config LIMIT 1)`,[emp.nombre]);
          log.push(`  → Nombre actualizado a "${emp.nombre}" en ${schema}`);
        }
      } catch(e){ log.push(`  ERROR ${schema}: ${e.message}`); }
      finally{ client.release(); }
    }
    
    // 3. Verificar usuarios con schema incorrecto
    const users = await pool.query(`SELECT id,username,empresa_id,schema_name FROM public.usuarios`);
    let fixedUsers = 0;
    for(const u of users.rows){
      if(!u.empresa_id) continue;
      const empR = await pool.query('SELECT slug FROM public.empresas WHERE id=$1',[u.empresa_id]);
      if(!empR.rows[0]) continue;
      const correctSchema = 'emp_'+empR.rows[0].slug.replace(/[^a-z0-9]/g,'_');
      if(u.schema_name !== correctSchema){
        await pool.query('UPDATE public.usuarios SET schema_name=$1 WHERE id=$2',[correctSchema,u.id]);
        fixedUsers++;
        log.push(`Usuario ${u.username}: schema ${u.schema_name} → ${correctSchema}`);
      }
    }
    log.push(`Usuarios con schema corregido: ${fixedUsers}`);
    
    res.json({ok:true, log});
  } catch(e){ res.status(500).json({error:e.message, log}); }
});

/* ─── DIAGNÓSTICO — ver estado real de la BD ──────────────
   GET /api/diagnostico
──────────────────────────────────────────────────────── */
app.get('/api/diagnostico', async (req,res)=>{
  try {
    const schemas   = await pool.query(`SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast') ORDER BY schema_name`);
    const empresas  = await pool.query(`SELECT id,slug,nombre,activa,trial_hasta,suscripcion_estatus FROM empresas`).catch(()=>({rows:[]}));
    const usuarios  = await pool.query(`SELECT id,username,rol,empresa_id,schema_name,password_hash IS NOT NULL has_hash FROM public.usuarios`).catch(()=>({rows:[]}));
    const tablas    = await pool.query(`SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog','pg_toast') ORDER BY table_schema,table_name`);
    const por={}; for(const r of tablas.rows){if(!por[r.table_schema])por[r.table_schema]=[];por[r.table_schema].push(r.table_name);}
    res.json({ schemas:schemas.rows.map(r=>r.schema_name), empresas:empresas.rows,
      usuarios:usuarios.rows, tablas_por_schema:por,
      global_schema:global._defaultSchema, global_empresa_id:global._defaultEmpresaId });
  } catch(e){ res.status(500).json({error:e.message}); }
});

/* ─── FIX TOTAL ──────────────────────────────────────────
   GET /api/fix?key=vef2025
   Muestra estado + fixea todo en un paso
──────────────────────────────────────────────────────── */
app.get('/api/fix', async (req,res)=>{
  if(req.query.key!=='vef2025') return res.status(403).json({error:'Clave incorrecta'});
  const log=[]; const t=Date.now();
  try {
    // PASO 1: Ver estado actual
    const schemas=(await pool.query(`SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast') ORDER BY schema_name`)).rows.map(r=>r.schema_name);
    const publicTbls=(await pool.query(`SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name`)).rows.map(r=>r.table_name);
    const empVefTbls=(await pool.query(`SELECT table_name FROM information_schema.tables WHERE table_schema='emp_vef' ORDER BY table_name`).catch(()=>({rows:[]}))).rows.map(r=>r.table_name);
    log.push('Schemas: '+schemas.join(', '));
    log.push('Public tables: '+publicTbls.join(', ')||'ninguna');
    log.push('emp_vef tables: '+empVefTbls.join(', ')||'ninguna');

    // PASO 2: Columnas actuales de usuarios
    const usrCols=(await pool.query(`SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='usuarios' ORDER BY ordinal_position`).catch(()=>({rows:[]}))).rows.map(r=>r.column_name);
    log.push('usuarios cols: '+usrCols.join(', ')||'tabla no existe');

    // PASO 3: Crear tabla empresas si no existe
    await pool.query(`CREATE TABLE IF NOT EXISTS public.empresas (id SERIAL PRIMARY KEY, slug VARCHAR(50) UNIQUE NOT NULL, nombre VARCHAR(200) NOT NULL, logo TEXT, activa BOOLEAN DEFAULT true, trial_hasta DATE, suscripcion_estatus VARCHAR(30) DEFAULT 'trial', suscripcion_hasta DATE, created_at TIMESTAMP DEFAULT NOW())`);
    for(const[c,d]of[['trial_hasta','DATE'],['suscripcion_estatus',"VARCHAR(30) DEFAULT 'trial'"],['suscripcion_hasta','DATE'],['activa','BOOLEAN DEFAULT true']]){
      try{await pool.query(`ALTER TABLE public.empresas ADD COLUMN IF NOT EXISTS ${c} ${d}`);}catch{}
    }

    // PASO 4: Empresa VEF
    let emp=(await pool.query(`SELECT id,slug FROM public.empresas WHERE slug='vef'`)).rows[0];
    if(!emp){
      emp=(await pool.query(`INSERT INTO public.empresas(slug,nombre,trial_hasta,suscripcion_estatus,activa) VALUES('vef','VEF Automatización',CURRENT_DATE + INTERVAL '30 days','trial',true) RETURNING id,slug`)).rows[0];
      log.push('✅ Empresa VEF creada id='+emp.id);
    } else {
      await pool.query(`UPDATE public.empresas SET trial_hasta=CURRENT_DATE + INTERVAL '30 days',suscripcion_estatus='trial',activa=true WHERE id=$1`,[emp.id]);
      log.push('✅ Empresa VEF trial activado id='+emp.id);
    }
    global._defaultEmpresaId=emp.id;
    global._defaultSchema='emp_vef';

    // PASO 5: Crear tabla usuarios si no existe
    await pool.query(`CREATE TABLE IF NOT EXISTS public.usuarios (id SERIAL PRIMARY KEY, username VARCHAR(100) UNIQUE NOT NULL, nombre VARCHAR(200), password_hash TEXT, rol VARCHAR(30) DEFAULT 'usuario', activo BOOLEAN DEFAULT true, email TEXT, empresa_id INTEGER, schema_name VARCHAR(100), ultimo_acceso TIMESTAMP, created_at TIMESTAMP DEFAULT NOW())`);
    for(const[c,d]of[['password_hash','TEXT'],['activo','BOOLEAN DEFAULT true'],['email','TEXT'],['empresa_id','INTEGER'],['schema_name','VARCHAR(100)'],['nombre','VARCHAR(200)'],['ultimo_acceso','TIMESTAMP']]){
      try{await pool.query(`ALTER TABLE public.usuarios ADD COLUMN IF NOT EXISTS ${c} ${d}`);}catch{}
    }

    // PASO 6: Schema emp_vef con todas las tablas
    await pool.query(`CREATE SCHEMA IF NOT EXISTS emp_vef`);
    const sc=await pool.connect();
    try {
      await sc.query(`SET search_path TO emp_vef`);
      const tbls=[
        `CREATE TABLE IF NOT EXISTS empresa_config (id SERIAL PRIMARY KEY,nombre VARCHAR(200) DEFAULT 'VEF Automatización',razon_social VARCHAR(200),rfc VARCHAR(30),regimen_fiscal VARCHAR(100),contacto VARCHAR(100),telefono VARCHAR(50),email VARCHAR(100),direccion TEXT,ciudad VARCHAR(100),estado VARCHAR(100),cp VARCHAR(10),pais VARCHAR(50) DEFAULT 'México',sitio_web VARCHAR(150),moneda_default VARCHAR(10) DEFAULT 'USD',iva_default NUMERIC(5,2) DEFAULT 16,margen_ganancia NUMERIC(5,2) DEFAULT 0,smtp_host VARCHAR(100),smtp_port INTEGER DEFAULT 465,smtp_user VARCHAR(100),smtp_pass VARCHAR(200),notas_factura TEXT,notas_cotizacion TEXT,updated_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS clientes (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,contacto TEXT,direccion TEXT,telefono TEXT,email TEXT,rfc TEXT,regimen_fiscal TEXT,cp TEXT,ciudad TEXT,activo BOOLEAN DEFAULT true,constancia_pdf BYTEA,constancia_nombre TEXT,constancia_fecha TIMESTAMP,estado_cuenta_pdf BYTEA,estado_cuenta_nombre TEXT,estado_cuenta_fecha TIMESTAMP,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS proveedores (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,contacto TEXT,direccion TEXT,telefono TEXT,email TEXT,rfc TEXT,condiciones_pago TEXT,activo BOOLEAN DEFAULT true,constancia_pdf BYTEA,constancia_nombre TEXT,constancia_fecha TIMESTAMP,estado_cuenta_pdf BYTEA,estado_cuenta_nombre TEXT,estado_cuenta_fecha TIMESTAMP,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS proyectos (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,cliente_id INTEGER,responsable TEXT,fecha_creacion DATE DEFAULT CURRENT_DATE,estatus TEXT DEFAULT 'activo',created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS cotizaciones (id SERIAL PRIMARY KEY,proyecto_id INTEGER,numero_cotizacion TEXT UNIQUE,fecha_emision DATE DEFAULT CURRENT_DATE,validez_hasta DATE,alcance_tecnico TEXT,notas_importantes TEXT,comentarios_generales TEXT,servicio_postventa TEXT,condiciones_entrega TEXT,condiciones_pago TEXT,garantia TEXT,responsabilidad TEXT,validez TEXT,fuerza_mayor TEXT,ley_aplicable TEXT,total NUMERIC(15,2) DEFAULT 0,moneda TEXT DEFAULT 'USD',estatus TEXT DEFAULT 'borrador',created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS items_cotizacion (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,descripcion TEXT,cantidad NUMERIC(10,2),precio_unitario NUMERIC(15,2),total NUMERIC(15,2))`,
        `CREATE TABLE IF NOT EXISTS seguimientos (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,notas TEXT,proxima_accion TEXT)`,
        `CREATE TABLE IF NOT EXISTS facturas (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,numero_factura TEXT,cliente_id INTEGER,moneda TEXT DEFAULT 'USD',subtotal NUMERIC(15,2) DEFAULT 0,iva NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,monto NUMERIC(15,2) DEFAULT 0,fecha_emision DATE DEFAULT CURRENT_DATE,fecha_vencimiento DATE,estatus TEXT DEFAULT 'pendiente',estatus_pago TEXT DEFAULT 'pendiente',notas TEXT,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS pagos (id SERIAL PRIMARY KEY,factura_id INTEGER,fecha DATE DEFAULT CURRENT_DATE,monto NUMERIC(15,2),metodo TEXT,referencia TEXT,notas TEXT,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS ordenes_proveedor (id SERIAL PRIMARY KEY,proveedor_id INTEGER,numero_op TEXT UNIQUE,fecha_emision DATE DEFAULT CURRENT_DATE,fecha_entrega DATE,condiciones_pago TEXT,lugar_entrega TEXT,notas TEXT,subtotal NUMERIC(15,2) DEFAULT 0,iva NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,moneda TEXT DEFAULT 'USD',estatus TEXT DEFAULT 'borrador',cotizacion_ref_pdf TEXT,factura_pdf BYTEA,factura_nombre TEXT,factura_fecha TIMESTAMP,cotizacion_pdf BYTEA,cotizacion_nombre TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS items_orden_proveedor (id SERIAL PRIMARY KEY,orden_id INTEGER,descripcion TEXT,cantidad NUMERIC(10,2),precio_unitario NUMERIC(15,2),total NUMERIC(15,2))`,
        `CREATE TABLE IF NOT EXISTS seguimientos_oc (id SERIAL PRIMARY KEY,orden_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,notas TEXT,proxima_accion TEXT)`,
        `CREATE TABLE IF NOT EXISTS inventario (id SERIAL PRIMARY KEY,codigo TEXT,nombre TEXT NOT NULL,descripcion TEXT,categoria TEXT,unidad TEXT DEFAULT 'pza',cantidad_actual NUMERIC(10,2) DEFAULT 0,cantidad_minima NUMERIC(10,2) DEFAULT 0,stock_actual NUMERIC(10,2) DEFAULT 0,stock_minimo NUMERIC(10,2) DEFAULT 0,precio_costo NUMERIC(15,2) DEFAULT 0,precio_venta NUMERIC(15,2) DEFAULT 0,ubicacion TEXT,proveedor_id INTEGER,foto TEXT,fecha_ultima_entrada DATE,notas TEXT,activo BOOLEAN DEFAULT true,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS movimientos_inventario (id SERIAL PRIMARY KEY,producto_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,cantidad NUMERIC(10,2),stock_anterior NUMERIC(10,2) DEFAULT 0,stock_nuevo NUMERIC(10,2) DEFAULT 0,referencia TEXT,notas TEXT,created_by INTEGER)`,
        `CREATE TABLE IF NOT EXISTS tareas (id SERIAL PRIMARY KEY,titulo VARCHAR(300) NOT NULL,descripcion TEXT,proyecto_id INTEGER,asignado_a INTEGER,creado_por INTEGER,prioridad VARCHAR(20) DEFAULT 'normal',estatus VARCHAR(30) DEFAULT 'pendiente',fecha_inicio DATE,fecha_vencimiento DATE,fecha_completada TIMESTAMP,notas TEXT,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS egresos (id SERIAL PRIMARY KEY,fecha DATE NOT NULL DEFAULT CURRENT_DATE,proveedor_id INTEGER,proveedor_nombre VARCHAR(200),categoria VARCHAR(100),descripcion TEXT,subtotal NUMERIC(15,2) DEFAULT 0,iva NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,metodo VARCHAR(50) DEFAULT 'Transferencia',referencia VARCHAR(100),numero_factura VARCHAR(100),factura_pdf BYTEA,factura_nombre TEXT,notas TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS pdfs_guardados (id SERIAL PRIMARY KEY,tipo VARCHAR(30),referencia_id INTEGER,numero_doc VARCHAR(100),cliente_proveedor VARCHAR(200),ruta_archivo TEXT,nombre_archivo VARCHAR(200),tamanio_bytes INTEGER,pdf_data BYTEA,generado_por INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS reportes_servicio (id SERIAL PRIMARY KEY,numero_reporte VARCHAR(50),titulo VARCHAR(300) NOT NULL,cliente_id INTEGER,proyecto_id INTEGER,fecha_reporte DATE DEFAULT CURRENT_DATE,fecha_servicio DATE,tecnico VARCHAR(200),estatus VARCHAR(30) DEFAULT 'borrador',introduccion TEXT,objetivo TEXT,alcance TEXT,descripcion_sistema TEXT,arquitectura TEXT,desarrollo_tecnico TEXT,resultados_pruebas TEXT,problemas_detectados TEXT,soluciones_implementadas TEXT,conclusiones TEXT,recomendaciones TEXT,anexos TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
      ];
      for(const sql of tbls){try{await sc.query(sql);}catch(e){log.push('⚠ '+e.message.slice(0,60));}}
      const ec=(await sc.query(`SELECT id FROM empresa_config LIMIT 1`)).rows;
      if(!ec.length){
        await sc.query(`INSERT INTO empresa_config(nombre,pais,moneda_default,iva_default) VALUES('VEF Automatización','México','USD',16)`);
        log.push('✅ empresa_config creado en emp_vef');
      }
    } finally {sc.release();}

    // Refresh DB cache
    const{rows:cr}=await pool.query(`SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='emp_vef' ORDER BY table_name,ordinal_position`);
    DB={}; for(const r of cr){if(!DB[r.table_name])DB[r.table_name]=[];DB[r.table_name].push(r.column_name);}
    global.dbSchema=DB;
    log.push('✅ DB cache: '+Object.keys(DB).length+' tablas en emp_vef');

    // PASO 7: Admin user limpio
    const hash=await bcrypt.hash('admin123',10);
    await pool.query(`DELETE FROM public.usuarios WHERE username='admin'`);
    await pool.query(`INSERT INTO public.usuarios(username,nombre,rol,password_hash,password,activo,email,empresa_id,schema_name) VALUES('admin','Administrador','admin',$1,$1,true,'admin@vef.com',$2,'emp_vef')`,[hash,emp.id]);
    log.push('✅ Admin recreado: admin/admin123');

    // Verificar
    const verify=await pool.query(`SELECT id,username,rol,empresa_id,schema_name,(password_hash IS NOT NULL) has_hash FROM public.usuarios WHERE username='admin'`);
    
    res.json({
      ok:true, tiempo_ms:Date.now()-t, log,
      admin: verify.rows[0],
      empresa: emp,
      instrucciones: '👉 Entra con: admin / admin123'
    });
  } catch(e){
    res.status(500).json({error:e.message, log, stack:e.stack?.slice(0,300)});
  }
});

// ================================================================
// AUTO SETUP — se adapta al esquema REAL de la BD
// ================================================================
// AUTO SETUP — se adapta al esquema REAL de la BD
// ================================================================
// AUTO SETUP — se adapta al esquema REAL de la BD
// ================================================================
async function autoSetup() {
  try {
    console.log('\n🔧 VEF ERP — Iniciando setup...');

    // ══════════════════════════════════════════════════════
    // 1. TABLAS GLOBALES en public (usuarios y empresas)
    // ══════════════════════════════════════════════════════
    await pool.query(`CREATE TABLE IF NOT EXISTS public.empresas (
      id SERIAL PRIMARY KEY, slug VARCHAR(50) UNIQUE NOT NULL,
      nombre VARCHAR(200) NOT NULL, logo TEXT, activa BOOLEAN DEFAULT true,
      trial_hasta DATE, suscripcion_estatus VARCHAR(30) DEFAULT 'trial',
      suscripcion_hasta DATE, created_at TIMESTAMP DEFAULT NOW())`);

    await pool.query(`CREATE TABLE IF NOT EXISTS public.usuarios (
      id SERIAL PRIMARY KEY, username VARCHAR(100) UNIQUE NOT NULL,
      nombre VARCHAR(200), password_hash TEXT,
      rol VARCHAR(30) DEFAULT 'usuario', activo BOOLEAN DEFAULT true,
      email TEXT, empresa_id INTEGER, schema_name VARCHAR(100),
      ultimo_acceso TIMESTAMP, created_at TIMESTAMP DEFAULT NOW())`);

    // Columnas extra que pueden faltar
    for(const[c,d]of[['password_hash','TEXT'],['activo','BOOLEAN DEFAULT true'],
      ['email','TEXT'],['empresa_id','INTEGER'],['schema_name','VARCHAR(100)'],
      ['nombre','VARCHAR(200)'],['ultimo_acceso','TIMESTAMP'],
      ['trial_hasta','DATE'],['suscripcion_estatus',"VARCHAR(30) DEFAULT 'trial'"],
      ['suscripcion_hasta','DATE']]) {
      try{ await pool.query(`ALTER TABLE public.empresas ADD COLUMN IF NOT EXISTS ${c} ${d}`); }catch{}
      try{ await pool.query(`ALTER TABLE public.usuarios ADD COLUMN IF NOT EXISTS ${c} ${d}`); }catch{}
    }

    // ══════════════════════════════════════════════════════
    // 2. EMPRESA POR DEFECTO — VEF Automatización
    // ══════════════════════════════════════════════════════
    let emp = (await pool.query(`SELECT id,slug FROM public.empresas WHERE slug='vef'`)).rows[0];
    if(!emp){
      emp=(await pool.query(
        `INSERT INTO public.empresas(slug,nombre,trial_hasta,suscripcion_estatus,activa)
         VALUES('vef','VEF Automatización',CURRENT_DATE + INTERVAL '30 days','trial',true) RETURNING id,slug`
      )).rows[0];
      console.log('  ✅ Empresa VEF creada');
    } else {
      await pool.query(`UPDATE public.empresas SET
        trial_hasta=GREATEST(COALESCE(trial_hasta,CURRENT_DATE),CURRENT_DATE)+30,
        suscripcion_estatus='trial', activa=true WHERE id=$1`,[emp.id]);
    }
    global._defaultEmpresaId = emp.id;
    global._defaultSchema    = 'emp_vef';
    console.log('  🏢 Empresa id='+emp.id+' schema=emp_vef');

    // ══════════════════════════════════════════════════════
    // 3. SCHEMA emp_vef — TODAS las tablas de negocio aquí
    //    Completamente separado de public
    // ══════════════════════════════════════════════════════
    await pool.query(`CREATE SCHEMA IF NOT EXISTS emp_vef`);

    // Usar cliente dedicado con search_path TO emp_vef
    const sc = await pool.connect();
    try {
      await sc.query(`SET search_path TO emp_vef`);
      
      const TBLS = [
        // empresa_config — configuración de esta empresa
        `CREATE TABLE IF NOT EXISTS empresa_config (
          id SERIAL PRIMARY KEY,
          nombre VARCHAR(200) NOT NULL DEFAULT 'VEF Automatización',
          razon_social VARCHAR(200), rfc VARCHAR(30), regimen_fiscal VARCHAR(100),
          contacto VARCHAR(100), telefono VARCHAR(50), email VARCHAR(100),
          direccion TEXT, ciudad VARCHAR(100), estado VARCHAR(100), cp VARCHAR(10),
          pais VARCHAR(50) DEFAULT 'México', sitio_web VARCHAR(150),
          moneda_default VARCHAR(10) DEFAULT 'USD', iva_default NUMERIC(5,2) DEFAULT 16.00,
          margen_ganancia NUMERIC(5,2) DEFAULT 0,
          smtp_host VARCHAR(100), smtp_port INTEGER DEFAULT 465,
          smtp_user VARCHAR(100), smtp_pass VARCHAR(200),
          notas_factura TEXT, notas_cotizacion TEXT,
          updated_at TIMESTAMP DEFAULT NOW())`,
        // Gestión
        `CREATE TABLE IF NOT EXISTS clientes (
          id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
          contacto TEXT, direccion TEXT, telefono TEXT, email TEXT,
          rfc TEXT, regimen_fiscal TEXT, cp TEXT, ciudad TEXT,
          activo BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS proveedores (
          id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
          contacto TEXT, direccion TEXT, telefono TEXT, email TEXT,
          rfc TEXT, condiciones_pago TEXT,
          activo BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS proyectos (
          id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
          cliente_id INTEGER, responsable TEXT,
          fecha_creacion DATE DEFAULT CURRENT_DATE,
          estatus TEXT DEFAULT 'activo', created_at TIMESTAMP DEFAULT NOW())`,
        // Ventas
        `CREATE TABLE IF NOT EXISTS cotizaciones (
          id SERIAL PRIMARY KEY, proyecto_id INTEGER,
          numero_cotizacion TEXT UNIQUE,
          fecha_emision DATE DEFAULT CURRENT_DATE, validez_hasta DATE,
          alcance_tecnico TEXT, notas_importantes TEXT, comentarios_generales TEXT,
          servicio_postventa TEXT, condiciones_entrega TEXT, condiciones_pago TEXT,
          garantia TEXT, responsabilidad TEXT, validez TEXT, fuerza_mayor TEXT,
          ley_aplicable TEXT, total NUMERIC(15,2) DEFAULT 0,
          moneda TEXT DEFAULT 'USD', estatus TEXT DEFAULT 'borrador',
          created_by INTEGER, created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS items_cotizacion (
          id SERIAL PRIMARY KEY, cotizacion_id INTEGER,
          descripcion TEXT, cantidad NUMERIC(10,2),
          precio_unitario NUMERIC(15,2), total NUMERIC(15,2))`,
        `CREATE TABLE IF NOT EXISTS seguimientos (
          id SERIAL PRIMARY KEY, cotizacion_id INTEGER,
          fecha TIMESTAMP DEFAULT NOW(), tipo TEXT,
          notas TEXT, proxima_accion TEXT)`,
        // Facturas
        `CREATE TABLE IF NOT EXISTS facturas (
          id SERIAL PRIMARY KEY, cotizacion_id INTEGER,
          numero_factura TEXT, cliente_id INTEGER,
          moneda TEXT DEFAULT 'USD',
          subtotal NUMERIC(15,2) DEFAULT 0,
          iva NUMERIC(15,2) DEFAULT 0,
          total NUMERIC(15,2) DEFAULT 0,
          monto NUMERIC(15,2) DEFAULT 0,
          fecha_emision DATE DEFAULT CURRENT_DATE,
          fecha_vencimiento DATE,
          estatus TEXT DEFAULT 'pendiente',
          estatus_pago TEXT DEFAULT 'pendiente',
          notas TEXT, created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS pagos (
          id SERIAL PRIMARY KEY, factura_id INTEGER,
          fecha DATE DEFAULT CURRENT_DATE, monto NUMERIC(15,2),
          metodo TEXT, referencia TEXT, notas TEXT,
          created_at TIMESTAMP DEFAULT NOW())`,
        // Compras
        `CREATE TABLE IF NOT EXISTS ordenes_proveedor (
          id SERIAL PRIMARY KEY, proveedor_id INTEGER,
          numero_op TEXT UNIQUE,
          fecha_emision DATE DEFAULT CURRENT_DATE, fecha_entrega DATE,
          condiciones_pago TEXT, lugar_entrega TEXT, notas TEXT,
          total NUMERIC(15,2) DEFAULT 0, moneda TEXT DEFAULT 'USD',
          estatus TEXT DEFAULT 'borrador',
          cotizacion_ref_pdf TEXT,
          factura_pdf BYTEA, factura_nombre TEXT, factura_fecha TIMESTAMP,
          cotizacion_pdf BYTEA, cotizacion_nombre TEXT,
          created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS items_orden_proveedor (
          id SERIAL PRIMARY KEY, orden_id INTEGER,
          descripcion TEXT, cantidad NUMERIC(10,2),
          precio_unitario NUMERIC(15,2), total NUMERIC(15,2))`,
        `CREATE TABLE IF NOT EXISTS seguimientos_oc (
          id SERIAL PRIMARY KEY, orden_id INTEGER,
          fecha TIMESTAMP DEFAULT NOW(), tipo TEXT,
          notas TEXT, proxima_accion TEXT)`,
        // Inventario
        `CREATE TABLE IF NOT EXISTS inventario (
          id SERIAL PRIMARY KEY, codigo TEXT, nombre TEXT NOT NULL,
          descripcion TEXT, categoria TEXT, unidad TEXT DEFAULT 'pza',
          cantidad_actual NUMERIC(10,2) DEFAULT 0,
          cantidad_minima NUMERIC(10,2) DEFAULT 0,
          stock_actual NUMERIC(10,2) DEFAULT 0,
          stock_minimo NUMERIC(10,2) DEFAULT 0,
          precio_costo NUMERIC(15,2) DEFAULT 0,
          precio_venta NUMERIC(15,2) DEFAULT 0,
          ubicacion TEXT, proveedor_id INTEGER, foto TEXT,
          fecha_ultima_entrada DATE, notas TEXT,
          activo BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS movimientos_inventario (
          id SERIAL PRIMARY KEY, producto_id INTEGER,
          fecha TIMESTAMP DEFAULT NOW(), tipo TEXT,
          cantidad NUMERIC(10,2),
          stock_anterior NUMERIC(10,2) DEFAULT 0,
          stock_nuevo NUMERIC(10,2) DEFAULT 0,
          referencia TEXT, notas TEXT, created_by INTEGER)`,
        // Tareas
        `CREATE TABLE IF NOT EXISTS tareas (
          id SERIAL PRIMARY KEY, titulo VARCHAR(300) NOT NULL,
          descripcion TEXT, proyecto_id INTEGER,
          asignado_a INTEGER, creado_por INTEGER,
          prioridad VARCHAR(20) DEFAULT 'normal',
          estatus VARCHAR(30) DEFAULT 'pendiente',
          fecha_inicio DATE, fecha_vencimiento DATE,
          fecha_completada TIMESTAMP, notas TEXT,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW())`,
        // Egresos
        `CREATE TABLE IF NOT EXISTS egresos (
          id SERIAL PRIMARY KEY,
          fecha DATE NOT NULL DEFAULT CURRENT_DATE,
          proveedor_id INTEGER, proveedor_nombre VARCHAR(200),
          categoria VARCHAR(100), descripcion TEXT,
          subtotal NUMERIC(15,2) DEFAULT 0,
          iva NUMERIC(15,2) DEFAULT 0,
          total NUMERIC(15,2) DEFAULT 0,
          metodo VARCHAR(50) DEFAULT 'Transferencia',
          referencia VARCHAR(100), numero_factura VARCHAR(100),
          factura_pdf BYTEA, factura_nombre TEXT,
          notas TEXT, created_by INTEGER,
          created_at TIMESTAMP DEFAULT NOW())`,
        // PDFs
        `CREATE TABLE IF NOT EXISTS pdfs_guardados (
          id SERIAL PRIMARY KEY, tipo VARCHAR(30),
          referencia_id INTEGER, numero_doc VARCHAR(100),
          cliente_proveedor VARCHAR(200), ruta_archivo TEXT,
          nombre_archivo VARCHAR(200), tamanio_bytes INTEGER,
          pdf_data BYTEA, generado_por INTEGER,
          created_at TIMESTAMP DEFAULT NOW())`,
      ];

      for(const sql of TBLS){
        try{ await sc.query(sql); }
        catch(e){ console.log('  ⚠ tabla:', e.message.slice(0,80)); }
      }

      // Agregar columnas faltantes a inventario (compatibilidad con BD existente)
      const invAlters = [
        "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS fecha_ultima_entrada DATE",
        "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS foto TEXT",
        "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS notas TEXT",
        "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS activo BOOLEAN DEFAULT true",
        "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS stock_actual NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS stock_minimo NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS rfc TEXT",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS regimen_fiscal TEXT",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS cp TEXT",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS ciudad TEXT",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS activo BOOLEAN DEFAULT true",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS tipo_persona VARCHAR(10) DEFAULT 'moral'",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS constancia_pdf BYTEA",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS constancia_nombre TEXT",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS constancia_fecha TIMESTAMP",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS estado_cuenta_pdf BYTEA",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS estado_cuenta_nombre TEXT",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS estado_cuenta_fecha TIMESTAMP",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS tipo_persona VARCHAR(10) DEFAULT 'fisica'",
        "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS tipo_persona VARCHAR(10) DEFAULT 'fisica'",
        "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS tipo_persona VARCHAR(10) DEFAULT 'moral'",
        "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS constancia_pdf BYTEA",
        "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS constancia_nombre TEXT",
        "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS constancia_fecha TIMESTAMP",
        "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS estado_cuenta_pdf BYTEA",
        "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS estado_cuenta_nombre TEXT",
        "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS estado_cuenta_fecha TIMESTAMP",
        "ALTER TABLE cotizaciones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()",
        "ALTER TABLE cotizaciones ADD COLUMN IF NOT EXISTS created_by INTEGER",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS cliente_id INTEGER",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS subtotal NUMERIC(15,2) DEFAULT 0",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS iva NUMERIC(15,2) DEFAULT 0",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS retencion_isr NUMERIC(15,2) DEFAULT 0",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS retencion_iva NUMERIC(15,2) DEFAULT 0",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS iva NUMERIC(15,2) DEFAULT 0",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS monto NUMERIC(15,2) DEFAULT 0",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS fecha_vencimiento DATE",
        "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS estatus_pago TEXT DEFAULT 'pendiente'",
        "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS subtotal NUMERIC(15,2) DEFAULT 0",
        "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS iva NUMERIC(15,2) DEFAULT 0",
        "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS factura_pdf BYTEA",
        "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS factura_nombre TEXT",
        "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS factura_fecha TIMESTAMP",
        "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS cotizacion_pdf BYTEA",
        "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS cotizacion_nombre TEXT",
        "ALTER TABLE tareas ADD COLUMN IF NOT EXISTS fecha_completada TIMESTAMP",
        `CREATE TABLE IF NOT EXISTS sat_solicitudes (
          id SERIAL PRIMARY KEY,
          id_solicitud VARCHAR(100) UNIQUE,
          fecha_inicio DATE, fecha_fin DATE,
          tipo VARCHAR(20) DEFAULT 'CFDI',
          estatus VARCHAR(30) DEFAULT 'pendiente',
          paquetes TEXT,
          created_by INTEGER,
          created_at TIMESTAMP DEFAULT NOW()
        )`,
        `CREATE TABLE IF NOT EXISTS sat_cfdis (
          id SERIAL PRIMARY KEY,
          uuid VARCHAR(100) UNIQUE,
          fecha_cfdi TIMESTAMP,
          tipo_comprobante VARCHAR(5),
          subtotal NUMERIC(15,2) DEFAULT 0,
          total NUMERIC(15,2) DEFAULT 0,
          moneda VARCHAR(10) DEFAULT 'MXN',
          emisor_rfc VARCHAR(20),
          emisor_nombre VARCHAR(300),
          receptor_rfc VARCHAR(20),
          receptor_nombre VARCHAR(300),
          uso_cfdi VARCHAR(10),
          xml_content TEXT,
          id_paquete VARCHAR(200),
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        )`,
        "CREATE TABLE IF NOT EXISTS reportes_servicio (id SERIAL PRIMARY KEY,numero_reporte VARCHAR(50),titulo VARCHAR(300) NOT NULL,cliente_id INTEGER,proyecto_id INTEGER,fecha_reporte DATE DEFAULT CURRENT_DATE,fecha_servicio DATE,tecnico VARCHAR(200),estatus VARCHAR(30) DEFAULT 'borrador',introduccion TEXT,objetivo TEXT,alcance TEXT,descripcion_sistema TEXT,arquitectura TEXT,desarrollo_tecnico TEXT,resultados_pruebas TEXT,problemas_detectados TEXT,soluciones_implementadas TEXT,conclusiones TEXT,recomendaciones TEXT,anexos TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())",
        "ALTER TABLE tareas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()",
        "ALTER TABLE egresos ADD COLUMN IF NOT EXISTS proveedor_id INTEGER",
        "ALTER TABLE egresos ADD COLUMN IF NOT EXISTS factura_pdf BYTEA",
        "ALTER TABLE egresos ADD COLUMN IF NOT EXISTS factura_nombre TEXT",
      ];
      for(const sql of invAlters){
        try{ await sc.query(sql); } catch(e){ /* columna ya existe */ }
      }
      console.log('  ✅ Columnas verificadas/agregadas');

      // empresa_config con datos reales de VEF
      const ec = await sc.query(`SELECT id FROM empresa_config LIMIT 1`);
      if(!ec.rows.length){
        await sc.query(`INSERT INTO empresa_config
          (nombre,razon_social,telefono,email,ciudad,estado,pais,moneda_default,iva_default)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          ['VEF Automatización','VEF Automatización S.A. de C.V.',
           '+52 (722) 115-7792','soporte.ventas@vef-automatizacion.com',
           'Toluca','Estado de México','México','USD',16.00]);
        console.log('  ✅ empresa_config creado');
      }
    } finally { sc.release(); }

    // ══════════════════════════════════════════════════════
    // 4. Leer columnas de emp_vef para has()
    // ══════════════════════════════════════════════════════
    const {rows:colRows} = await pool.query(`
      SELECT table_name, column_name
      FROM information_schema.columns
      WHERE table_schema = 'emp_vef'
      ORDER BY table_name, ordinal_position`);
    DB = {};
    for(const r of colRows){
      if(!DB[r.table_name]) DB[r.table_name] = [];
      DB[r.table_name].push(r.column_name);
    }
    global.dbSchema = DB;
    const tblNames = Object.keys(DB);
    console.log('  📦 emp_vef tablas ('+tblNames.length+'):', tblNames.join(', '));

    // ══════════════════════════════════════════════════════
    // 5. USUARIOS por defecto — en public, empresa=emp_vef
    // ══════════════════════════════════════════════════════
    const USERS=[
      {u:'admin',    n:'Administrador',       r:'admin',   p:'admin123'},
      {u:'ventas',   n:'Ejecutivo de Ventas', r:'ventas',  p:'ventas123'},
      {u:'compras',  n:'Agente de Compras',   r:'compras', p:'compras123'},
      {u:'almacen',  n:'Encargado Almacén',   r:'almacen', p:'almacen123'},
      {u:'gerencia', n:'Gerencia General',    r:'admin',   p:'gerencia123'},
    ];
    for(const u of USERS){
      try {
        const hash = await bcrypt.hash(u.p, 10);
        const ex = await pool.query(`SELECT id FROM public.usuarios WHERE username=$1`,[u.u]);
        if(!ex.length){
          await pool.query(
            `INSERT INTO public.usuarios(username,nombre,rol,password_hash,password,activo,email,empresa_id,schema_name)
             VALUES($1,$2,$3,$4,$4,true,$5,$6,'emp_vef')`,
            [u.u, u.n, u.r, hash, u.u+'@vef.com', global._defaultEmpresaId]);
          console.log('  ✅ '+u.u+' / '+u.p);
        } else {
          await pool.query(
            `UPDATE public.usuarios SET password_hash=$1, password=$1, rol=$2,
             empresa_id=COALESCE(empresa_id,$3),
             schema_name=COALESCE(NULLIF(schema_name,''),'emp_vef')
             WHERE username=$4`,
            [hash, u.r, global._defaultEmpresaId, u.u]);
          console.log('  🔄 '+u.u+' actualizado');
        }
      } catch(e){ console.error('  ⚠ usuario '+u.u+':', e.message); }
    }

    clearColCache(); // Invalidar cache de columnas tras setup
    console.log('\n✅ Setup VEF ERP completo');
    console.log('   Empresa: VEF Automatización → schema: emp_vef');
    console.log('   Login: admin / admin123');
    console.log('');

  } catch(e){
    console.error('\n❌ Setup FATAL:', e.message);
    console.error(e.stack?.slice(0,400));
  }
}

// ── Helper: columnas seguras para SELECT ─────────────────────────
// Construye SELECT * pero omite columnas que no existen
function safeSelect(table, alias='') {
  const a = alias ? alias+'.' : '';
  // Devuelve * si no tenemos el esquema todavía
  return `${a}*`;
}

// ================================================================
// AUTH
// ================================================================
// ── Registro público: crear cuenta + empresa ─────────────
app.post('/api/registro', async (req,res)=>{
  try {
    const {nombre,apellido,email,password,empresa_nombre,telefono} = req.body;
    if(!nombre||!email||!password||!empresa_nombre)
      return res.status(400).json({error:'Todos los campos son requeridos'});
    if(password.length<8) return res.status(400).json({error:'La contraseña debe tener mínimo 8 caracteres'});
    // Verificar email único
    const existing = await pool.query('SELECT id FROM usuarios WHERE username=$1 OR email=$1',[email]);
    if(existing.rows.length>0) return res.status(400).json({error:'Este email ya está registrado'});
    // Generar slug único para la empresa
    let baseSlug = empresa_nombre.toLowerCase()
      .normalize('NFD').replace(/[̀-ͯ]/g,'')
      .replace(/[^a-z0-9]/g,'_').replace(/_+/g,'_').slice(0,30);
    let slug = baseSlug, n=2;
    while((await pool.query('SELECT id FROM empresas WHERE slug=$1',[slug])).rows.length>0){
      slug=baseSlug+'_'+n++; if(n>99) slug=baseSlug+'_'+Date.now();
    }
    // Crear empresa con trial 30 días
    const trialHasta = new Date(); trialHasta.setDate(trialHasta.getDate()+30);
    const emp = await pool.query(
      `INSERT INTO empresas (slug,nombre,trial_hasta,suscripcion_estatus) VALUES ($1,$2,$3,'trial') RETURNING *`,
      [slug, empresa_nombre, trialHasta.toISOString().slice(0,10)]);
    const empId = emp[0]?.id;
    // Crear schema de la empresa
    const schema = await crearSchemaEmpresa(slug, empresa_nombre);
    // Crear usuario admin de la empresa
    const hash = await bcrypt.hash(password,12);
    const fullName = (nombre+' '+apellido).trim();
    const usr = await pool.query(
      `INSERT INTO usuarios (username,nombre,email,password_hash,password,rol,empresa_id,schema_name)
       VALUES ($1,$2,$3,$4,$4,$5,$6,$7) RETURNING id,username,nombre,email,rol`,
      [email, fullName, email, hash, 'admin', empId, schema]);
    // Actualizar empresa_config en el nuevo schema
    const client = await pool.connect();
    try {
      await client.query(`SET search_path TO "${schema}", public`);
      await client.query(`UPDATE empresa_config SET nombre=$1,email=$2,telefono=$3`,[empresa_nombre,email,telefono||'']);
    } finally { client.release(); }
    console.log('✅ Nuevo registro:', email, '→', schema, '→ trial hasta', trialHasta.toISOString().slice(0,10));
    res.status(201).json({
      ok:true,
      mensaje:'Cuenta creada exitosamente. Trial de 30 días activo.',
      empresa:{id:empId,nombre:empresa_nombre,slug,trial_hasta:trialHasta.toISOString().slice(0,10)},
      usuario:{id:usr[0]?.id,nombre:fullName,email}
    });
  } catch(e){ console.error('Registro error:',e.message); res.status(500).json({error:e.message}); }
});

// ── Verificar suscripción en login ────────────────────────
async function checkSuscripcion(empId) {
  if(!empId) return {ok:true};
  const rows = await pool.query('SELECT trial_hasta,suscripcion_estatus,suscripcion_hasta FROM empresas WHERE id=$1',[empId]);
  if(!rows.rows.length) return {ok:true};
  const e = rows.rows[0];
  const hoy = new Date();
  if(e.suscripcion_estatus==='activa' && e.suscripcion_hasta && new Date(e.suscripcion_hasta)>=hoy) return {ok:true,estatus:'activa'};
  if(e.suscripcion_estatus==='trial' && e.trial_hasta && new Date(e.trial_hasta)>=hoy){
    const diasRestantes = Math.ceil((new Date(e.trial_hasta)-hoy)/86400000);
    return {ok:true, estatus:'trial', dias_restantes:diasRestantes, trial_hasta:e.trial_hasta};
  }
  if(e.suscripcion_estatus==='trial' && e.trial_hasta && new Date(e.trial_hasta)<hoy)
    return {ok:false, estatus:'trial_vencido', trial_hasta:e.trial_hasta};
  return {ok:false, estatus:'inactiva'};
}

// ── Listar empresas disponibles para un usuario ──────────
app.get('/api/empresas', async (req,res)=>{
  try {
    // Sin auth: listar empresas activas para mostrar en login
    const rows = await pool.query('SELECT id,slug,nombre,logo FROM empresas WHERE activa=true ORDER BY nombre');
    res.json(rows.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ── Admin: CRUD empresas ──────────────────────────────────
app.post('/api/empresas', auth, async (req,res)=>{
  if(req.user.rol!=='admin') return res.status(403).json({error:'Solo admin'});
  try {
    const {nombre,slug} = req.body;
    if(!nombre||!slug) return res.status(400).json({error:'nombre y slug requeridos'});
    const cleanSlug = slug.toLowerCase().replace(/[^a-z0-9_]/g,'_');
    const emp = await pool.query(`INSERT INTO empresas (slug,nombre) VALUES ($1,$2) RETURNING *`,[cleanSlug,nombre]);
    const schema = await crearSchemaEmpresa(cleanSlug, nombre);
    // Dar acceso admin al creador
    await pool.query(`INSERT INTO usuario_empresa (usuario_id,empresa_id,rol) VALUES ($1,$2,'admin') ON CONFLICT DO NOTHING`,[req.user.id,emp.rows[0].id]);
    res.status(201).json({...emp[0], schema});
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.put('/api/empresas/:id', auth, async (req,res)=>{
  if(req.user.rol!=='admin') return res.status(403).json({error:'Solo admin'});
  try {
    const {nombre,activa,suscripcion_estatus,dias_suscripcion} = req.body;
    let extraSQL='', extraVals=[];
    if(suscripcion_estatus==='activa'&&dias_suscripcion){
      const hasta=new Date(); hasta.setDate(hasta.getDate()+parseInt(dias_suscripcion||30));
      extraSQL=`,suscripcion_estatus='activa',suscripcion_hasta='${hasta.toISOString().slice(0,10)}'`;
    } else if(suscripcion_estatus) {
      extraSQL=`,suscripcion_estatus='${suscripcion_estatus}'`;
    }
    const r = await pool.query(
      `UPDATE empresas SET nombre=COALESCE($1,nombre),activa=COALESCE($2,activa)${extraSQL} WHERE id=$3 RETURNING *`,
      [nombre,activa,req.params.id]);
    res.json(r[0]);
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ── Login con selección de empresa ───────────────────────
// ── Admin: Ver datos por schema ─────────────────────────────────
app.get('/api/admin/schema-data', async (req,res)=>{
  if(req.query.key!=='vef2025') return res.status(403).json({error:'Clave requerida'});
  try{
    const schemas = await pool.query(`
      SELECT e.id, e.nombre, e.slug, 'emp_'||e.slug as schema_name,
        (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='emp_'||e.slug) tablas
      FROM public.empresas e ORDER BY e.id`);
    const result = {};
    for(const emp of schemas.rows){
      const sch = emp.schema_name.replace(/[^a-z0-9_]/g,'');
      const counts = {};
      for(const tbl of ['clientes','proveedores','cotizaciones','facturas','inventario','egresos','proyectos','tareas']){
        try{
          const r = await pool.query(`SELECT COUNT(*) cnt FROM "${sch}".${tbl}`);
          counts[tbl] = parseInt(r.rows[0].cnt);
        }catch{ counts[tbl]=0; }
      }
      result[emp.nombre] = {schema:sch, counts};
    }
    res.json(result);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Admin: Limpiar schema de una empresa (PELIGROSO) ─────────────
app.delete('/api/admin/schema-data/:empresa_id', async (req,res)=>{
  if(req.query.key!=='vef2025') return res.status(403).json({error:'Clave requerida'});
  const tablas=['cotizaciones','items_cotizacion','seguimientos','facturas','pagos',
    'ordenes_proveedor','items_orden_proveedor','seguimientos_oc','clientes','proveedores',
    'proyectos','inventario','movimientos_inventario','tareas','egresos','pdfs_guardados',
    'reportes_servicio','empresa_config'];
  try{
    const emp = await pool.query('SELECT slug FROM public.empresas WHERE id=$1',[req.params.empresa_id]);
    if(!emp.rows[0]) return res.status(404).json({error:'Empresa no encontrada'});
    const sch = 'emp_'+emp.rows[0].slug.replace(/[^a-z0-9]/g,'_');
    const deleted = {};
    for(const t of tablas){
      try{
        const r = await pool.query(`DELETE FROM "${sch}".${t}`);
        deleted[t] = r.rowCount;
      }catch(e){ deleted[t]='skip:'+e.message.slice(0,30); }
    }
    // Reset empresa_config to defaults
    try{
      await pool.query(`INSERT INTO "${sch}".empresa_config(nombre) VALUES($1) ON CONFLICT DO NOTHING`,[emp.rows[0].slug]);
    }catch{}
    res.json({ok:true, schema:sch, deleted});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── DEBUG LOGIN ───────────────────────────────────────────────── — ver exactamente por qué falla ────────────
// GET /api/debug-login?user=EMAIL&key=vef2025
app.get('/api/debug-login', async (req,res)=>{
  if(req.query.key!=='vef2025') return res.status(403).json({error:'Clave incorrecta'});
  const username = req.query.user||'admin';
  try{
    // Buscar usuario
    let r1 = await pool.query('SELECT id,username,email,rol,activo,empresa_id,schema_name FROM public.usuarios WHERE username=$1',[username]);
    if(!r1.rows.length) r1 = await pool.query('SELECT id,username,email,rol,activo,empresa_id,schema_name FROM public.usuarios WHERE email=$1',[username]);
    if(!r1.rows.length) return res.json({found:false, msg:'Usuario no encontrado'});
    
    const u = r1.rows[0];
    // Ver hashes
    const r2 = await pool.query('SELECT length(password_hash::text) len_hash, left(password_hash::text,10) hash_preview, length(password::text) len_pass FROM public.usuarios WHERE id=$1',[u.id]).catch(()=>({rows:[{}]}));
    const hashInfo = r2.rows[0]||{};
    
    res.json({
      found: true,
      usuario: u,
      hash_info: hashInfo,
      msg: hashInfo.len_hash>0 ? 'Tiene password_hash' : 'NO tiene password_hash - solo password columna'
    });
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/auth/login', async (req,res) => {
  const {username,password,empresa_id}=req.body;
  if(!username||!password) return res.status(400).json({error:'Usuario y contraseña requeridos'});
  try {
    // Buscar por username O por email
    let result = await pool.query('SELECT * FROM public.usuarios WHERE username=$1',[username]);
    if(!result.rows.length){
      result = await pool.query('SELECT * FROM public.usuarios WHERE email=$1',[username]).catch(()=>({rows:[]}));
    }
    const user = result.rows[0];
    if(!user) return res.status(401).json({error:'Usuario no encontrado'});
    if(user.activo===false) return res.status(401).json({error:'Usuario desactivado'});

    // Verificar contraseña — revisar TODAS las columnas posibles
    const hash = user.password_hash || user.password || user.contrasena || '';
    if(!hash) return res.status(401).json({error:'Sin contraseña configurada. Ejecuta /api/fix?key=vef2025'});
    const passOk = await bcrypt.compare(password, hash);
    if(!passOk) return res.status(401).json({error:'Contraseña incorrecta'});

    // Actualizar último acceso
    try { await pool.query('UPDATE public.usuarios SET ultimo_acceso=NOW() WHERE id=$1',[user.id]); } catch{}

    // Empresa del usuario — siempre derivar schema del slug en la BD
    let empId = user.empresa_id || global._defaultEmpresaId;
    const empRow = await pool.query('SELECT nombre,slug FROM public.empresas WHERE id=$1 LIMIT 1',[empId]);
    const empSlug = empRow.rows[0]?.slug || 'vef';
    const empNombre = empRow.rows[0]?.nombre || 'VEF Automatización';
    // Schema siempre derivado del slug — ignora schema_name que puede estar corrupto
    let schema = 'emp_' + empSlug.replace(/[^a-z0-9]/g,'_');
    // Fallback al schema_name del usuario si no hay empresa
    if(!empRow.rows[0]) schema = user.schema_name || global._defaultSchema || 'emp_vef';
    // Actualizar schema_name del usuario si está desactualizado
    if(user.schema_name !== schema){
      try { await pool.query('UPDATE public.usuarios SET schema_name=$1,empresa_id=$2 WHERE id=$3',[schema,empId,user.id]); } catch{}
    }

    // Verificar suscripción
    if(!empId){
      const token=jwt.sign({id:user.id,username:user.username,nombre:user.nombre,
        rol:user.rol||'usuario',empresa_id:null,schema:'public',empresa_nombre:'Sistema'},JWT_SECRET,{expiresIn:'8h'});
      return res.json({token,user:{id:user.id,nombre:user.nombre,username:user.username,rol:user.rol||'usuario'}});
    }
    // Admin siempre puede entrar (para evitar quedar bloqueado del sistema)
    let susc = {ok:true, estatus:'activa'};
    if(user.rol !== 'admin'){
      susc = await checkSuscripcion(empId);
      if(!susc.ok){
        return res.status(402).json({
          error: susc.estatus==='trial_vencido'
            ? 'Tu período de prueba venció. Contacta al administrador.'
            : 'Cuenta inactiva. Contacta al administrador.',
          estatus: susc.estatus,
          requiere_pago: true
        });
      }
    }
    const token=jwt.sign({
      id:user.id, username:user.username, nombre:user.nombre,
      rol:user.rol||'usuario', empresa_id:empId, schema, empresa_nombre:empNombre,
      trial: susc.estatus==='trial', dias_restantes: susc.dias_restantes
    },JWT_SECRET,{expiresIn:'8h'});
    res.json({token, user:{id:user.id,nombre:user.nombre,username:user.username,
      rol:user.rol||'usuario', empresa_id:empId, empresa_nombre:empNombre, schema,
      trial: susc.estatus==='trial', dias_restantes: susc.dias_restantes}});
  } catch(e){ res.status(500).json({error:'Error: '+e.message}); }
});

// Verificar token — usado por app.html al cargar para validar sesión activa
app.get('/api/auth/verify', auth, async (req,res)=>{
  try {
    // Devolver datos frescos desde la BD para garantizar rol correcto
    const fresh = await pool.query(
      'SELECT id,username,nombre,email,rol,activo,empresa_id,schema_name FROM public.usuarios WHERE id=$1',
      [req.user.id]);
    if(!fresh.rows.length) return res.status(401).json({error:'Usuario no encontrado'});
    const u = fresh.rows[0];
    if(u.activo===false) return res.status(401).json({error:'Usuario desactivado'});
    // Obtener nombre de empresa
    // Siempre obtener nombre fresco de la BD
    let empresa_nombre = '';
    let empresa_schema = u.schema_name || global._defaultSchema || 'emp_vef';
    if(u.empresa_id){
      const empR = await pool.query('SELECT nombre,slug FROM public.empresas WHERE id=$1',[u.empresa_id]);
      empresa_nombre = empR.rows[0]?.nombre || '';
      // Asegurar schema correcto basado en slug
      if(empR.rows[0]?.slug && !u.schema_name){
        empresa_schema = 'emp_' + empR.rows[0].slug.replace(/[^a-z0-9]/g,'_');
      }
    }
    empresa_nombre = empresa_nombre || req.user.empresa_nombre || 'VEF Automatización';
    res.json({ ok:true, user:{
      id:u.id, username:u.username, nombre:u.nombre,
      email:u.email, rol:u.rol||'usuario',
      empresa_id:u.empresa_id, schema_name:empresa_schema,
      empresa_nombre, schema:empresa_schema,
      trial:req.user.trial, dias_restantes:req.user.dias_restantes
    }});
  } catch(e){ res.json({ ok:true, user: req.user }); }
});

app.post('/api/auth/change-password', auth, async (req,res)=>{
  try {
    const {password_actual,password_nuevo}=req.body;
    const rows=await pool.query('SELECT * FROM public.usuarios WHERE id=$1',[req.user.id]);
    const u=rows.rows[0];
    if (!u) return res.status(404).json({error:'Usuario no encontrado'});
    const h=u.password_hash||u.password||u.contrasena||'';
    if (!await bcrypt.compare(password_actual,h)) return res.status(401).json({error:'Contraseña actual incorrecta'});
    const newHash=await bcrypt.hash(password_nuevo,12);
    const csets=[];const cvals=[];let ci=1;
    const cpCols=await pool.query(`SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='usuarios' AND column_name IN ('password_hash','password','contrasena')`);
    const cpColList=cpCols.rows.map(r=>r.column_name);
    if(cpColList.includes('password_hash')){csets.push(`password_hash=$${ci++}`);cvals.push(newHash);}
    if(cpColList.includes('password'))     {csets.push(`password=$${ci++}`);     cvals.push(newHash);}
    if(cpColList.includes('contrasena'))   {csets.push(`contrasena=$${ci++}`);   cvals.push(newHash);}
    if(!csets.length) return res.status(500).json({error:'No se encontró columna de contraseña'});
    cvals.push(req.user.id);
    await pool.query(`UPDATE public.usuarios SET ${csets.join(',')} WHERE id=$${ci}`,cvals);
    res.json({ok:true});
  } catch(e){ 
    console.error('change-password:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

// ================================================================
// DASHBOARD
// ================================================================
app.get('/api/dashboard/metrics', auth, async (req,res)=>{
  // Schema del usuario — siempre del JWT que viene del login
  const schema = (req.user?.schema || req.user?.schema_name || global._defaultSchema || 'emp_vef').replace(/["']/g,'');
  const mes_actual = new Date().toLocaleDateString('es-MX',{month:'long',year:'numeric'});
  const base = {cotizaciones_activas:0,clientes:0,proveedores:0,items_inventario:0,
    facturas_pendientes:0,tareas_pendientes:0,ing_mes:0,egr_mes:0,cob_mes:0,
    empresa:{nombre:'VEF Automatización'},cot_recientes:[],fac_vencer:[],inv_bajo:[],mes_actual};

  // Timeout duro 9s
  const tid = setTimeout(()=>{ if(!res.headersSent) res.json({...base,_warn:'timeout'}); },9000);
  res.on('finish',()=>clearTimeout(tid));

  let client;
  try { client = await pool.connect(); } 
  catch(e){ return res.json({...base,_warn:'no_conn:'+e.message}); }

  try {
    // Aplicar schema sin comillas dobles
    await client.query('SET search_path TO '+schema+',public');

    const n = async(sql)=>{ try{return parseInt((await client.query(sql)).rows[0]?.val||0);}catch{return 0;}};
    const s = async(sql,p)=>{ try{return parseFloat((await client.query(sql,p)).rows[0]?.val||0);}catch{return 0;}};
    const q = async(sql,p=[])=>{ try{return (await client.query(sql,p)).rows;}catch{return [];}};

    const M=new Date().getMonth()+1, Y=new Date().getFullYear();

    const emp    = (await q('SELECT * FROM empresa_config LIMIT 1'))[0]||{};
    const cots   = await n('SELECT COUNT(*) val FROM cotizaciones');
    const clts   = await n('SELECT COUNT(*) val FROM clientes');
    const provs  = await n('SELECT COUNT(*) val FROM proveedores');
    const prods  = await n('SELECT COUNT(DISTINCT nombre) val FROM inventario WHERE COALESCE(activo,true)=true');
    const facts  = await n("SELECT COUNT(*) val FROM facturas WHERE estatus IN ('pendiente','parcial')");
    const tar    = await n("SELECT COUNT(*) val FROM tareas WHERE estatus!='completada'");
    const ing    = await s('SELECT COALESCE(SUM(total),0) val FROM facturas WHERE EXTRACT(MONTH FROM fecha_emision)=$1 AND EXTRACT(YEAR FROM fecha_emision)=$2',[M,Y]);
    const ivaCol = await n("SELECT 1 FROM information_schema.columns WHERE table_schema='"+schema+"' AND table_name='facturas' AND column_name='iva' LIMIT 1");
    const isrCol = await n("SELECT 1 FROM information_schema.columns WHERE table_schema='"+schema+"' AND table_name='facturas' AND column_name='retencion_isr' LIMIT 1");
    const ivaFac = ivaCol ? await s('SELECT COALESCE(SUM(iva),0) val FROM facturas WHERE EXTRACT(MONTH FROM fecha_emision)=$1 AND EXTRACT(YEAR FROM fecha_emision)=$2',[M,Y]) : 0;
    const isrFac = isrCol ? await s('SELECT COALESCE(SUM(retencion_isr),0) val FROM facturas WHERE EXTRACT(MONTH FROM fecha_emision)=$1 AND EXTRACT(YEAR FROM fecha_emision)=$2',[M,Y]) : 0;
    const cob    = await s('SELECT COALESCE(SUM(monto),0) val FROM pagos WHERE EXTRACT(MONTH FROM fecha)=$1 AND EXTRACT(YEAR FROM fecha)=$2',[M,Y]);
    // Egresos = egresos directos + OC proveedores aprobadas del mes
    const egrDir = await s('SELECT COALESCE(SUM(total),0) val FROM egresos WHERE EXTRACT(MONTH FROM fecha)=$1 AND EXTRACT(YEAR FROM fecha)=$2',[M,Y]);
    const egrOC  = await s("SELECT COALESCE(SUM(total),0) val FROM ordenes_proveedor WHERE estatus IN ('aprobada','recibida','pagada') AND EXTRACT(MONTH FROM fecha_emision)=$1 AND EXTRACT(YEAR FROM fecha_emision)=$2",[M,Y]);
    const egr = egrDir + egrOC;
    // OC pendientes del mes (para mostrar en dashboard)
    const ocPend = await n("SELECT COUNT(*) val FROM ordenes_proveedor WHERE estatus IN ('borrador','enviada','pendiente')");
    const ocMes  = await s("SELECT COALESCE(SUM(total),0) val FROM ordenes_proveedor WHERE EXTRACT(MONTH FROM fecha_emision)=$1 AND EXTRACT(YEAR FROM fecha_emision)=$2",[M,Y]);
    const cots5  = await q('SELECT c.numero_cotizacion,c.total,c.moneda,c.estatus,c.created_at,cl.nombre cliente_nombre FROM cotizaciones c LEFT JOIN proyectos p ON p.id=c.proyecto_id LEFT JOIN clientes cl ON cl.id=p.cliente_id ORDER BY c.created_at DESC LIMIT 5');
    const facs5  = await q("SELECT f.numero_factura,f.total,f.moneda,f.estatus,f.fecha_vencimiento,cl.nombre cliente_nombre FROM facturas f LEFT JOIN clientes cl ON cl.id=f.cliente_id WHERE f.estatus IN ('pendiente','parcial') ORDER BY f.fecha_vencimiento LIMIT 5");
    const inv6   = await q(`SELECT DISTINCT ON (nombre) nombre,unidad,
      COALESCE(cantidad_actual,stock_actual,0) stock,
      COALESCE(cantidad_minima,stock_minimo,0) minimo 
      FROM inventario 
      WHERE COALESCE(activo,true)=true
        AND COALESCE(cantidad_actual,stock_actual,0)<=COALESCE(cantidad_minima,stock_minimo,0)
      ORDER BY nombre LIMIT 6`);

    res.json({cotizaciones_activas:cots,clientes:clts,proveedores:provs,
      items_inventario:prods,facturas_pendientes:facts,tareas_pendientes:tar,
      ing_mes:ing,egr_mes:egr,egr_directo:egrDir,egr_oc:egrOC,
      oc_pendientes:ocPend,oc_mes:ocMes,
      cob_mes:cob,iva_mes:ivaFac,isr_mes:isrFac,
      empresa:emp,
      cot_recientes:cots5,fac_vencer:facs5,inv_bajo:inv6,mes_actual});
  } catch(e){
    console.error('dashboard err:',e.message);
    res.json({...base,_error:e.message});
  } finally { try{client.release();}catch{} }
});
// ══════════════════════════════════════════════
// ADMIN API — Gestión global del sistema
// ══════════════════════════════════════════════

// Login admin — sin depender de superadmin flag
app.post('/api/admin/login', async (req,res)=>{
  const {password} = req.body;
  if(!password) return res.status(400).json({error:'Contraseña requerida'});
  try {
    const result = await pool.query("SELECT * FROM public.usuarios WHERE username='admin' LIMIT 1");
    const user = result.rows[0];
    if(!user) return res.status(401).json({error:'Usuario admin no existe. Reinicia el servidor.'});
    const hash = user.password_hash||user.password||'';
    if(!hash) return res.status(401).json({error:'Sin contraseña configurada. Reinicia el servidor.'});
    const ok = await bcrypt.compare(password, hash);
    if(!ok) return res.status(401).json({error:'Contraseña incorrecta'});
    const token = jwt.sign(
      {id:user.id, username:'admin', nombre:user.nombre||'Administrador',
       rol:'admin', superadmin:true, schema:user.schema_name||'emp_vef',
       empresa_id:user.empresa_id},
      JWT_SECRET, {expiresIn:'8h'});
    res.json({ok:true, token, nombre:user.nombre||'Administrador'});
  } catch(e){ 
    console.error('admin/login error:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

// Panel completo — solo necesita token válido con rol admin
app.get('/api/admin/panel', auth, async (req,res)=>{
  if(req.user.rol!=='admin') return res.status(403).json({error:'Solo admin'});
  // Usar conexión directa con search_path a public para asegurar acceso a tablas globales
  const client = await pool.connect();
  try {
    await client.query("SET search_path TO public");
    const empresas = await client.query(`
      SELECT e.id, e.slug, e.nombre, e.activa, e.trial_hasta,
        e.suscripcion_estatus, e.suscripcion_hasta, e.created_at,
        COUNT(u.id) total_usuarios
      FROM public.empresas e
      LEFT JOIN public.usuarios u ON u.empresa_id=e.id
      GROUP BY e.id ORDER BY e.created_at DESC`);
    const usuarios = await client.query(`
      SELECT u.id, u.username, u.nombre, u.email, u.rol, u.empresa_id,
        u.schema_name, u.activo, u.ultimo_acceso, u.created_at,
        e.nombre empresa_nombre
      FROM public.usuarios u
      LEFT JOIN public.empresas e ON e.id=u.empresa_id
      ORDER BY e.nombre NULLS LAST, u.username`);
    const stats = await client.query(`
      SELECT
        (SELECT COUNT(*) FROM public.empresas) total_empresas,
        (SELECT COUNT(*) FROM public.empresas WHERE activa=true) empresas_activas,
        (SELECT COUNT(*) FROM public.empresas WHERE suscripcion_estatus='trial' AND trial_hasta>=CURRENT_DATE) en_trial,
        (SELECT COUNT(*) FROM public.usuarios) total_usuarios`);
    res.json({
      empresas: empresas.rows,
      usuarios: usuarios.rows,
      stats: stats.rows[0]
    });
  } catch(e){
    console.error('admin/panel error:', e.message);
    res.status(500).json({error:e.message});
  } finally { client.release(); }
});

// Activar/modificar empresa
app.put('/api/admin/empresas/:id', auth, async (req,res)=>{
  try {
    const {nombre, activa, dias_trial, suscripcion_activa, dias_suscripcion} = req.body;
    const sets=[]; const vals=[]; let i=1;
    const add=(c,v)=>{sets.push(`${c}=$${i++}`);vals.push(v);};
    if(nombre!==undefined)  add('nombre',nombre);
    if(activa!==undefined)  add('activa',activa);
    if(dias_trial){
      sets.push(`trial_hasta=CURRENT_DATE + ($${i++}::int * INTERVAL '1 day')`); vals.push(parseInt(dias_trial));
      sets.push(`suscripcion_estatus='trial'`);
    }
    if(suscripcion_activa && dias_suscripcion){
      sets.push(`suscripcion_estatus='activa'`);
      sets.push(`suscripcion_hasta=CURRENT_DATE + ($${i++}::int * INTERVAL '1 day')`); vals.push(parseInt(dias_suscripcion));
    }
    if(!sets.length) return res.status(400).json({error:'Nada que actualizar'});
    vals.push(req.params.id);
    const r = await pool.query(`UPDATE empresas SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json({ok:true, empresa:r[0]});
  } catch(e){ res.status(500).json({error:e.message}); }
});

// Crear nueva empresa
app.post('/api/admin/empresas', auth, async (req,res)=>{
  try {
    const {nombre, slug, dias_trial=30} = req.body;
    if(!nombre||!slug) return res.status(400).json({error:'nombre y slug requeridos'});
    const cleanSlug = slug.toLowerCase().replace(/[^a-z0-9_]/g,'_');
    const existing = await pool.query('SELECT id FROM public.empresas WHERE slug=$1',[cleanSlug]);
    if(existing.rows.length) return res.status(400).json({error:'Slug ya existe'});
    const dias = parseInt(dias_trial)||30;
    const emp = await pool.query(
      `INSERT INTO public.empresas(slug,nombre,trial_hasta,suscripcion_estatus,activa)
       VALUES($1,$2,CURRENT_DATE + ($3::int * INTERVAL '1 day'),'trial',true) RETURNING *`,
      [cleanSlug, nombre, dias]);
    const schema = await crearSchemaEmpresa(cleanSlug, nombre);
    res.status(201).json({ok:true, empresa:emp.rows[0], schema});
  } catch(e){ 
    console.error('crear empresa:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

// Reset contraseña de usuario
app.post('/api/admin/usuarios/:id/reset', auth, async (req,res)=>{
  if(req.user.rol!=='admin') return res.status(403).json({error:'Solo admin'});
  try {
    const {nueva_password='password123'} = req.body;
    if(!nueva_password||nueva_password.length<6) return res.status(400).json({error:'Mínimo 6 caracteres'});
    const hash = await bcrypt.hash(nueva_password,10);
    // Verificar qué columnas existen en la tabla usuarios
    const colRes = await pool.query(
      `SELECT column_name FROM information_schema.columns 
       WHERE table_schema='public' AND table_name='usuarios' 
       AND column_name IN ('password_hash','password','contrasena')`);
    const cols = colRes.rows.map(r=>r.column_name);
    const sets = []; const vals = []; let i=1;
    if(cols.includes('password_hash')){ sets.push(`password_hash=$${i++}`); vals.push(hash); }
    if(cols.includes('password'))     { sets.push(`password=$${i++}`);      vals.push(hash); }
    if(cols.includes('contrasena'))   { sets.push(`contrasena=$${i++}`);    vals.push(hash); }
    if(!sets.length) return res.status(500).json({error:'No se encontró columna de contraseña'});
    vals.push(req.params.id);
    await pool.query(`UPDATE public.usuarios SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const u = await pool.query('SELECT id,username,nombre FROM public.usuarios WHERE id=$1',[req.params.id]);
    if(!u.rows.length) return res.status(404).json({error:'Usuario no encontrado'});
    res.json({ok:true, usuario:u.rows[0], nueva_password});
  } catch(e){ console.error('reset pass:', e.message); res.status(500).json({error:e.message}); }
});

// Asignar empresa a usuario
app.put('/api/admin/usuarios/:id/empresa', auth, async (req,res)=>{
  if(req.user.rol!=='admin') return res.status(403).json({error:'Solo admin'});
  try {
    const {empresa_id} = req.body;
    const emp = await pool.query('SELECT slug,nombre FROM public.empresas WHERE id=$1',[empresa_id]);
    if(!emp.rows.length) return res.status(404).json({error:'Empresa no encontrada'});
    const schema = 'emp_'+(emp[0]?.slug||'').replace(/[^a-z0-9]/g,'_');
    await pool.query('UPDATE public.usuarios SET empresa_id=$1,schema_name=$2 WHERE id=$3',[empresa_id,schema,req.params.id]);
    res.json({ok:true, schema, empresa:emp[0]?.nombre});
  } catch(e){ console.error('cambiar empresa:', e.message); res.status(500).json({error:e.message}); }
});

// Crear usuario en una empresa
app.post('/api/admin/usuarios', auth, async (req,res)=>{
  try {
    const {username,nombre,password,rol='usuario',empresa_id} = req.body;
    if(!username||!password) return res.status(400).json({error:'username y password requeridos'});
    const hash = await bcrypt.hash(password,10);
    // Derivar schema del slug de la empresa
    let schema = null;
    let finalEmpId = empresa_id || null;
    if(finalEmpId){
      const emp = await pool.query('SELECT slug FROM public.empresas WHERE id=$1',[finalEmpId]);
      if(emp.rows[0]?.slug) schema = 'emp_'+emp.rows[0].slug.replace(/[^a-z0-9]/g,'_');
    }
    // Si no se especificó empresa, usar la del admin que crea
    if(!finalEmpId && req.user.empresa_id){
      finalEmpId = req.user.empresa_id;
      // Derivar schema desde slug
      const adminEmpR = await pool.query('SELECT slug FROM public.empresas WHERE id=$1',[finalEmpId]);
      if(adminEmpR.rows[0]?.slug) schema = 'emp_'+adminEmpR.rows[0].slug.replace(/[^a-z0-9]/g,'_');
      else schema = req.user.schema || req.user.schema_name;
    }
    // Generar email si no se proporcionó
    const userEmail = username.includes('@') ? username : username+'@'+((schema||'vef').replace('emp_',''))+'.com';
    const r = await pool.query(
      `INSERT INTO public.usuarios(username,nombre,rol,password_hash,password,activo,email,empresa_id,schema_name)
       VALUES($1,$2,$3,$4,$4,true,$5,$6,$7) RETURNING id,username,nombre,rol`,
      [username, nombre||username, rol, hash, userEmail, finalEmpId, schema]);
    res.status(201).json({ok:true, usuario:r.rows[0]});
  } catch(e){ 
    console.error('crear usuario admin:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

// ── Panel de administración global (solo admin) ───────────
// (admin panel endpoint moved to /api/admin/* section above)

// ── Lista de empresas (para selector en formulario de usuarios) ──
app.get('/api/empresas-lista', auth, async (req,res)=>{
  try{
    // Admin del sistema ve todas; admin de empresa solo ve la suya
    let rows;
    if(req.user.rol==='admin'){
      rows = await pool.query('SELECT id,nombre,slug,activa FROM public.empresas WHERE activa=true ORDER BY nombre');
    } else {
      rows = await pool.query('SELECT id,nombre,slug FROM public.empresas WHERE id=$1',[req.user.empresa_id]);
    }
    res.json(rows.rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// REPORTES
// ================================================================
app.get('/api/reportes/cotizaciones', auth, async (req,res)=>{
  const resumen=await QR(req,'SELECT estatus,COUNT(*) cantidad,COALESCE(SUM(total),0) total FROM cotizaciones GROUP BY estatus');
  const detalle=await QR(req,`
    SELECT c.numero_cotizacion, cl.nombre cliente, c.total, c.estatus,
           TO_CHAR(COALESCE(c.created_at,NOW()),'DD/MM/YYYY') fecha
    FROM cotizaciones c
    JOIN proyectos p ON c.proyecto_id=p.id
    JOIN clientes cl ON p.cliente_id=cl.id
    ORDER BY c.id DESC LIMIT 20`);
  res.json({resumen,detalle});
});
app.get('/api/reportes/facturas-pendientes', auth, async (req,res)=>{
  const estCol=has('facturas','estatus_pago')?'f.estatus_pago':'f.estatus';
  res.json(await QR(req,`
    SELECT f.numero_factura, COALESCE(f.total,f.monto,0) monto, ${estCol} estatus,
           TO_CHAR(f.fecha_emision,'DD/MM/YYYY') fecha
    FROM facturas f WHERE ${estCol}='pendiente' ORDER BY f.id DESC`));
});
app.get('/api/reportes/proyectos-activos', auth, async (req,res)=>{
  const respCol=has('proyectos','responsable')?"p.responsable":"'VEF Automatización'";
  res.json(await QR(req,`
    SELECT p.nombre, c.nombre cliente, ${respCol} responsable,
           TO_CHAR(COALESCE(p.fecha_creacion,NOW()::date),'DD/MM/YYYY') fecha
    FROM proyectos p JOIN clientes c ON p.cliente_id=c.id
    WHERE p.estatus='activo' ORDER BY p.nombre`));
});
app.get('/api/reportes/inventario-bajo', auth, async (req,res)=>{
  const cantCol=has('inventario','cantidad_actual')?'cantidad_actual':'COALESCE(stock_actual,0)';
  const minCol =has('inventario','cantidad_minima')?'cantidad_minima':'COALESCE(stock_minimo,0)';
  const actFil =has('inventario','activo')?'WHERE activo=true':'';
  res.json(await QR(req,`
    SELECT nombre,categoria,unidad,${cantCol} cantidad_actual,${minCol} cantidad_minima,
           precio_costo,precio_venta,ubicacion,
           CASE WHEN ${cantCol}<=${minCol} THEN 'BAJO' ELSE 'OK' END estado
    FROM inventario ${actFil} ORDER BY nombre`));
});
app.get('/api/reportes/facturas-por-vencer', auth, async (req,res)=>{
  const estCol=has('facturas','estatus_pago')?'f.estatus_pago':'f.estatus';
  res.json(await QR(req,`
    SELECT f.numero_factura, COALESCE(f.total,f.monto,0) monto, f.moneda,
           TO_CHAR(f.fecha_vencimiento,'DD/MM/YYYY') vencimiento,
           ${estCol} estatus,
           COALESCE((SELECT SUM(p.monto) FROM pagos p WHERE p.factura_id=f.id),0) pagado,
           (f.fecha_vencimiento-CURRENT_DATE) dias
    FROM facturas f
    WHERE ${estCol}!='pagada'
      AND f.fecha_vencimiento IS NOT NULL
      AND f.fecha_vencimiento<=CURRENT_DATE + INTERVAL '30 days'
    ORDER BY f.fecha_vencimiento`));
});

// ================================================================
// CLIENTES
// ================================================================
app.get('/api/clientes', auth, async (req,res)=>{
  const w=has('clientes','activo')?'WHERE activo=true':'';
  res.json(await QR(req,`SELECT * FROM clientes ${w} ORDER BY nombre`));
});
app.get('/api/clientes/:id', auth, async (req,res)=>{
  const r=await QR(req,'SELECT * FROM clientes WHERE id=$1',[req.params.id]);
  r[0]?res.json(r[0]):res.status(404).json({error:'No encontrado'});
});
app.post('/api/clientes', auth, async (req,res)=>{
  try {
    const {nombre,contacto,direccion,telefono,email,rfc,regimen_fiscal,cp,ciudad,tipo_persona}=req.body;
    const cols=['nombre','contacto','direccion','telefono','email'];
    const vals=[nombre,contacto,direccion,telefono,email];
    if(has('clientes','rfc')){cols.push('rfc');vals.push(rfc?.toUpperCase()||null);}
    if(has('clientes','regimen_fiscal')){cols.push('regimen_fiscal');vals.push(regimen_fiscal||null);}
    if(has('clientes','cp')){cols.push('cp');vals.push(cp||null);}
    if(has('clientes','ciudad')){cols.push('ciudad');vals.push(ciudad||null);}
    if(has('clientes','tipo_persona')){cols.push('tipo_persona');vals.push(tipo_persona||'moral');}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const rows=await QR(req,`INSERT INTO clientes (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/clientes/:id', auth, async (req,res)=>{
  try {
    const {nombre,contacto,direccion,telefono,email,rfc,regimen_fiscal,cp,ciudad,tipo_persona}=req.body;
    const sets=[]; const vals=[];let i=1;
    const add=(c,v)=>{sets.push(`${c}=$${i++}`);vals.push(v);};
    add('nombre',nombre);add('contacto',contacto);add('direccion',direccion);
    add('telefono',telefono);add('email',email);
    if(has('clientes','rfc')) add('rfc',rfc?.toUpperCase()||null);
    if(has('clientes','regimen_fiscal')) add('regimen_fiscal',regimen_fiscal||null);
    if(has('clientes','cp')) add('cp',cp||null);
    if(has('clientes','ciudad')) add('ciudad',ciudad||null);
    if(has('clientes','tipo_persona')) add('tipo_persona',tipo_persona||'moral');
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE clientes SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/clientes/:id', auth, adminOnly, async (req,res)=>{
  if(has('clientes','activo')) await QR(req,'UPDATE clientes SET activo=false WHERE id=$1',[req.params.id]);
  else await QR(req,'DELETE FROM clientes WHERE id=$1',[req.params.id]);
  res.json({ok:true});
});

// ── Subir Constancia Fiscal — Cliente ─────────────────────────
app.post('/api/clientes/:id/constancia', auth, async (req,res)=>{
  try {
    const {pdf_base64, nombre} = req.body;
    if(!pdf_base64) return res.status(400).json({error:'PDF requerido'});
    const buf = Buffer.from(pdf_base64,'base64');
    await QR(req,`UPDATE clientes SET constancia_pdf=$1,constancia_nombre=$2,constancia_fecha=NOW() WHERE id=$3`,
      [buf, nombre||'constancia.pdf', req.params.id]);
    res.json({ok:true, nombre:nombre||'constancia.pdf', tamanio:buf.length});
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/clientes/:id/constancia', auth, async (req,res)=>{
  try {
    const [r]=await QR(req,'SELECT constancia_pdf,constancia_nombre FROM clientes WHERE id=$1',[req.params.id]);
    if(!r?.constancia_pdf) return res.status(404).json({error:'Sin constancia'});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="${r.constancia_nombre||'constancia.pdf'}"`);
    res.send(r.constancia_pdf);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Subir Estado de Cuenta — Cliente ──────────────────────────
app.post('/api/clientes/:id/estado-cuenta', auth, async (req,res)=>{
  try {
    const {pdf_base64, nombre} = req.body;
    if(!pdf_base64) return res.status(400).json({error:'PDF requerido'});
    const buf = Buffer.from(pdf_base64,'base64');
    await QR(req,`UPDATE clientes SET estado_cuenta_pdf=$1,estado_cuenta_nombre=$2,estado_cuenta_fecha=NOW() WHERE id=$3`,
      [buf, nombre||'estado_cuenta.pdf', req.params.id]);
    res.json({ok:true, nombre:nombre||'estado_cuenta.pdf', tamanio:buf.length});
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/clientes/:id/estado-cuenta', auth, async (req,res)=>{
  try {
    const [r]=await QR(req,'SELECT estado_cuenta_pdf,estado_cuenta_nombre FROM clientes WHERE id=$1',[req.params.id]);
    if(!r?.estado_cuenta_pdf) return res.status(404).json({error:'Sin estado de cuenta'});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="${r.estado_cuenta_nombre||'estado_cuenta.pdf'}"`);
    res.send(r.estado_cuenta_pdf);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Subir Constancia Fiscal — Proveedor ───────────────────────
app.post('/api/proveedores/:id/constancia', auth, async (req,res)=>{
  try {
    const {pdf_base64, nombre} = req.body;
    if(!pdf_base64) return res.status(400).json({error:'PDF requerido'});
    const buf = Buffer.from(pdf_base64,'base64');
    await QR(req,`UPDATE proveedores SET constancia_pdf=$1,constancia_nombre=$2,constancia_fecha=NOW() WHERE id=$3`,
      [buf, nombre||'constancia.pdf', req.params.id]);
    res.json({ok:true, nombre:nombre||'constancia.pdf', tamanio:buf.length});
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/proveedores/:id/constancia', auth, async (req,res)=>{
  try {
    const [r]=await QR(req,'SELECT constancia_pdf,constancia_nombre FROM proveedores WHERE id=$1',[req.params.id]);
    if(!r?.constancia_pdf) return res.status(404).json({error:'Sin constancia'});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="${r.constancia_nombre||'constancia.pdf'}"`);
    res.send(r.constancia_pdf);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Subir Estado de Cuenta — Proveedor ────────────────────────
app.post('/api/proveedores/:id/estado-cuenta', auth, async (req,res)=>{
  try {
    const {pdf_base64, nombre} = req.body;
    if(!pdf_base64) return res.status(400).json({error:'PDF requerido'});
    const buf = Buffer.from(pdf_base64,'base64');
    await QR(req,`UPDATE proveedores SET estado_cuenta_pdf=$1,estado_cuenta_nombre=$2,estado_cuenta_fecha=NOW() WHERE id=$3`,
      [buf, nombre||'estado_cuenta.pdf', req.params.id]);
    res.json({ok:true, nombre:nombre||'estado_cuenta.pdf', tamanio:buf.length});
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/proveedores/:id/estado-cuenta', auth, async (req,res)=>{
  try {
    const [r]=await QR(req,'SELECT estado_cuenta_pdf,estado_cuenta_nombre FROM proveedores WHERE id=$1',[req.params.id]);
    if(!r?.estado_cuenta_pdf) return res.status(404).json({error:'Sin estado de cuenta'});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="${r.estado_cuenta_nombre||'estado_cuenta.pdf'}"`);
    res.send(r.estado_cuenta_pdf);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Ficha PDF de Cliente ────────────────────────────────────────
app.get('/api/clientes/:id/pdf', auth, async (req,res)=>{
  try {
    const [c]=await QR(req,'SELECT * FROM clientes WHERE id=$1',[req.params.id]);
    if(!c) return res.status(404).json({error:'No encontrado'});
    const cots=await QR(req,`SELECT c.numero_cotizacion,c.fecha_emision,c.total,c.moneda,c.estatus
      FROM cotizaciones c
      LEFT JOIN proyectos p ON p.id=c.proyecto_id
      WHERE p.cliente_id=$1 ORDER BY c.fecha_emision DESC LIMIT 20`,[req.params.id]);
    const pags=await QR(req,`SELECT COALESCE(SUM(pg.monto),0) total_pagado,COUNT(f.id) total_facturas
      FROM facturas f LEFT JOIN pagos pg ON pg.factura_id=f.id
      WHERE f.cliente_id=$1`,[req.params.id]);
    const emp=(await QR(req,'SELECT * FROM empresa_config LIMIT 1'))[0]||{};
    const lp=getLogoPath();
    const PDFKit=require('pdfkit');
    const doc=new PDFKit({margin:28,size:'A4'});
    const bufs=[]; doc.on('data',d=>bufs.push(d));
    await new Promise(resolve=>doc.on('end',resolve));
    const M=28,W=539,H=70;
    // Header
    if(lp){ doc.rect(M,14,120,H-8).fill('#0D2B55');
      try{doc.image(lp,M+4,16,{fit:[112,H-12],align:'center',valign:'center'});}catch(e){} }
    doc.rect(lp?M+124:M,14,lp?W-124:W,H).fill('#0D2B55');
    doc.fillColor('#fff').font('Helvetica-Bold').fontSize(15)
      .text(emp.nombre||VEF_NOMBRE,lp?M+130:M+12,22,{width:lp?W-140:W-20});
    doc.fontSize(9).font('Helvetica').fillColor('#A8C5F0')
      .text(`Tel: ${emp.telefono||VEF_TELEFONO}  |  ${emp.email||VEF_CORREO}`,lp?M+130:M+12,42,{width:W-140});
    doc.moveDown(4);
    doc.fillColor('#0D2B55').font('Helvetica-Bold').fontSize(16)
      .text('FICHA DE CLIENTE',M,H+30,{align:'center',width:W});
    doc.moveDown(.6);
    // Datos cliente
    const fila=(lbl,val)=>{
      const y=doc.y;
      doc.rect(M,y,130,18).fill('#e8f0fa');
      doc.rect(M+130,y,W-130,18).fill('#f8fafc');
      doc.rect(M,y,W,18).lineWidth(0.3).strokeColor('#ddd').stroke();
      doc.fillColor('#0D2B55').font('Helvetica-Bold').fontSize(9).text(lbl,M+5,y+4,{width:122,lineBreak:false});
      doc.fillColor('#222').font('Helvetica').fontSize(9).text(val||'—',M+135,y+4,{width:W-140});
      doc.y=y+18;
    };
    doc.moveDown(.3);
    fila('Nombre:',c.nombre);
    fila('RFC:',c.rfc);
    fila('Régimen Fiscal:',c.regimen_fiscal);
    fila('Contacto:',c.contacto);
    fila('Teléfono:',c.telefono);
    fila('Email:',c.email);
    fila('Dirección:',c.direccion);
    if(c.ciudad||c.cp) fila('Ciudad / CP:',(c.ciudad||'')+(c.cp?' | CP: '+c.cp:''));
    doc.moveDown(.8);
    // Resumen financiero
    const tp=parseFloat(pags[0]?.total_pagado||0);
    const tf=parseInt(pags[0]?.total_facturas||0);
    doc.rect(M,doc.y,W,26).fill('#0D2B55');
    doc.fillColor('#fff').font('Helvetica-Bold').fontSize(10)
      .text(`Facturas: ${tf}   |   Total cobrado: $${tp.toLocaleString('es-MX',{minimumFractionDigits:2})}`,M+10,doc.y-19,{width:W-20,align:'center'});
    doc.moveDown(1.5);
    // Historial cotizaciones
    if(cots.length){
      doc.fillColor('#0D2B55').font('Helvetica-Bold').fontSize(11).text('Historial de Cotizaciones',M);
      doc.moveDown(.3);
      const C2=[M,M+100,M+320,M+400,M+470];
      doc.rect(M,doc.y,W,16).fill('#0D2B55');
      ['N° Cotización','Fecha','Total','Moneda','Estatus'].forEach((h,i)=>
        doc.fillColor('#fff').font('Helvetica-Bold').fontSize(8)
          .text(h,C2[i]+3,doc.y-12,{width:(C2[i+1]||M+W)-C2[i]-4,lineBreak:false}));
      doc.moveDown(.2);
      cots.forEach((co,idx)=>{
        const y=doc.y;
        if(idx%2===0) doc.rect(M,y,W,14).fill('#f4f6fa');
        const fmt=d=>d?new Date(d).toLocaleDateString('es-MX',{day:'2-digit',month:'short',year:'numeric'}):'—';
        [co.numero_cotizacion||'—',fmt(co.fecha_emision),
          '$'+parseFloat(co.total||0).toLocaleString('es-MX',{minimumFractionDigits:2}),
          co.moneda||'USD',co.estatus||'—'
        ].forEach((v,i)=>doc.fillColor('#333').font('Helvetica').fontSize(8)
          .text(String(v),C2[i]+3,y+3,{width:(C2[i+1]||M+W)-C2[i]-4,lineBreak:false}));
        doc.y=y+14;
      });
    }
    // Footer
    doc.rect(M,doc.page.height-50,W,30).fill('#0D2B55');
    doc.fillColor('#A8C5F0').font('Helvetica').fontSize(8)
      .text(`${emp.nombre||VEF_NOMBRE}  |  Tel: ${emp.telefono||VEF_TELEFONO}  |  ${emp.email||VEF_CORREO}  |  ${new Date().toLocaleDateString('es-MX')}`,
        M+8,doc.page.height-42,{width:W-16,align:'center'});
    doc.end();
    const buf=Buffer.concat(bufs);
    savePDFToFile(buf,'cliente',c.id,c.nombre,c.nombre,req.user?.id).catch(()=>{});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="CLI-${c.nombre.replace(/[^a-zA-Z0-9]/g,'_')}.pdf"`);
    res.send(buf);
  }catch(e){console.error(e);res.status(500).json({error:e.message});}
});

// ================================================================
// PROVEEDORES
// ================================================================
app.get('/api/proveedores', auth, async (req,res)=>{
  const w=has('proveedores','activo')?'WHERE activo=true':'';
  res.json(await QR(req,`SELECT * FROM proveedores ${w} ORDER BY nombre`));
});
app.post('/api/proveedores', auth, async (req,res)=>{
  try {
    const {nombre,contacto,direccion,telefono,email,rfc,condiciones_pago,tipo_persona}=req.body;
    const cols=['nombre','contacto','direccion','telefono','email'];
    const vals=[nombre,contacto,direccion,telefono,email];
    if(has('proveedores','rfc')){cols.push('rfc');vals.push(rfc?.toUpperCase()||null);}
    if(has('proveedores','condiciones_pago')){cols.push('condiciones_pago');vals.push(condiciones_pago||null);}
    if(has('proveedores','tipo_persona')){cols.push('tipo_persona');vals.push(tipo_persona||'moral');}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const rows=await QR(req,`INSERT INTO proveedores (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/proveedores/:id', auth, async (req,res)=>{
  try {
    const {nombre,contacto,direccion,telefono,email,rfc,condiciones_pago,tipo_persona}=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(c,v)=>{sets.push(`${c}=$${i++}`);vals.push(v);};
    add('nombre',nombre);add('contacto',contacto);add('direccion',direccion);
    add('telefono',telefono);add('email',email);
    if(has('proveedores','rfc')) add('rfc',rfc?.toUpperCase()||null);
    if(has('proveedores','condiciones_pago')) add('condiciones_pago',condiciones_pago||null);
    if(has('proveedores','tipo_persona')) add('tipo_persona',tipo_persona||'moral');
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE proveedores SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/proveedores/:id', auth, adminOnly, async (req,res)=>{
  if(has('proveedores','activo')) await QR(req,'UPDATE proveedores SET activo=false WHERE id=$1',[req.params.id]);
  else await QR(req,'DELETE FROM proveedores WHERE id=$1',[req.params.id]);
  res.json({ok:true});
});

app.get('/api/proveedores/:id/pdf', auth, async (req,res)=>{
  try {
    const [p]=await QR(req,'SELECT * FROM proveedores WHERE id=$1',[req.params.id]);
    if(!p) return res.status(404).json({error:'No encontrado'});
    const ocs=await QR(req,`SELECT op.*, COALESCE(SUM(io.total),0) items_total
      FROM ordenes_proveedor op
      LEFT JOIN items_orden_proveedor io ON io.orden_id=op.id
      WHERE op.proveedor_id=$1
      GROUP BY op.id ORDER BY op.fecha_emision DESC LIMIT 20`,[req.params.id]);
    const emp=(await QR(req,'SELECT * FROM empresa_config LIMIT 1'))[0]||{};
    const lp=getLogoPath();
    const PDFKit=require('pdfkit');
    const doc=new PDFKit({margin:28,size:'A4'});
    const bufs=[]; doc.on('data',d=>bufs.push(d));
    await new Promise(resolve=>doc.on('end',resolve));

    // Header
    const W=539,M=28,H=70;
    if(lp){
      doc.rect(M,14,120,H-8).fill('#0D2B55');
      try{doc.image(lp,M+4,16,{fit:[112,H-12],align:'center',valign:'center'});}catch(e){}
    } else {
      doc.rect(M,14,W,H).fill('#0D2B55');
    }
    doc.rect(lp?M+124:M,14,lp?W-124:W,H).fill('#0D2B55');
    doc.fillColor('#fff').font('Helvetica-Bold').fontSize(15)
      .text(emp.nombre||VEF_NOMBRE,lp?M+130:M+12,22,{width:lp?W-140:W-20});
    doc.fontSize(9).font('Helvetica').fillColor('#A8C5F0')
      .text(`${emp.telefono||VEF_TELEFONO}  |  ${emp.email||VEF_CORREO}`,lp?M+130:M+12,42,{width:W-140});

    doc.moveDown(4);
    // Título
    doc.fillColor('#0D2B55').font('Helvetica-Bold').fontSize(16)
      .text('FICHA DE PROVEEDOR',M,H+30,{align:'center',width:W});
    doc.moveDown(.5);

    // Datos del proveedor
    const fila=(lbl,val)=>{
      doc.font('Helvetica-Bold').fontSize(9).fillColor('#555').text(lbl,M,doc.y,{continued:true,width:120});
      doc.font('Helvetica').fillColor('#222').text(val||'—',{width:W-120});
    };
    doc.rect(M,doc.y,W,1).fill('#e8ecf0'); doc.moveDown(.3);
    fila('Nombre:',p.nombre);
    fila('RFC:',p.rfc);
    fila('Contacto:',p.contacto);
    fila('Teléfono:',p.telefono);
    fila('Email:',p.email);
    fila('Dirección:',p.direccion);
    fila('Cond. de Pago:',p.condiciones_pago);
    doc.rect(M,doc.y+4,W,1).fill('#e8ecf0'); doc.moveDown(.8);

    // Historial de OC
    if(ocs.length){
      doc.font('Helvetica-Bold').fontSize(11).fillColor('#0D2B55').text('Historial de Órdenes de Compra',M,doc.y);
      doc.moveDown(.4);
      const cols=[M,M+90,M+200,M+280,M+370,M+450];
      const hdr=['N° OC','Emisión','Entrega','Total','Moneda','Estatus'];
      doc.rect(M,doc.y,W,16).fill('#0D2B55');
      hdr.forEach((h,i)=>doc.fillColor('#fff').font('Helvetica-Bold').fontSize(8)
        .text(h,cols[i]+3,doc.y-13,{width:cols[i+1]?cols[i+1]-cols[i]-4:80}));
      doc.moveDown(.2);
      ocs.forEach((o,idx)=>{
        const y=doc.y;
        if(idx%2===0) doc.rect(M,y,W,14).fill('#f4f6fa');
        const fmt=d=>d?new Date(d).toLocaleDateString('es-MX',{day:'2-digit',month:'short',year:'numeric'}):'—';
        const row=[o.numero_op||'—',fmt(o.fecha_emision),fmt(o.fecha_entrega),
          parseFloat(o.total||0).toLocaleString('es-MX',{minimumFractionDigits:2}),o.moneda||'USD',o.estatus||'—'];
        row.forEach((v,i)=>doc.fillColor('#333').font('Helvetica').fontSize(8)
          .text(String(v),cols[i]+3,y+3,{width:cols[i+1]?cols[i+1]-cols[i]-4:80}));
        doc.y=y+14;
      });
    }

    // Footer
    doc.rect(M,doc.page.height-50,W,30).fill('#0D2B55');
    doc.fillColor('#A8C5F0').font('Helvetica').fontSize(8)
      .text(`${emp.nombre||VEF_NOMBRE}  |  Tel: ${emp.telefono||VEF_TELEFONO}  |  ${emp.email||VEF_CORREO}  |  Generado: ${new Date().toLocaleDateString('es-MX')}`,
        M+8,doc.page.height-42,{width:W-16,align:'center'});

    doc.end();
    const buf=Buffer.concat(bufs);
    savePDFToFile(buf,'proveedor',p.id,p.nombre,p.nombre,req.user?.id).catch(()=>{});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="PROV-${p.nombre.replace(/[^a-zA-Z0-9]/g,'_')}.pdf"`);
    res.send(buf);
  }catch(e){console.error(e);res.status(500).json({error:e.message});}
});

// ================================================================
// PROYECTOS
// ================================================================
app.get('/api/proyectos', auth, async (req,res)=>{
  const respCol=has('proyectos','responsable')?"p.responsable,":"";
  res.json(await QR(req,`
    SELECT p.id,p.nombre,p.cliente_id,${respCol}p.estatus,
           COALESCE(p.fecha_creacion,p.created_at) fecha,
           c.nombre cliente_nombre
    FROM proyectos p LEFT JOIN clientes c ON c.id=p.cliente_id
    ORDER BY p.id DESC`));
});
app.post('/api/proyectos', auth, async (req,res)=>{
  try {
    const {nombre,cliente_id,responsable,estatus}=req.body;
    const cols=['nombre','cliente_id','estatus'];
    const vals=[nombre,cliente_id||null,estatus||'activo'];
    if(has('proyectos','responsable')){cols.push('responsable');vals.push(responsable||'VEF Automatización');}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const rows=await QR(req,`INSERT INTO proyectos (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/proyectos/:id', auth, async (req,res)=>{
  try {
    const {nombre,cliente_id,responsable,estatus}=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(c,v)=>{sets.push(`${c}=$${i++}`);vals.push(v);};
    add('nombre',nombre);add('cliente_id',cliente_id);add('estatus',estatus);
    if(has('proyectos','responsable')) add('responsable',responsable);
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE proyectos SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/proyectos/:id', auth, adminOnly, async (req,res)=>{
  await QR(req,'DELETE FROM proyectos WHERE id=$1',[req.params.id]); res.json({ok:true});
});

// ================================================================
// COTIZACIONES
// ================================================================
app.get('/api/cotizaciones', auth, async (req,res)=>{
  const dateCol=has('cotizaciones','created_at')?'c.created_at':'c.fecha_emision';
  res.json(await QR(req,`
    SELECT c.id, c.numero_cotizacion, c.fecha_emision, c.total, c.moneda, c.estatus,
           ${dateCol} fecha_orden,
           p.nombre proyecto_nombre, cl.nombre cliente_nombre, cl.email cliente_email
    FROM cotizaciones c
    LEFT JOIN proyectos p ON p.id=c.proyecto_id
    LEFT JOIN clientes cl ON cl.id=p.cliente_id
    ORDER BY ${dateCol} DESC`));
});

app.get('/api/cotizaciones/:id', auth, async (req,res)=>{
  const [c]=await QR(req,`
    SELECT c.*,
      p.nombre proyecto_nombre,
      cl.nombre cliente_nombre, cl.contacto cliente_contacto,
      cl.email cliente_email, cl.telefono cliente_tel,
      cl.direccion cliente_dir,
      cl.rfc cliente_rfc
    FROM cotizaciones c
    LEFT JOIN proyectos p ON p.id=c.proyecto_id
    LEFT JOIN clientes cl ON cl.id=p.cliente_id
    WHERE c.id=$1`,[req.params.id]);
  if(!c) return res.status(404).json({error:'No encontrada'});
  const items=await QR(req,'SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',[req.params.id]);
  const segs =await QR(req,'SELECT * FROM seguimientos WHERE cotizacion_id=$1 ORDER BY fecha DESC',[req.params.id]);
  res.json({...c,items,seguimientos:segs});
});

app.post('/api/cotizaciones', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO ${schema},public`);
    await client.query('BEGIN');
    const {proyecto_id,moneda,items=[],folio,alcance_tecnico,notas_importantes,
           comentarios_generales,servicio_postventa,condiciones_entrega,condiciones_pago,
           garantia,responsabilidad,validez,fuerza_mayor,ley_aplicable,validez_hasta}=req.body;
    const yr=new Date().getFullYear();
    const cnt=(await client.query("SELECT COUNT(*) FROM cotizaciones WHERE fecha_emision::text LIKE $1",[`${yr}%`])).rows[0]?.count||0;
    const num=folio||`COT-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const total=items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0);

    // Construir INSERT dinámico para cotizaciones
    const cols=['proyecto_id','numero_cotizacion','total','moneda','estatus',
                'alcance_tecnico','notas_importantes','comentarios_generales',
                'condiciones_pago','garantia','validez_hasta'];
    const vals=[proyecto_id||null,num,total,moneda||'USD','borrador',
                alcance_tecnico,notas_importantes,comentarios_generales,
                condiciones_pago,garantia,validez_hasta||null];
    const opt=[
      ['servicio_postventa',servicio_postventa],['condiciones_entrega',condiciones_entrega],
      ['responsabilidad',responsabilidad],['validez',validez],
      ['fuerza_mayor',fuerza_mayor],['ley_aplicable',ley_aplicable],
    ];
    for(const [c,v] of opt){ if(has('cotizaciones',c)){cols.push(c);vals.push(v);} }
    if(has('cotizaciones','created_by')){cols.push('created_by');vals.push(req.user.id);}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows:[cot]}=await client.query(`INSERT INTO cotizaciones (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    for(const it of items){
      await client.query(
        'INSERT INTO items_cotizacion (cotizacion_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
        [cot.id,it.descripcion,it.cantidad,it.precio_unitario,(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0)]);
    }
    await client.query('COMMIT');
    res.status(201).json(cot);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.put('/api/cotizaciones/:id', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO ${schema},public`);
    await client.query('BEGIN');
    const {estatus,moneda,items,alcance_tecnico,notas_importantes,comentarios_generales,
           servicio_postventa,condiciones_entrega,condiciones_pago,garantia,
           responsabilidad,validez,fuerza_mayor,ley_aplicable,validez_hasta}=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(k,v)=>{if(v!==undefined){sets.push(`${k}=$${i++}`);vals.push(v);}};
    add('estatus',estatus);add('moneda',moneda);
    add('alcance_tecnico',alcance_tecnico);add('notas_importantes',notas_importantes);
    add('comentarios_generales',comentarios_generales);add('condiciones_pago',condiciones_pago);
    add('garantia',garantia);add('validez_hasta',validez_hasta);
    if(has('cotizaciones','servicio_postventa')) add('servicio_postventa',servicio_postventa);
    if(has('cotizaciones','condiciones_entrega')) add('condiciones_entrega',condiciones_entrega);
    if(has('cotizaciones','responsabilidad')) add('responsabilidad',responsabilidad);
    if(has('cotizaciones','validez')) add('validez',validez);
    if(has('cotizaciones','fuerza_mayor')) add('fuerza_mayor',fuerza_mayor);
    if(has('cotizaciones','ley_aplicable')) add('ley_aplicable',ley_aplicable);
    if(items){ const t=items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0); add('total',t); }
    if(sets.length){ vals.push(req.params.id); await client.query(`UPDATE cotizaciones SET ${sets.join(',')} WHERE id=$${i}`,vals); }
    if(items){
      await client.query('DELETE FROM items_cotizacion WHERE cotizacion_id=$1',[req.params.id]);
      for(const it of items) await client.query(
        'INSERT INTO items_cotizacion (cotizacion_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
        [req.params.id,it.descripcion,it.cantidad,it.precio_unitario,(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0)]);
    }
    await client.query('COMMIT');
    res.json((await QR(req,'SELECT * FROM cotizaciones WHERE id=$1',[req.params.id]))[0]);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.delete('/api/cotizaciones/:id', auth, adminOnly, async (req,res)=>{
  try {
    await QR(req,'DELETE FROM items_cotizacion WHERE cotizacion_id=$1',[req.params.id]);
    await QR(req,'DELETE FROM seguimientos WHERE cotizacion_id=$1',[req.params.id]);
    await QR(req,'DELETE FROM cotizaciones WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/cotizaciones/:id/seguimiento', auth, async (req,res)=>{
  try {
    const {tipo,notas,proxima_accion}=req.body;
    const {rows}=await pool.query(
      'INSERT INTO seguimientos (cotizacion_id,tipo,notas,proxima_accion) VALUES ($1,$2,$3,$4) RETURNING *',
      [req.params.id,tipo,notas,proxima_accion]);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/cotizaciones/:id/pdf', auth, async (req,res)=>{
  try {
    const [cot]=await QR(req,`
      SELECT c.*,p.nombre proyecto_nombre,
        cl.nombre cliente_nombre,cl.contacto cliente_contacto,
        cl.email cliente_email,cl.telefono cliente_tel,cl.direccion cliente_dir,
        COALESCE((SELECT rfc FROM clientes WHERE id=cl.id),NULL) cliente_rfc
      FROM cotizaciones c
      LEFT JOIN proyectos p ON p.id=c.proyecto_id
      LEFT JOIN clientes cl ON cl.id=p.cliente_id
      WHERE c.id=$1`,[req.params.id]);
    if(!cot) return res.status(404).json({error:'No encontrada'});
    const items=await QR(req,'SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',[req.params.id]);
    const buf=await buildPDFCotizacion(cot,items,req.user?.schema);
    // Guardar en disco automáticamente
    savePDFToFile(buf,'cotizacion',cot.id,cot.numero_cotizacion,cot.cliente_nombre,req.user?.id).catch(()=>{});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="COT-${cot.numero_cotizacion}.pdf"`);
    res.send(buf);
  }catch(e){console.error(e);res.status(500).json({error:e.message});}
});

app.post('/api/cotizaciones/:id/email', auth, async (req,res)=>{
  try {
    const {to,cc,asunto,mensaje}=req.body;
    if(!to) return res.status(400).json({error:'to requerido'});
    const [cot]=await QR(req,`
      SELECT c.*,p.nombre proyecto_nombre,
        cl.nombre cliente_nombre,cl.contacto cliente_contacto,
        cl.email cliente_email,cl.telefono cliente_tel,cl.direccion cliente_dir
      FROM cotizaciones c
      LEFT JOIN proyectos p ON p.id=c.proyecto_id
      LEFT JOIN clientes cl ON cl.id=p.cliente_id
      WHERE c.id=$1`,[req.params.id]);
    if(!cot) return res.status(404).json({error:'No encontrada'});
    const items=await QR(req,'SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',[req.params.id]);
    const buf=await buildPDFCotizacion(cot,items,req.user?.schema);
    const sym=(cot.moneda||'USD')==='USD'?'$':'MX$';
    const dynMailer = await getMailer(req.user?.schema);
    const fromEmail = await getFromEmail(req.user?.schema);
    const empCfg = (await Q('SELECT nombre,telefono,email FROM empresa_config LIMIT 1',[],req.user?.schema))[0]||{};
    const nomEmp = empCfg.nombre||VEF_NOMBRE;
    // Convertir saltos de línea del mensaje a HTML
    const msgHtml = (mensaje||`Estimado/a ${cot.cliente_nombre||'Cliente'},\n\nPor medio del presente, me es grato hacerte llegar la cotización solicitada.\n\nEn el archivo adjunto (PDF) encontrarás el desglose de los precios, la descripción del servicio y las condiciones comerciales.\n\nQuedo atento a cualquier duda o comentario que puedas tener.\n\nSaludos cordiales,`)
      .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
      .replace(/\n/g,'<br>');

    const htmlBody = `<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f4f6f9;font-family:Arial,Helvetica,sans-serif">
<div style="max-width:620px;margin:30px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,.08)">
  <!-- Header -->
  <div style="background:#0D2B55;padding:28px 32px">
    <h1 style="color:#fff;margin:0;font-size:22px;font-weight:700">${nomEmp}</h1>
    ${empCfg.telefono?`<p style="color:#A8C5F0;margin:6px 0 0;font-size:13px">📞 ${empCfg.telefono}</p>`:''}
    ${(empCfg.email||fromEmail)?`<p style="color:#A8C5F0;margin:4px 0 0;font-size:13px">✉️ ${empCfg.email||fromEmail}</p>`:''}
  </div>

  <!-- Cotización badge -->
  <div style="background:#1A4A8A;padding:14px 32px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
    <div>
      <span style="color:#A8C5F0;font-size:11px;text-transform:uppercase;letter-spacing:1px">Cotización</span>
      <div style="color:#fff;font-size:18px;font-weight:700;font-family:monospace">${cot.numero_cotizacion||'—'}</div>
    </div>
    <div style="text-align:right">
      <span style="color:#A8C5F0;font-size:11px;text-transform:uppercase;letter-spacing:1px">Total</span>
      <div style="color:#60d394;font-size:20px;font-weight:700">${sym}${parseFloat(cot.total||0).toLocaleString('es-MX',{minimumFractionDigits:2})} ${cot.moneda||'USD'}</div>
    </div>
  </div>

  <!-- Mensaje -->
  <div style="padding:32px;color:#1e293b;line-height:1.7;font-size:15px">
    ${msgHtml}
  </div>

  <!-- Info box -->
  <div style="margin:0 32px 24px;background:#f0f7ff;border-left:4px solid #1A4A8A;border-radius:0 8px 8px 0;padding:16px">
    <p style="margin:0;font-size:13px;color:#334155">
      <strong>📄 Cotización:</strong> ${cot.numero_cotizacion||'—'}<br>
      ${cot.proyecto_nombre?`<strong>📁 Proyecto:</strong> ${cot.proyecto_nombre}<br>`:''}
      <strong>📅 Fecha:</strong> ${new Date().toLocaleDateString('es-MX',{day:'2-digit',month:'long',year:'numeric'})}<br>
      <strong>💰 Total:</strong> ${sym}${parseFloat(cot.total||0).toLocaleString('es-MX',{minimumFractionDigits:2})} ${cot.moneda||'USD'}
    </p>
  </div>

  <!-- Footer -->
  <div style="background:#0D2B55;padding:16px 32px;text-align:center">
    <p style="color:#A8C5F0;margin:0;font-size:12px">
      ${nomEmp}
      ${empCfg.telefono?` · 📞 ${empCfg.telefono}`:''}
      ${(empCfg.email||fromEmail)?` · ✉️ ${empCfg.email||fromEmail}`:''}
    </p>
    <p style="color:#64748b;margin:4px 0 0;font-size:11px">Este correo fue generado automáticamente por el sistema ERP</p>
  </div>
</div>
</body></html>`;

    await dynMailer.sendMail({
      from:`"${nomEmp}" <${fromEmail}>`,
      to, cc:cc||undefined,
      subject:asunto||`Cotización ${cot.numero_cotizacion} — ${nomEmp}`,
      html: htmlBody,
      attachments:[{filename:`COT-${cot.numero_cotizacion}.pdf`,content:buf}]
    });
    res.json({ok:true,msg:`Correo enviado a ${to}`});
  }catch(e){res.status(500).json({error:e.message});}
});

// ================================================================
// ORDENES DE PROVEEDOR
// ================================================================
app.get('/api/ordenes-proveedor', auth, async (req,res)=>{
  try {
    const dateCol = has('ordenes_proveedor','created_at') ? 'op.created_at' : 'op.fecha_emision';
    const factNom = has('ordenes_proveedor','factura_nombre')   ? 'op.factura_nombre'   : "NULL AS factura_nombre";
    const factFec = has('ordenes_proveedor','factura_fecha')    ? 'op.factura_fecha'    : "NULL AS factura_fecha";
    const cotNom  = has('ordenes_proveedor','cotizacion_nombre')? 'op.cotizacion_nombre': "NULL AS cotizacion_nombre";
    const tieneFac= has('ordenes_proveedor','factura_pdf')      ? '(op.factura_pdf IS NOT NULL) tiene_factura'  : 'FALSE AS tiene_factura';
    const tieneCot= has('ordenes_proveedor','cotizacion_pdf')   ? '(op.cotizacion_pdf IS NOT NULL) tiene_cotizacion' : 'FALSE AS tiene_cotizacion';
    const createdAt = has('ordenes_proveedor','created_at')     ? 'op.created_at'       : 'op.fecha_emision AS created_at';
    res.json(await QR(req,`
      SELECT op.id,op.numero_op,op.proveedor_id,op.fecha_emision,op.fecha_entrega,
             op.condiciones_pago,op.lugar_entrega,op.notas,op.total,op.moneda,op.estatus,
             ${createdAt},${factNom},${factFec},${cotNom},
             ${tieneFac}, ${tieneCot},
             p.nombre proveedor_nombre,p.email proveedor_email
      FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id
      ORDER BY ${dateCol} DESC`));
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/ordenes-proveedor/:id', auth, async (req,res)=>{
  const factNom = has('ordenes_proveedor','factura_nombre')    ? 'op.factura_nombre'    : "NULL AS factura_nombre";
  const factFec = has('ordenes_proveedor','factura_fecha')     ? 'op.factura_fecha'     : "NULL AS factura_fecha";
  const cotNom  = has('ordenes_proveedor','cotizacion_nombre') ? 'op.cotizacion_nombre' : "NULL AS cotizacion_nombre";
  const tieneFac= has('ordenes_proveedor','factura_pdf')       ? '(op.factura_pdf IS NOT NULL) tiene_factura'  : 'FALSE AS tiene_factura';
  const tieneCot= has('ordenes_proveedor','cotizacion_pdf')    ? '(op.cotizacion_pdf IS NOT NULL) tiene_cotizacion' : 'FALSE AS tiene_cotizacion';
  const createdAt= has('ordenes_proveedor','created_at')       ? 'op.created_at'        : 'op.fecha_emision AS created_at';
  const [op]=await QR(req,`
    SELECT op.id,op.numero_op,op.proveedor_id,op.fecha_emision,op.fecha_entrega,
           op.condiciones_pago,op.lugar_entrega,op.notas,op.total,op.moneda,op.estatus,
           ${createdAt},${factNom},${factFec},${cotNom},
           ${tieneFac},${tieneCot},
           p.nombre proveedor_nombre,p.contacto proveedor_contacto,
           p.email proveedor_email,p.telefono proveedor_tel,
           p.direccion proveedor_dir,p.rfc proveedor_rfc
    FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id
    WHERE op.id=$1`,[req.params.id]);
  if(!op) return res.status(404).json({error:'No encontrada'});
  const items=await QR(req,'SELECT * FROM items_orden_proveedor WHERE orden_id=$1 ORDER BY id',[req.params.id]);
  const segs =await QR(req,'SELECT * FROM seguimientos_oc WHERE orden_id=$1 ORDER BY fecha DESC',[req.params.id]);
  res.json({...op,items,seguimientos:segs});
});

app.post('/api/ordenes-proveedor', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO ${schema},public`);
    await client.query('BEGIN');
    const {proveedor_id,moneda,items=[],condiciones_pago,fecha_entrega,lugar_entrega,notas,folio,iva:ivaBody,subtotal:subBody}=req.body;
    const yr=new Date().getFullYear();
    const cnt=(await client.query("SELECT COUNT(*) FROM ordenes_proveedor WHERE fecha_emision::text LIKE $1",[`${yr}%`])).rows[0].count;
    const num=folio||`OP-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const subtotal=parseFloat(subBody)||items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0);
    const iva=parseFloat(ivaBody)||0;
    const total=subtotal+iva;
    const {rows:[op]}=await client.query(
      `INSERT INTO ordenes_proveedor (proveedor_id,numero_op,moneda,subtotal,iva,total,condiciones_pago,fecha_entrega,lugar_entrega,notas,estatus)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'borrador') RETURNING *`,
      [proveedor_id,num,moneda||'USD',subtotal,iva,total,condiciones_pago,fecha_entrega||null,lugar_entrega,notas]);
    for(const it of items) await client.query(
      'INSERT INTO items_orden_proveedor (orden_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
      [op.id,it.descripcion,it.cantidad,it.precio_unitario,(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0)]);
    await client.query('COMMIT');
    res.status(201).json(op);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.put('/api/ordenes-proveedor/:id', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO ${schema},public`);
    await client.query('BEGIN');
    const {estatus,notas,proveedor_id,moneda,fecha_entrega,lugar_entrega,condiciones_pago,total,items,
           iva:ivaUpd,subtotal:subUpd}=req.body;
    // Build dynamic UPDATE
    const sets=[];const vals=[];let i=1;
    const add=(k,v)=>{if(v!==undefined){sets.push(`${k}=$${i++}`);vals.push(v);}};
    add('estatus',estatus);add('notas',notas);
    add('proveedor_id',proveedor_id?parseInt(proveedor_id):undefined);
    add('moneda',moneda);add('fecha_entrega',fecha_entrega||null);
    add('lugar_entrega',lugar_entrega);add('condiciones_pago',condiciones_pago);
    if(ivaUpd!==undefined) add('iva',parseFloat(ivaUpd)||0);
    if(subUpd!==undefined) add('subtotal',parseFloat(subUpd)||0);
    if(total!==undefined) add('total',parseFloat(total)||0);
    if(sets.length){ vals.push(req.params.id); await client.query(`UPDATE ordenes_proveedor SET ${sets.join(',')} WHERE id=$${i}`,vals); }
    // Update items if provided
    if(items){
      await client.query('DELETE FROM items_orden_proveedor WHERE orden_id=$1',[req.params.id]);
      for(const it of items){
        const tot=(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0);
        await client.query('INSERT INTO items_orden_proveedor (orden_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
          [req.params.id,it.descripcion,it.cantidad,it.precio_unitario,tot]);
      }
      const newSub=items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0);
      const newIva=parseFloat(ivaUpd||req.body.iva)||0;
      const newTotal=newSub+newIva;
      await client.query('UPDATE ordenes_proveedor SET subtotal=$1 WHERE id=$2',[newSub,req.params.id]);
      await client.query('UPDATE ordenes_proveedor SET total=$1 WHERE id=$2',[newTotal,req.params.id]);
    }
    await client.query('COMMIT');
    const [updated]=await QR(req,'SELECT * FROM ordenes_proveedor WHERE id=$1',[req.params.id]);
    res.json(updated);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.delete('/api/ordenes-proveedor/:id', auth, adminOnly, async (req,res)=>{
  try {
    await QR(req,'DELETE FROM items_orden_proveedor WHERE orden_id=$1',[req.params.id]);
    await QR(req,'DELETE FROM seguimientos_oc WHERE orden_id=$1',[req.params.id]);
    await QR(req,'DELETE FROM ordenes_proveedor WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/ordenes-proveedor/:id/seguimiento', auth, async (req,res)=>{
  try {
    const {tipo,notas,proxima_accion}=req.body;
    const {rows}=await pool.query(
      'INSERT INTO seguimientos_oc (orden_id,tipo,notas,proxima_accion) VALUES ($1,$2,$3,$4) RETURNING *',
      [req.params.id,tipo,notas,proxima_accion]);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Subir factura del proveedor a la OC ──────────────────────────
app.post('/api/ordenes-proveedor/:id/factura', auth, async (req,res)=>{
  try {
    const {data, nombre, mime} = req.body;
    if(!data) return res.status(400).json({error:'data requerido'});
    const buf = Buffer.from(data.replace(/^data:[^;]+;base64,/,''),'base64');
    if(buf.length > 15*1024*1024) return res.status(400).json({error:'Archivo muy grande (máx 15MB)'});
    await QR(req,
      'UPDATE ordenes_proveedor SET factura_pdf=$1, factura_nombre=$2, factura_fecha=NOW() WHERE id=$3',
      [buf, nombre||'factura.pdf', req.params.id]);
    const [oc]=await QR(req,'SELECT numero_op,proveedor_id FROM ordenes_proveedor WHERE id=$1',[req.params.id]);
    savePDFToFile(buf,'factura_proveedor',req.params.id,oc?.numero_op,'Proveedor',req.user?.id,req.user?.schema).catch(()=>{});
    res.json({ok:true, nombre: nombre||'factura.pdf', bytes: buf.length});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Ver/descargar factura del proveedor ──────────────────────────
app.get('/api/ordenes-proveedor/:id/factura', auth, async (req,res)=>{
  try {
    const rows = await QR(req,'SELECT factura_pdf, factura_nombre FROM ordenes_proveedor WHERE id=$1',[req.params.id]);
    if(!rows.length||!rows[0].factura_pdf) return res.status(404).json({error:'Sin factura subida'});
    const {factura_pdf, factura_nombre} = rows[0];
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="${factura_nombre||'factura.pdf'}"`);
    res.send(Buffer.isBuffer(factura_pdf)?factura_pdf:Buffer.from(factura_pdf));
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Subir cotización del proveedor (referencia) ───────────────────
app.post('/api/ordenes-proveedor/:id/cotizacion-prov', auth, async (req,res)=>{
  try {
    const {data, nombre} = req.body;
    if(!data) return res.status(400).json({error:'data requerido'});
    const buf = Buffer.from(data.replace(/^data:[^;]+;base64,/,''),'base64');
    if(buf.length > 15*1024*1024) return res.status(400).json({error:'Archivo muy grande (máx 15MB)'});
    await QR(req,
      'UPDATE ordenes_proveedor SET cotizacion_pdf=$1, cotizacion_nombre=$2 WHERE id=$3',
      [buf, nombre||'cotizacion_proveedor.pdf', req.params.id]);
    res.json({ok:true, nombre: nombre||'cotizacion_proveedor.pdf', bytes: buf.length});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Ver/descargar cotización del proveedor ────────────────────────
app.get('/api/ordenes-proveedor/:id/cotizacion-prov', auth, async (req,res)=>{
  try {
    const rows = await QR(req,'SELECT cotizacion_pdf, cotizacion_nombre FROM ordenes_proveedor WHERE id=$1',[req.params.id]);
    if(!rows.length||!rows[0].cotizacion_pdf) return res.status(404).json({error:'Sin cotización subida'});
    const {cotizacion_pdf, cotizacion_nombre} = rows[0];
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="${cotizacion_nombre||'cotizacion.pdf'}"`);
    res.send(Buffer.isBuffer(cotizacion_pdf)?cotizacion_pdf:Buffer.from(cotizacion_pdf));
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/ordenes-proveedor/:id/pdf', auth, async (req,res)=>{
  try {
    const [op]=await QR(req,`
      SELECT op.*,p.nombre proveedor_nombre,p.contacto proveedor_contacto,
             p.email proveedor_email,p.telefono proveedor_tel,
             p.direccion proveedor_dir,p.rfc proveedor_rfc
      FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id
      WHERE op.id=$1`,[req.params.id]);
    if(!op) return res.status(404).json({error:'No encontrada'});
    const items=await QR(req,'SELECT * FROM items_orden_proveedor WHERE orden_id=$1 ORDER BY id',[req.params.id]);
    const buf=await buildPDFOrden(op,items,req.user?.schema);
    savePDFToFile(buf,'orden_compra',op.id,op.numero_op,op.proveedor_nombre,req.user?.id).catch(()=>{});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="OP-${op.numero_op}.pdf"`);
    res.send(buf);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/ordenes-proveedor/:id/email', auth, async (req,res)=>{
  try {
    const {to,cc,mensaje}=req.body;
    const [op]=await QR(req,`SELECT op.*,p.nombre proveedor_nombre,p.email proveedor_email,p.contacto proveedor_contacto,p.telefono proveedor_tel,p.direccion proveedor_dir,p.rfc proveedor_rfc FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id WHERE op.id=$1`,[req.params.id]);
    if(!op) return res.status(404).json({error:'No encontrada'});
    const items=await QR(req,'SELECT * FROM items_orden_proveedor WHERE orden_id=$1 ORDER BY id',[req.params.id]);
    const dest=to||op.proveedor_email;
    if(!dest) return res.status(400).json({error:'Destinatario requerido'});
    const buf=await buildPDFOrden(op,items,req.user?.schema);
    const dynMailerOC = await getMailer(req.user?.schema);
    const fromEmailOC = await getFromEmail(req.user?.schema);
    const empCfgOC = (await Q('SELECT nombre FROM empresa_config LIMIT 1',[],req.user?.schema))[0]||{};
    const nomEmpOC = empCfgOC.nombre||VEF_NOMBRE;
    await dynMailerOC.sendMail({
      from:`"${nomEmpOC}" <${fromEmailOC}>`,to:dest,cc:cc||undefined,
      subject:`Orden de Compra ${op.numero_op} — ${nomEmpOC}`,
      html:`<p>${mensaje||'Estimado proveedor, adjuntamos la orden de compra.'}</p><p>OC: <b>${op.numero_op}</b></p>`,
      attachments:[{filename:`OP-${op.numero_op}.pdf`,content:buf}]
    });
    res.json({ok:true,msg:`Enviado a ${dest}`});
  }catch(e){res.status(500).json({error:e.message});}
});

// ================================================================
// FACTURAS
// ================================================================
app.get('/api/facturas', auth, async (req,res)=>{
  const filtro=req.query.estatus;
  const estCol=has('facturas','estatus_pago')?'f.estatus_pago':'f.estatus';
  const monedaCol=has('facturas','moneda')?"f.moneda":"'USD'";
  const totalCol=has('facturas','total')?'f.total':has('facturas','monto')?'f.monto':'0';
  const isrCol2=has('facturas','retencion_isr')?'f.retencion_isr':'0 AS retencion_isr';
  const ivaCol2=has('facturas','iva')?'f.iva':'0 AS iva';
  let sql=`
    SELECT f.id, f.numero_factura, ${totalCol} total, ${monedaCol} moneda,
           ${estCol} estatus, f.fecha_emision,
           ${has('facturas','fecha_vencimiento')?'f.fecha_vencimiento,':''}
           COALESCE(c.numero_cotizacion,'—') numero_cotizacion,
           COALESCE(cl.nombre,'—') cliente_nombre,
           COALESCE((SELECT SUM(p.monto) FROM pagos p WHERE p.factura_id=f.id),0) pagado
    FROM facturas f
    LEFT JOIN cotizaciones c ON c.id=f.cotizacion_id
    LEFT JOIN clientes cl ON cl.id=${has('facturas','cliente_id')?'f.cliente_id':'c.proyecto_id'}
    WHERE 1=1`;
  const params=[];
  if(filtro&&filtro!=='todos'){
    if(filtro==='vencidas') sql+=` AND ${estCol}!='pagada' AND f.fecha_vencimiento<CURRENT_DATE`;
    else{ sql+=` AND ${estCol}=$1`; params.push(filtro); }
  }
  sql+=' ORDER BY f.id DESC';
  res.json(await QR(req,sql,params));
});

app.post('/api/facturas', auth, async (req,res)=>{
  try {
    const {cotizacion_id,cliente_id,moneda,subtotal,iva,total,fecha_vencimiento,notas}=req.body;
    const yr=new Date().getFullYear();
    const cnt=((await QR(req,"SELECT COUNT(*) FROM facturas WHERE fecha_emision::text LIKE $1",[`${yr}%`]))[0]||{}).count||0;
    const num=`FAC-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const {retencion_isr=0, retencion_iva=0} = req.body;
    const cols=['numero_factura','cotizacion_id'];
    const vals=[num,cotizacion_id||null];
    const maybePush=(col,val)=>{ if(has('facturas',col)){cols.push(col);vals.push(val);} };
    maybePush('cliente_id',cliente_id||null);
    maybePush('moneda',moneda||'USD');
    maybePush('subtotal',parseFloat(subtotal)||0);
    maybePush('iva',parseFloat(iva)||0);
    maybePush('retencion_isr',parseFloat(retencion_isr)||0);
    maybePush('retencion_iva',parseFloat(retencion_iva)||0);
    maybePush('total',parseFloat(total)||0);
    maybePush('monto',parseFloat(total)||0);
    maybePush('fecha_vencimiento',fecha_vencimiento||null);
    maybePush('notas',notas);
    maybePush('estatus','pendiente');
    maybePush('estatus_pago','pendiente');
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const rows=await QR(req,`INSERT INTO facturas (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.put('/api/facturas/:id', auth, async (req,res)=>{
  try {
    const {estatus,notas,fecha_vencimiento}=req.body;
    const sets=[];const vals=[];let i=1;
    if(has('facturas','estatus')){sets.push(`estatus=$${i++}`);vals.push(estatus);}
    if(has('facturas','estatus_pago')){sets.push(`estatus_pago=$${i++}`);vals.push(estatus);}
    if(notas!==undefined&&has('facturas','notas')){sets.push(`notas=$${i++}`);vals.push(notas);}
    if(fecha_vencimiento!==undefined&&has('facturas','fecha_vencimiento')){sets.push(`fecha_vencimiento=$${i++}`);vals.push(fecha_vencimiento||null);}
    if(!sets.length) return res.status(400).json({error:'Nada que actualizar'});
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE facturas SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/facturas/:id/pago', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO ${schema},public`);
    await client.query('BEGIN');
    const {monto,metodo,referencia,notas,fecha}=req.body;
    // Insert with fecha if column exists
    const fechaVal=fecha||null;
    if(has('pagos','fecha')){
      await client.query('INSERT INTO pagos (factura_id,monto,metodo,referencia,notas,fecha) VALUES ($1,$2,$3,$4,$5,$6)',
        [req.params.id,monto,metodo,referencia,notas,fechaVal]);
    } else {
      await client.query('INSERT INTO pagos (factura_id,monto,metodo,referencia,notas) VALUES ($1,$2,$3,$4,$5)',
        [req.params.id,monto,metodo,referencia,notas]);
    }
    const pg=(await client.query('SELECT COALESCE(SUM(monto),0) total FROM pagos WHERE factura_id=$1',[req.params.id])).rows[0];
    const ft=(await client.query(`SELECT COALESCE(total,monto,0) total FROM facturas WHERE id=$1`,[req.params.id])).rows[0];
    const pagado=parseFloat(pg.total), totalF=parseFloat(ft?.total||0);
    const estatus=pagado>=totalF?'pagada':pagado>0?'parcial':'pendiente';
    if(has('facturas','estatus')) await client.query('UPDATE facturas SET estatus=$1 WHERE id=$2',[estatus,req.params.id]);
    if(has('facturas','estatus_pago')) await client.query('UPDATE facturas SET estatus_pago=$1 WHERE id=$2',[estatus,req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true,estatus,pagado,saldo:totalF-pagado});
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.get('/api/facturas/:id/pagos', auth, async (req,res)=>{
  res.json(await QR(req,'SELECT * FROM pagos WHERE factura_id=$1 ORDER BY fecha DESC',[req.params.id]));
});

app.delete('/api/facturas/:id', auth, adminOnly, async (req,res)=>{
  try {
    await QR(req,'DELETE FROM pagos WHERE factura_id=$1',[req.params.id]);
    await QR(req,'DELETE FROM facturas WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/facturas/:id/pdf', auth, async (req,res)=>{
  try {
    const [f]=await QR(req,`
      SELECT f.*,cl.nombre cliente_nombre,cl.rfc cliente_rfc,
             cl.email cliente_email,cl.telefono cliente_tel
      FROM facturas f LEFT JOIN clientes cl ON cl.id=f.cliente_id
      WHERE f.id=$1`,[req.params.id]);
    if(!f) return res.status(404).json({error:'No encontrada'});
    const items=f.cotizacion_id?await QR(req,'SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',[f.cotizacion_id]):[];
    const buf=await buildPDFFactura(f,items,req.user?.schema);
    savePDFToFile(buf,'factura',f.id,f.numero_factura,f.cliente_nombre,req.user?.id).catch(()=>{});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="FAC-${f.numero_factura}.pdf"`);
    res.send(buf);
  }catch(e){res.status(500).json({error:e.message});}
});

// ================================================================
// INVENTARIO
// ================================================================
app.get('/api/inventario', auth, async (req,res)=>{
  try {
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    // Detectar columnas en tiempo real
    const C = await getCols(schema, 'inventario');
    const cantCol  = C.has('cantidad_actual')?'i.cantidad_actual':C.has('stock_actual')?'i.stock_actual':'0';
    const minCol   = C.has('cantidad_minima')?'i.cantidad_minima':C.has('stock_minimo')?'i.stock_minimo':'0';
    const fotoCol  = C.has('foto')?'i.foto':"'' AS foto";
    const fechaCol = C.has('fecha_ultima_entrada')?'i.fecha_ultima_entrada':'NULL AS fecha_ultima_entrada';
    const notasCol = C.has('notas')?'i.notas':"'' AS notas";
    const activoCol= C.has('activo')?'i.activo':'true AS activo';
    const actFil   = C.has('activo') ? (req.query.todos==='1'?'':'WHERE COALESCE(i.activo,true)=true') : '';
    res.json(await QR(req,`
      SELECT i.id,i.codigo,i.nombre,i.descripcion,i.categoria,i.unidad,
        i.precio_costo,i.precio_venta,i.ubicacion,i.proveedor_id,
        ${fechaCol}, ${notasCol}, ${activoCol}, i.created_at,
        ${cantCol} qty_actual, ${minCol} qty_minima,
        ${fotoCol},
        pr.nombre proveedor_nombre
      FROM inventario i LEFT JOIN proveedores pr ON pr.id=i.proveedor_id
      ${actFil} ORDER BY i.nombre`));
  } catch(e){ 
    console.error('inv GET:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

// Endpoint dedicado para foto individual (evita cargar todas las fotos en el listado)
app.get('/api/inventario/:id/foto', auth, async (req,res)=>{
  try {
    if(!has('inventario','foto')) return res.status(404).json({error:'Sin foto'});
    const rows=await QR(req,'SELECT foto FROM inventario WHERE id=$1',[req.params.id]);
    if(!rows.length||!rows[0].foto) return res.status(404).json({error:'Sin foto'});
    // Devolver como imagen
    const data=rows[0].foto;
    if(data.startsWith('data:')){
      const [header,b64]=data.split(',');
      const mime=header.match(/:(.*?);/)?.[1]||'image/png';
      const buf=Buffer.from(b64,'base64');
      res.setHeader('Content-Type',mime);
      res.setHeader('Cache-Control','private,max-age=86400');
      return res.send(buf);
    }
    res.json({foto:data});
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/inventario', auth, async (req,res)=>{
  try {
    const {codigo,nombre,descripcion,categoria,unidad,cantidad_actual,cantidad_minima,
           precio_costo,precio_venta,ubicacion,proveedor_id,notas,foto}=req.body;
    if(!nombre) return res.status(400).json({error:'Nombre requerido'});
    const cols=['nombre','descripcion','categoria','unidad','precio_costo','precio_venta'];
    const vals=[nombre,descripcion||null,categoria||null,unidad||'pza',precio_costo||0,precio_venta||0];
    const mp=(c,v)=>{if(v!==undefined&&v!==''&&has('inventario',c)){cols.push(c);vals.push(v);}};
    mp('codigo',codigo||null);
    mp('ubicacion',ubicacion||null);
    mp('notas',notas||null);
    mp('proveedor_id',proveedor_id?parseInt(proveedor_id):null);
    mp('foto',foto||null);
    mp('cantidad_actual',parseFloat(cantidad_actual)||0);
    mp('cantidad_minima',parseFloat(cantidad_minima)||0);
    mp('stock_actual',parseFloat(cantidad_actual)||0);
    mp('stock_minimo',parseFloat(cantidad_minima)||0);
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const rows=await QR(req,`INSERT INTO inventario (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){console.error('inv POST:',e.message);res.status(500).json({error:e.message});}
});

app.put('/api/inventario/:id', auth, async (req,res)=>{
  try {
    const {codigo,nombre,descripcion,categoria,unidad,cantidad_minima,
           precio_costo,precio_venta,ubicacion,notas,foto}=req.body;
    if(!nombre) return res.status(400).json({error:'Nombre requerido'});
    
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    const cols = await getCols(schema, 'inventario');
    
    const sets=[]; const vals=[]; let i=1;
    const add=(c,v)=>{sets.push(`${c}=$${i++}`);vals.push(v);};
    const addIf=(c,v)=>{if(cols.has(c)){add(c,v);}};
    
    add('nombre', nombre);
    addIf('descripcion', descripcion||null);
    addIf('categoria', categoria||null);
    addIf('unidad', unidad||'pza');
    addIf('precio_costo', parseFloat(precio_costo)||0);
    addIf('precio_venta', parseFloat(precio_venta)||0);
    addIf('codigo', codigo||null);
    addIf('ubicacion', ubicacion||null);
    addIf('notas', notas||null);
    addIf('cantidad_minima', parseFloat(cantidad_minima)||0);
    addIf('stock_minimo', parseFloat(cantidad_minima)||0);
    if(cols.has('foto') && foto!==undefined){ add('foto', foto); }
    
    if(sets.length===0) return res.status(400).json({error:'Nada que actualizar'});
    vals.push(req.params.id);
    const rows = await QR(req,`UPDATE inventario SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    if(!rows.length) return res.status(404).json({error:'Producto no encontrado'});
    res.json(rows[0]);
  }catch(e){ 
    console.error('inv PUT:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

app.post('/api/inventario/:id/movimiento', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    await client.query(`SET search_path TO ${schema},public`);
    await client.query('BEGIN');
    const {tipo,cantidad,notas,referencia}=req.body;
    const cant=parseFloat(cantidad)||0;
    // Detectar columnas de stock usando cache
    const invColsSet = await getCols(schema, 'inventario');
    const stockCols = ['cantidad_actual','stock_actual'].filter(c => invColsSet.has(c));
    const cantCol=stockCols.includes('cantidad_actual')?'cantidad_actual':stockCols.includes('stock_actual')?'stock_actual':'cantidad_actual';
    const [prod]=(await client.query(`SELECT COALESCE(${cantCol},0) stock FROM inventario WHERE id=$1`,[req.params.id])).rows;
    if(!prod) throw new Error('Producto no encontrado');
    let nuevo=parseFloat(prod.stock)||0;
    if(tipo==='entrada') nuevo+=cant;
    else if(tipo==='salida'){if(nuevo<cant) throw new Error('Stock insuficiente');nuevo-=cant;}
    else if(tipo==='ajuste') nuevo=cant;
    // Usar stockCols ya detectadas arriba + fecha si existe
    const upd=[];
    if(stockCols.includes('cantidad_actual')) upd.push(`cantidad_actual=${nuevo}`);
    if(stockCols.includes('stock_actual'))    upd.push(`stock_actual=${nuevo}`);
    if(invColsSet.has('fecha_ultima_entrada')) upd.push(`fecha_ultima_entrada=CURRENT_DATE`);
    if(upd.length===0) upd.push(`cantidad_actual=${nuevo}`); // fallback
    await client.query(`UPDATE inventario SET ${upd.join(',')} WHERE id=$1`,[req.params.id]);
    // Insertar movimiento — intenta con columnas extendidas, si falla usa mínimas
    try {
      const mCols=['producto_id','tipo','cantidad'];
      const mVals=[req.params.id,tipo,cant];
      const movCols = await getCols(schema, 'movimientos_inventario');
      const mAdd=(col,val)=>{ if(movCols.has(col)){mCols.push(col);mVals.push(val);} };
      mAdd('stock_anterior',    prod.stock);
      mAdd('stock_nuevo',       nuevo);
      mAdd('cantidad_anterior', prod.stock);
      mAdd('cantidad_nueva',    nuevo);
      mAdd('notas',             notas||null);
      mAdd('referencia',        referencia||null);
      mAdd('created_by',        req.user.id);
      const mPh=mVals.map((_,i)=>`$${i+1}`).join(',');
      await client.query(`INSERT INTO movimientos_inventario (${mCols.join(',')}) VALUES (${mPh})`,mVals);
    } catch(e2) {
      // Fallback: solo columnas mínimas garantizadas
      console.warn('movimiento INSERT fallback:', e2.message);
      await client.query(
        'INSERT INTO movimientos_inventario (producto_id,tipo,cantidad) VALUES ($1,$2,$3)',
        [req.params.id,tipo,cant]);
    }
    await client.query('COMMIT');
    res.json({ok:true,stock_nuevo:nuevo});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

app.get('/api/inventario/movimientos', auth, async (req,res)=>{
  res.json(await QR(req,`
    SELECT m.*,i.nombre producto_nombre
    FROM movimientos_inventario m LEFT JOIN inventario i ON i.id=m.producto_id
    ORDER BY m.fecha DESC LIMIT 200`));
});

app.delete('/api/inventario/:id', auth, adminOnly, async (req,res)=>{
  if(has('inventario','activo')) await QR(req,'UPDATE inventario SET activo=false WHERE id=$1',[req.params.id]);
  else await QR(req,'DELETE FROM inventario WHERE id=$1',[req.params.id]);
  res.json({ok:true});
});

// ================================================================
// USUARIOS
// ================================================================
app.get('/api/usuarios', auth, adminOnly, async (req,res)=>{
  try {
    const empId = req.user.empresa_id;
    const result = await pool.query(
      'SELECT id,username,nombre,email,rol,activo,ultimo_acceso FROM public.usuarios WHERE empresa_id=$1 AND COALESCE(activo,true)=true ORDER BY nombre',
      [empId]);
    res.json(result.rows);
  } catch(e) { res.status(500).json({error:e.message}); }
});
app.post('/api/usuarios', auth, adminOnly, async (req,res)=>{
  try {
    const {username,nombre,email,password,rol}=req.body;
    if(!username||!password) return res.status(400).json({error:'username y password requeridos'});
    const hash=await bcrypt.hash(password,12);
    // Siempre usar la empresa del usuario que crea si no se especifica otra
    // empresa_id null/undefined/0 => heredar del creador
    const bodyEmpId = req.body.empresa_id ? parseInt(req.body.empresa_id) : null;
    const creatorEmpId = bodyEmpId || req.user.empresa_id || null;
    // Siempre derivar schema desde slug en BD — nunca confiar en datos del JWT
    let creatorSchema = req.user.schema || req.user.schema_name || null;
    if(creatorEmpId){
      const empR = await pool.query('SELECT slug FROM public.empresas WHERE id=$1',[creatorEmpId]);
      if(empR.rows[0]?.slug){
        creatorSchema = 'emp_'+empR.rows[0].slug.replace(/[^a-z0-9]/g,'_');
      }
    }
    const emailVal = email || (username.includes('@')?username:username+'@erp.local');
    const cols=['username','nombre','rol','password_hash','password','activo','email','empresa_id','schema_name'];
    const vals=[username, nombre||username, rol||'usuario', hash, hash, true, emailVal, creatorEmpId, creatorSchema];
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows}=await pool.query(`INSERT INTO public.usuarios (${cols.join(',')}) VALUES (${ph}) RETURNING id,username,nombre,rol`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/usuarios/:id', auth, adminOnly, async (req,res)=>{
  try {
    const {nombre,email,rol,activo,empresa_id}=req.body;
    if(empresa_id){
      const emp=await pool.query('SELECT slug FROM empresas WHERE id=$1',[empresa_id]);
      if(emp.rows.length>0){
        const schema='emp_'+(emp.rows[0]?.slug||'').replace(/[^a-z0-9]/g,'_');
        await pool.query('UPDATE public.usuarios SET empresa_id=$1, schema_name=$2 WHERE id=$3',[empresa_id,schema,req.params.id]);
      }
    }
    const sets=[];const vals=[];let i=1;
    if(nombre!==undefined){sets.push(`nombre=$${i++}`);vals.push(nombre);}
    if(rol!==undefined){sets.push(`rol=$${i++}`);vals.push(rol);}
    if(email!==undefined){sets.push(`email=$${i++}`);vals.push(email);}
    if(activo!==undefined){sets.push(`activo=$${i++}`);vals.push(activo);}
    if(!sets.length) return res.json({ok:true});
    vals.push(req.params.id);
    const {rows}=await pool.query(`UPDATE public.usuarios SET ${sets.join(',')} WHERE id=$${i} RETURNING id,username,nombre,rol,activo`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

// Eliminar usuario (solo admin — no puede eliminarse a sí mismo)
app.delete('/api/usuarios/:id', auth, adminOnly, async (req,res)=>{
  try {
    if(parseInt(req.params.id)===req.user.id)
      return res.status(400).json({error:'No puedes eliminarte a ti mismo'});
    const u=await pool.query('SELECT username FROM public.usuarios WHERE id=$1',[req.params.id]);
    if(!u.rows.length) return res.status(404).json({error:'Usuario no encontrado'});
    if(u.rows[0].username==='admin')
      return res.status(400).json({error:'No se puede eliminar el usuario admin del sistema'});
    await pool.query('DELETE FROM public.usuarios WHERE id=$1',[req.params.id]);
    res.json({ok:true, eliminado:u.rows[0].username});
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/usuarios/:id/reset-password', auth, adminOnly, async (req,res)=>{
  try {
    const {password}=req.body;
    if(!password) return res.status(400).json({error:'Nueva contraseña requerida'});
    const hash=await bcrypt.hash(password,12);
    // Verificar columnas reales en tiempo real
    const colRes = await pool.query(
      `SELECT column_name FROM information_schema.columns 
       WHERE table_schema='public' AND table_name='usuarios' 
       AND column_name IN ('password_hash','password','contrasena')`);
    const cols = colRes.rows.map(r=>r.column_name);
    const sets=[];const pvals=[];let pi=1;
    if(cols.includes('password_hash')){ sets.push(`password_hash=$${pi++}`); pvals.push(hash); }
    if(cols.includes('password'))     { sets.push(`password=$${pi++}`);      pvals.push(hash); }
    if(cols.includes('contrasena'))   { sets.push(`contrasena=$${pi++}`);    pvals.push(hash); }
    if(!sets.length) return res.status(500).json({error:'No se encontró columna de contraseña'});
    pvals.push(req.params.id);
    await pool.query(`UPDATE public.usuarios SET ${sets.join(',')} WHERE id=$${pi}`,pvals);
    res.json({ok:true});
  }catch(e){ 
    console.error('reset-password:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

// ── Lista de empresas (para selector en formulario de usuarios) ──
app.get('/api/empresas-lista', auth, async (req,res)=>{
  try{
    // Admin del sistema ve todas; admin de empresa solo ve la suya
    let rows;
    if(req.user.rol==='admin'){
      rows = await pool.query('SELECT id,nombre,slug,activa FROM public.empresas WHERE activa=true ORDER BY nombre');
    } else {
      rows = await pool.query('SELECT id,nombre,slug FROM public.empresas WHERE id=$1',[req.user.empresa_id]);
    }
    res.json(rows.rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// REPORTES DE SERVICIO
// ================================================================
app.get('/api/reportes-servicio', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      ORDER BY rs.created_at DESC`);
    res.json(rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/reportes-servicio/:id', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre, cl.rfc cliente_rfc,
        cl.email cliente_email, cl.telefono cliente_tel,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      WHERE rs.id=$1`,[req.params.id]);
    if(!rows.length) return res.status(404).json({error:'No encontrado'});
    res.json(rows[0]);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/reportes-servicio', auth, async (req,res)=>{
  try{
    const {titulo,cliente_id,proyecto_id,fecha_reporte,fecha_servicio,tecnico,
           introduccion,objetivo,alcance,descripcion_sistema,arquitectura,
           desarrollo_tecnico,resultados_pruebas,problemas_detectados,
           soluciones_implementadas,conclusiones,recomendaciones,anexos} = req.body;
    if(!titulo) return res.status(400).json({error:'Título requerido'});
    const yr = new Date().getFullYear();
    const cnt = await QR(req,'SELECT COUNT(*) val FROM reportes_servicio');
    const num = `RS-${yr}-${String(parseInt(cnt[0]?.val||0)+1).padStart(3,'0')}`;
    const rows = await QR(req,`
      INSERT INTO reportes_servicio (numero_reporte,titulo,cliente_id,proyecto_id,
        fecha_reporte,fecha_servicio,tecnico,estatus,
        introduccion,objetivo,alcance,descripcion_sistema,arquitectura,
        desarrollo_tecnico,resultados_pruebas,problemas_detectados,
        soluciones_implementadas,conclusiones,recomendaciones,anexos,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'borrador',$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
      RETURNING *`,
      [num,titulo,cliente_id||null,proyecto_id||null,
       fecha_reporte||new Date().toISOString().slice(0,10),
       fecha_servicio||null, tecnico||req.user.nombre||'VEF',
       introduccion||null,objetivo||null,alcance||null,descripcion_sistema||null,
       arquitectura||null,desarrollo_tecnico||null,resultados_pruebas||null,
       problemas_detectados||null,soluciones_implementadas||null,
       conclusiones||null,recomendaciones||null,anexos||null,req.user.id]);
    res.status(201).json(rows[0]);
  }catch(e){ console.error('RS POST:',e.message); res.status(500).json({error:e.message}); }
});

app.put('/api/reportes-servicio/:id', auth, async (req,res)=>{
  try{
    const b = req.body;
    const sets=[]; const vals=[]; let i=1;
    const add=(k,v)=>{ sets.push(`${k}=$${i++}`); vals.push(v); };
    if(b.titulo!==undefined)                  add('titulo',b.titulo);
    if(b.cliente_id!==undefined)              add('cliente_id',b.cliente_id||null);
    if(b.proyecto_id!==undefined)             add('proyecto_id',b.proyecto_id||null);
    if(b.fecha_reporte!==undefined)           add('fecha_reporte',b.fecha_reporte);
    if(b.fecha_servicio!==undefined)          add('fecha_servicio',b.fecha_servicio||null);
    if(b.tecnico!==undefined)                 add('tecnico',b.tecnico);
    if(b.estatus!==undefined)                 add('estatus',b.estatus);
    if(b.introduccion!==undefined)            add('introduccion',b.introduccion);
    if(b.objetivo!==undefined)                add('objetivo',b.objetivo);
    if(b.alcance!==undefined)                 add('alcance',b.alcance);
    if(b.descripcion_sistema!==undefined)     add('descripcion_sistema',b.descripcion_sistema);
    if(b.arquitectura!==undefined)            add('arquitectura',b.arquitectura);
    if(b.desarrollo_tecnico!==undefined)      add('desarrollo_tecnico',b.desarrollo_tecnico);
    if(b.resultados_pruebas!==undefined)      add('resultados_pruebas',b.resultados_pruebas);
    if(b.problemas_detectados!==undefined)    add('problemas_detectados',b.problemas_detectados);
    if(b.soluciones_implementadas!==undefined)add('soluciones_implementadas',b.soluciones_implementadas);
    if(b.conclusiones!==undefined)            add('conclusiones',b.conclusiones);
    if(b.recomendaciones!==undefined)         add('recomendaciones',b.recomendaciones);
    if(b.anexos!==undefined)                  add('anexos',b.anexos);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req,`UPDATE reportes_servicio SET ${sets.join(',')} WHERE id=$${i}`,vals);
    const rows = await QR(req,'SELECT * FROM reportes_servicio WHERE id=$1',[req.params.id]);
    res.json(rows[0]||{});
  }catch(e){ console.error('RS PUT:',e.message); res.status(500).json({error:e.message}); }
});

app.delete('/api/reportes-servicio/:id', auth, adminOnly, async (req,res)=>{
  try{ await QR(req,'DELETE FROM reportes_servicio WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){ res.status(500).json({error:e.message}); }
});

// ── PDF del Reporte de Servicio ───────────────────────────────────
app.get('/api/reportes-servicio/:id/pdf', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre,cl.rfc cliente_rfc,cl.email cliente_email,
        cl.telefono cliente_tel,cl.direccion cliente_dir,
        cl.contacto cliente_contacto,cl.ciudad cliente_ciudad,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      WHERE rs.id=$1`,[req.params.id]);
    if(!rows.length) return res.status(404).json({error:'No encontrado'});
    const r = rows[0];
    const emp = await getEmpConfig(req.user?.schema);
    const buf = await buildPDFReporteServicio(r, emp);
    savePDFToFile(buf,'reporte_servicio',r.id,r.numero_reporte,r.cliente_nombre,req.user?.id,req.user?.schema).catch(()=>{});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="RS-${r.numero_reporte||r.id}.pdf"`);
    res.send(buf);
  }catch(e){ console.error('RS PDF:',e.message); res.status(500).json({error:e.message}); }
});

// ================================================================
// LOGO
// ================================================================
app.get('/api/logo/status', auth, (req,res)=>{
  const lp=getLogoPath();
  res.json({found:!!lp, filename:lp?path.basename(lp):null});
});

// ── Lista de empresas (para selector en formulario de usuarios) ──
app.get('/api/empresas-lista', auth, async (req,res)=>{
  try{
    // Admin del sistema ve todas; admin de empresa solo ve la suya
    let rows;
    if(req.user.rol==='admin'){
      rows = await pool.query('SELECT id,nombre,slug,activa FROM public.empresas WHERE activa=true ORDER BY nombre');
    } else {
      rows = await pool.query('SELECT id,nombre,slug FROM public.empresas WHERE id=$1',[req.user.empresa_id]);
    }
    res.json(rows.rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// REPORTES DE SERVICIO
// ================================================================
app.get('/api/reportes-servicio', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      ORDER BY rs.created_at DESC`);
    res.json(rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/reportes-servicio/:id', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre, cl.rfc cliente_rfc,
        cl.email cliente_email, cl.telefono cliente_tel,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      WHERE rs.id=$1`,[req.params.id]);
    if(!rows.length) return res.status(404).json({error:'No encontrado'});
    res.json(rows[0]);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/reportes-servicio', auth, async (req,res)=>{
  try{
    const {titulo,cliente_id,proyecto_id,fecha_reporte,fecha_servicio,tecnico,
           introduccion,objetivo,alcance,descripcion_sistema,arquitectura,
           desarrollo_tecnico,resultados_pruebas,problemas_detectados,
           soluciones_implementadas,conclusiones,recomendaciones,anexos} = req.body;
    if(!titulo) return res.status(400).json({error:'Título requerido'});
    const yr = new Date().getFullYear();
    const cnt = await QR(req,'SELECT COUNT(*) val FROM reportes_servicio');
    const num = `RS-${yr}-${String(parseInt(cnt[0]?.val||0)+1).padStart(3,'0')}`;
    const rows = await QR(req,`
      INSERT INTO reportes_servicio (numero_reporte,titulo,cliente_id,proyecto_id,
        fecha_reporte,fecha_servicio,tecnico,estatus,
        introduccion,objetivo,alcance,descripcion_sistema,arquitectura,
        desarrollo_tecnico,resultados_pruebas,problemas_detectados,
        soluciones_implementadas,conclusiones,recomendaciones,anexos,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'borrador',$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
      RETURNING *`,
      [num,titulo,cliente_id||null,proyecto_id||null,
       fecha_reporte||new Date().toISOString().slice(0,10),
       fecha_servicio||null, tecnico||req.user.nombre||'VEF',
       introduccion||null,objetivo||null,alcance||null,descripcion_sistema||null,
       arquitectura||null,desarrollo_tecnico||null,resultados_pruebas||null,
       problemas_detectados||null,soluciones_implementadas||null,
       conclusiones||null,recomendaciones||null,anexos||null,req.user.id]);
    res.status(201).json(rows[0]);
  }catch(e){ console.error('RS POST:',e.message); res.status(500).json({error:e.message}); }
});

app.put('/api/reportes-servicio/:id', auth, async (req,res)=>{
  try{
    const b = req.body;
    const sets=[]; const vals=[]; let i=1;
    const add=(k,v)=>{ sets.push(`${k}=$${i++}`); vals.push(v); };
    if(b.titulo!==undefined)                  add('titulo',b.titulo);
    if(b.cliente_id!==undefined)              add('cliente_id',b.cliente_id||null);
    if(b.proyecto_id!==undefined)             add('proyecto_id',b.proyecto_id||null);
    if(b.fecha_reporte!==undefined)           add('fecha_reporte',b.fecha_reporte);
    if(b.fecha_servicio!==undefined)          add('fecha_servicio',b.fecha_servicio||null);
    if(b.tecnico!==undefined)                 add('tecnico',b.tecnico);
    if(b.estatus!==undefined)                 add('estatus',b.estatus);
    if(b.introduccion!==undefined)            add('introduccion',b.introduccion);
    if(b.objetivo!==undefined)                add('objetivo',b.objetivo);
    if(b.alcance!==undefined)                 add('alcance',b.alcance);
    if(b.descripcion_sistema!==undefined)     add('descripcion_sistema',b.descripcion_sistema);
    if(b.arquitectura!==undefined)            add('arquitectura',b.arquitectura);
    if(b.desarrollo_tecnico!==undefined)      add('desarrollo_tecnico',b.desarrollo_tecnico);
    if(b.resultados_pruebas!==undefined)      add('resultados_pruebas',b.resultados_pruebas);
    if(b.problemas_detectados!==undefined)    add('problemas_detectados',b.problemas_detectados);
    if(b.soluciones_implementadas!==undefined)add('soluciones_implementadas',b.soluciones_implementadas);
    if(b.conclusiones!==undefined)            add('conclusiones',b.conclusiones);
    if(b.recomendaciones!==undefined)         add('recomendaciones',b.recomendaciones);
    if(b.anexos!==undefined)                  add('anexos',b.anexos);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req,`UPDATE reportes_servicio SET ${sets.join(',')} WHERE id=$${i}`,vals);
    const rows = await QR(req,'SELECT * FROM reportes_servicio WHERE id=$1',[req.params.id]);
    res.json(rows[0]||{});
  }catch(e){ console.error('RS PUT:',e.message); res.status(500).json({error:e.message}); }
});

app.delete('/api/reportes-servicio/:id', auth, adminOnly, async (req,res)=>{
  try{ await QR(req,'DELETE FROM reportes_servicio WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){ res.status(500).json({error:e.message}); }
});

// ── PDF del Reporte de Servicio ───────────────────────────────────
app.get('/api/reportes-servicio/:id/pdf', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre,cl.rfc cliente_rfc,cl.email cliente_email,
        cl.telefono cliente_tel,cl.direccion cliente_dir,
        cl.contacto cliente_contacto,cl.ciudad cliente_ciudad,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      WHERE rs.id=$1`,[req.params.id]);
    if(!rows.length) return res.status(404).json({error:'No encontrado'});
    const r = rows[0];
    const emp = await getEmpConfig(req.user?.schema);
    const buf = await buildPDFReporteServicio(r, emp);
    savePDFToFile(buf,'reporte_servicio',r.id,r.numero_reporte,r.cliente_nombre,req.user?.id,req.user?.schema).catch(()=>{});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="RS-${r.numero_reporte||r.id}.pdf"`);
    res.send(buf);
  }catch(e){ console.error('RS PDF:',e.message); res.status(500).json({error:e.message}); }
});

// ================================================================
// LOGO UPLOAD (base64) — guarda como logo.png en raíz del proyecto
// ================================================================
app.post('/api/logo/upload', auth, adminOnly, async (req,res)=>{
  try {
    const { data, mime, ext } = req.body;
    if (!data) return res.status(400).json({ error: 'data requerido' });
    const allowed = ['png','jpg','jpeg'];
    const extension = (ext||'png').toLowerCase().replace('jpeg','jpg');
    if (!allowed.includes(extension)) return res.status(400).json({ error: 'Solo PNG o JPG' });
    const buf = Buffer.from(data, 'base64');
    if (buf.length > 3 * 1024 * 1024) return res.status(400).json({ error: 'Archivo muy grande (máx 3MB)' });
    // Eliminar logos anteriores
    for (const n of ['logo.png','logo.jpg','logo.jpeg','logo.PNG','logo.JPG']) {
      const p = path.join(__dirname, n);
      if (fs.existsSync(p)) try { fs.unlinkSync(p); } catch {}
    }
    const dest = path.join(__dirname, 'logo.png');
    fs.writeFileSync(dest, buf);
    // Actualizar LOGO_PATH en memoria (para PDFs inmediatos)
    global._logoPathOverride = dest;
    console.log('🖼  Logo subido:', dest, buf.length, 'bytes');
    res.json({ ok: true, path: dest, size: buf.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// EGRESOS — CRUD completo
// ================================================================
app.get('/api/egresos', auth, async (req,res)=>{
  try {
    const {mes, anio, categoria} = req.query;
    let where = 'WHERE 1=1';
    const vals = [];
    let i = 1;
    if(mes)       { where += ` AND EXTRACT(MONTH FROM fecha)=$${i++}`; vals.push(parseInt(mes)); }
    if(anio)      { where += ` AND EXTRACT(YEAR  FROM fecha)=$${i++}`; vals.push(parseInt(anio)); }
    if(categoria) { where += ` AND categoria=$${i++}`; vals.push(categoria); }
    const rows = await QR(req,`
      SELECT e.*, p.nombre proveedor_ref
      FROM egresos e
      LEFT JOIN proveedores p ON p.id=e.proveedor_id
      ${where}
      ORDER BY e.fecha DESC, e.created_at DESC`, vals);
    res.json(rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/egresos/categorias', auth, async (req,res)=>{
  const rows = await QR(req,"SELECT DISTINCT categoria FROM egresos WHERE categoria IS NOT NULL ORDER BY categoria");
  res.json(rows.map(r=>r.categoria));
});

app.post('/api/egresos', auth, async (req,res)=>{
  try {
    const {fecha,proveedor_id,proveedor_nombre,categoria,descripcion,
           subtotal,iva,total,metodo,referencia,numero_factura,notas} = req.body;
    if(!fecha) return res.status(400).json({error:'Fecha requerida'});
    const rows = await QR(req,`
      INSERT INTO egresos (fecha,proveedor_id,proveedor_nombre,categoria,descripcion,
        subtotal,iva,total,metodo,referencia,numero_factura,notas,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *`,
      [fecha, proveedor_id||null, proveedor_nombre||null, categoria||null, descripcion||null,
       parseFloat(subtotal)||0, parseFloat(iva)||0, parseFloat(total)||parseFloat(subtotal)||0,
       metodo||'Transferencia', referencia||null, numero_factura||null, notas||null, req.user.id]);
    res.status(201).json(rows[0]||{});
  }catch(e){ 
    console.error('egreso POST:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

app.put('/api/egresos/:id', auth, async (req,res)=>{
  try {
    const {fecha,proveedor_id,proveedor_nombre,categoria,descripcion,
           subtotal,iva,total,metodo,referencia,numero_factura,notas} = req.body;
    const rows = await QR(req,`
      UPDATE egresos SET fecha=$1,proveedor_id=$2,proveedor_nombre=$3,categoria=$4,
        descripcion=$5,subtotal=$6,iva=$7,total=$8,metodo=$9,referencia=$10,
        numero_factura=$11,notas=$12 WHERE id=$13 RETURNING *`,
      [fecha, proveedor_id||null, proveedor_nombre||null, categoria||null, descripcion||null,
       parseFloat(subtotal)||0, parseFloat(iva)||0, parseFloat(total)||0,
       metodo||'Transferencia', referencia||null, numero_factura||null, notas||null,
       req.params.id]);
    res.json(rows[0]||{});
  }catch(e){ 
    console.error('egreso PUT:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

app.delete('/api/egresos/:id', auth, adminOnly, async (req,res)=>{
  try { await QR(req,'DELETE FROM egresos WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){ res.status(500).json({error:e.message}); }
});

// Subir factura del egreso
app.post('/api/egresos/:id/factura', auth, async (req,res)=>{
  try {
    const {data, nombre} = req.body;
    if(!data) return res.status(400).json({error:'data requerido'});
    // Aceptar tanto base64 puro como data URL
    const b64 = data.includes(',') ? data.split(',')[1] : data;
    const buf = Buffer.from(b64,'base64');
    if(buf.length === 0) return res.status(400).json({error:'Archivo vacío o inválido'});
    if(buf.length > 30*1024*1024) return res.status(400).json({error:'Archivo muy grande (máx 30MB)'});
    await QR(req,'UPDATE egresos SET factura_pdf=$1, factura_nombre=$2 WHERE id=$3',
      [buf, nombre||'factura.pdf', req.params.id]);
    res.json({ok:true, nombre:nombre||'factura.pdf', bytes:buf.length});
  }catch(e){ 
    console.error('egr factura upload:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

app.get('/api/egresos/:id/factura', auth, async (req,res)=>{
  try {
    const rows = await QR(req,'SELECT factura_pdf,factura_nombre FROM egresos WHERE id=$1',[req.params.id]);
    if(!rows.length||!rows[0].factura_pdf) return res.status(404).json({error:'Sin factura'});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="${rows[0].factura_nombre||'factura.pdf'}"`);
    res.send(Buffer.isBuffer(rows[0].factura_pdf)?rows[0].factura_pdf:Buffer.from(rows[0].factura_pdf));
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// CONTABILIDAD ELECTRONICA SAT — Catálogo, Balanza, Pólizas
// ================================================================

// Catálogo de cuentas SAT (plan de cuentas)
app.get('/api/contabilidad/cuentas', auth, async (req,res)=>{
  try {
    const rows = await QR(req,'SELECT * FROM cat_cuentas ORDER BY num_cta');
    res.json(rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/contabilidad/cuentas', auth, async (req,res)=>{
  try {
    const {num_cta,desc_cta,cod_agrup,nivel,naturaleza,tipo_cta,sub_cta_de} = req.body;
    const r = await pool.query(
      `INSERT INTO cat_cuentas (num_cta,desc_cta,cod_agrup,nivel,naturaleza,tipo_cta,sub_cta_de)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [num_cta,desc_cta,cod_agrup||null,nivel||1,naturaleza||'D',tipo_cta||'M',sub_cta_de||null]);
    res.status(201).json(r[0]||{});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.put('/api/contabilidad/cuentas/:id', auth, async (req,res)=>{
  try {
    const {num_cta,desc_cta,cod_agrup,nivel,naturaleza,tipo_cta,sub_cta_de} = req.body;
    const r = await pool.query(
      `UPDATE cat_cuentas SET num_cta=$1,desc_cta=$2,cod_agrup=$3,nivel=$4,
       naturaleza=$5,tipo_cta=$6,sub_cta_de=$7 WHERE id=$8 RETURNING *`,
      [num_cta,desc_cta,cod_agrup||null,nivel||1,naturaleza||'D',tipo_cta||'M',sub_cta_de||null,req.params.id]);
    res.json(r[0]);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.delete('/api/contabilidad/cuentas/:id', auth, adminOnly, async (req,res)=>{
  try { await pool.query('DELETE FROM cat_cuentas WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){ res.status(500).json({error:e.message}); }
});

// Pólizas contables
app.get('/api/contabilidad/polizas', auth, async (req,res)=>{
  try {
    const {mes,anio} = req.query;
    const rows = await QR(req,`
      SELECT p.*, array_agg(row_to_json(d.*) ORDER BY d.id) movs
      FROM polizas p
      LEFT JOIN polizas_detalle d ON d.poliza_id=p.id
      WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM p.fecha)=$1::int)
        AND ($2::int IS NULL OR EXTRACT(YEAR  FROM p.fecha)=$2::int)
      GROUP BY p.id ORDER BY p.fecha,p.num_un_iden_pol`,[mes||null,anio||null]);
    res.json(rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/contabilidad/polizas', auth, async (req,res)=>{
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const {fecha,tipo_pol,num_un_iden_pol,concepto,movimientos} = req.body;
    const pr = await client.query(
      `INSERT INTO polizas (fecha,tipo_pol,num_un_iden_pol,concepto,created_by)
       VALUES ($1,$2,$3,$4,$5) RETURNING *`,
      [fecha,tipo_pol||'D',num_un_iden_pol,concepto||'',req.user.id]);
    const pol = pr[0];
    for(const m of (movimientos||[])){
      await client.query(
        `INSERT INTO polizas_detalle (poliza_id,num_cta,concepto,debe,haber,num_cta_banco,
         banco_en_ext,dig_iden_ban,fec_cap,num_refer,monto_total,tipo_moneda,tip_camb,
         num_factura_pago,folio_fiscal0,rfc_emp,monto_tot_gravado)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
        [pol.id,m.num_cta,m.concepto||'',parseFloat(m.debe)||0,parseFloat(m.haber)||0,
         m.num_cta_banco||null,m.banco_en_ext||null,m.dig_iden_ban||null,
         m.fec_cap||null,m.num_refer||null,m.monto_total||null,
         m.tipo_moneda||null,m.tip_camb||null,m.num_factura||null,
         m.folio_fiscal||null,m.rfc_emp||null,m.monto_tot_gravado||null]);
    }
    await client.query('COMMIT');
    res.status(201).json(pol);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.delete('/api/contabilidad/polizas/:id', auth, adminOnly, async (req,res)=>{
  try {
    await pool.query('DELETE FROM polizas_detalle WHERE poliza_id=$1',[req.params.id]);
    await pool.query('DELETE FROM polizas WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ── Generar XML Catálogo de Cuentas ─────────────────────────────
app.get('/api/contabilidad/xml/catalogo', auth, async (req,res)=>{
  try {
    const {anio,mes} = req.query;
    const emp = (await QR(req,'SELECT * FROM empresa_config LIMIT 1'))[0]||{};
    const cuentas = await QR(req,'SELECT * FROM cat_cuentas ORDER BY num_cta');
    const rfc = (emp.rfc||'RFC000000000').toUpperCase();
    let xml = `<?xml version="1.0" encoding="UTF-8"?>
`;
    xml += `<catalogocuentas:Catalogo xmlns:catalogocuentas="http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/CatalogoCuentas"
`;
    xml += `  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
`;
    xml += `  xsi:schemaLocation="http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/CatalogoCuentas http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/CatalogoCuentas/CatalogoCuentas_1_3.xsd"
`;
    xml += `  Version="1.3" RFC="${rfc}" Mes="${String(mes||1).padStart(2,'0')}" Anio="${anio||new Date().getFullYear()}" TipoEnvio="N">
`;
    for(const c of cuentas){
      xml += `  <catalogocuentas:Ctas NumCta="${esc2(c.num_cta)}" Desc="${esc2(c.desc_cta)}" CodAgrup="${esc2(c.cod_agrup||c.num_cta)}" Nivel="${c.nivel||1}" Natur="${c.naturaleza||'D'}"`;
      if(c.tipo_cta) xml += ` TipoCta="${esc2(c.tipo_cta)}"`;
      if(c.sub_cta_de) xml += ` SubCtaDe="${esc2(c.sub_cta_de)}"`;
      xml += `/>
`;
    }
    xml += `</catalogocuentas:Catalogo>`;
    const nombre = `${rfc}${anio||new Date().getFullYear()}${String(mes||1).padStart(2,'0')}CT.xml`;
    res.setHeader('Content-Type','application/xml; charset=UTF-8');
    res.setHeader('Content-Disposition',`attachment; filename="${nombre}"`);
    res.send(xml);
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ── Generar XML Balanza de Comprobación ─────────────────────────
app.get('/api/contabilidad/xml/balanza', auth, async (req,res)=>{
  try {
    const {anio,mes,tipo_envio='N',fecha_mod_bal} = req.query;
    const emp = (await QR(req,'SELECT * FROM empresa_config LIMIT 1'))[0]||{};
    const rfc = (emp.rfc||'RFC000000000').toUpperCase();
    // Agrupar movimientos por cuenta
    const movs = await QR(req,`
      SELECT d.num_cta,
        COALESCE(SUM(d.debe),0) debe, COALESCE(SUM(d.haber),0) haber
      FROM polizas_detalle d
      JOIN polizas p ON p.id=d.poliza_id
      WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM p.fecha)=$1::int)
        AND ($2::int IS NULL OR EXTRACT(YEAR  FROM p.fecha)=$2::int)
      GROUP BY d.num_cta ORDER BY d.num_cta`,[mes||null,anio||null]);
    const m = String(mes||1).padStart(2,'0');
    const a = anio||new Date().getFullYear();
    let xml = `<?xml version="1.0" encoding="UTF-8"?>
`;
    xml += `<BCE:Balanza xmlns:BCE="http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/BalanzaComprobacion"
`;
    xml += `  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
`;
    xml += `  xsi:schemaLocation="http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/BalanzaComprobacion http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/BalanzaComprobacion/BalanzaComprobacion_1_3.xsd"
`;
    xml += `  Version="1.3" RFC="${rfc}" Mes="${m}" Anio="${a}" TipoEnvio="${tipo_envio}"`;
    if(tipo_envio==='C'&&fecha_mod_bal) xml += ` FechaModBal="${fecha_mod_bal}"`;
    xml += `>
`;
    for(const mv of movs){
      const saldoIni = 0; // Simplificado — en producción calcular saldo inicial
      const saldoFin = saldoIni + parseFloat(mv.debe) - parseFloat(mv.haber);
      xml += `  <BCE:Ctas NumCta="${esc2(mv.num_cta)}" SaldoIni="${saldoIni.toFixed(2)}" `;
      xml += `Debe="${parseFloat(mv.debe).toFixed(2)}" Haber="${parseFloat(mv.haber).toFixed(2)}" `;
      xml += `SaldoFin="${saldoFin.toFixed(2)}"/>
`;
    }
    xml += `</BCE:Balanza>`;
    const nombre = `${rfc}${a}${m}BN.xml`;
    res.setHeader('Content-Type','application/xml; charset=UTF-8');
    res.setHeader('Content-Disposition',`attachment; filename="${nombre}"`);
    res.send(xml);
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ── Generar XML Pólizas ──────────────────────────────────────────
app.get('/api/contabilidad/xml/polizas', auth, async (req,res)=>{
  try {
    const {anio,mes,tipo_sol,num_orden,num_tramite,rfc_sol} = req.query;
    const emp = (await QR(req,'SELECT * FROM empresa_config LIMIT 1'))[0]||{};
    const rfc = (emp.rfc||'RFC000000000').toUpperCase();
    const pols = await QR(req,`
      SELECT p.*, json_agg(row_to_json(d.*) ORDER BY d.id) movs
      FROM polizas p LEFT JOIN polizas_detalle d ON d.poliza_id=p.id
      WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM p.fecha)=$1::int)
        AND ($2::int IS NULL OR EXTRACT(YEAR  FROM p.fecha)=$2::int)
      GROUP BY p.id ORDER BY p.fecha,p.num_un_iden_pol`,[mes||null,anio||null]);
    const m = String(mes||1).padStart(2,'0');
    const a = anio||new Date().getFullYear();
    let xml = `<?xml version="1.0" encoding="UTF-8"?>
`;
    xml += `<PLZ:Polizas xmlns:PLZ="http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/PolizasPeriodo"
`;
    xml += `  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
`;
    xml += `  xsi:schemaLocation="http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/PolizasPeriodo http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/PolizasPeriodo/PolizasPeriodo_1_3.xsd"
`;
    xml += `  Version="1.3" RFC="${rfc}" Mes="${m}" Anio="${a}" TipoSolicitud="${tipo_sol||'AF'}"`;
    if(tipo_sol==='OF') xml += ` NumOrden="${num_orden||''}"`;
    if(tipo_sol==='CO') xml += ` NumTramite="${num_tramite||''}"`;
    if(rfc_sol) xml += ` RfcSolicitante="${rfc_sol}"`;
    xml += `>
`;
    for(const p of pols){
      const movs = Array.isArray(p.movs)?p.movs.filter(Boolean):[];
      const totDebe  = movs.reduce((s,d)=>s+parseFloat(d.debe||0),0);
      const totHaber = movs.reduce((s,d)=>s+parseFloat(d.haber||0),0);
      const fec = p.fecha?new Date(p.fecha).toISOString().slice(0,10):new Date().toISOString().slice(0,10);
      xml += `  <PLZ:Poliza Fecha="${fec}" NumUnIdenPol="${esc2(p.num_un_iden_pol||'1')}" Concepto="${esc2(p.concepto||'')}">
`;
      for(const d of movs){
        xml += `    <PLZ:Transaccion NumCta="${esc2(d.num_cta)}" Concepto="${esc2(d.concepto||'')}" Debe="${parseFloat(d.debe||0).toFixed(2)}" Haber="${parseFloat(d.haber||0).toFixed(2)}"`;
        if(d.num_refer)          xml += ` NumRef="${esc2(d.num_refer)}"`;
        if(d.folio_fiscal0)      xml += ` FolioFiscal0="${esc2(d.folio_fiscal0)}"`;
        if(d.rfc_emp)            xml += ` RfcEmisor="${esc2(d.rfc_emp)}"`;
        if(d.num_factura_pago)   xml += ` NumFactura="${esc2(d.num_factura_pago)}"`;
        if(d.monto_total)        xml += ` MontoTotal="${parseFloat(d.monto_total).toFixed(2)}"`;
        if(d.tipo_moneda)        xml += ` TipoMoneda="${esc2(d.tipo_moneda)}"`;
        if(d.tip_camb)           xml += ` TipCamb="${parseFloat(d.tip_camb).toFixed(2)}"`;
        xml += `/>
`;
      }
      xml += `  </PLZ:Poliza>
`;
    }
    xml += `</PLZ:Polizas>`;
    const nombre = `${rfc}${a}${m}PL.xml`;
    res.setHeader('Content-Type','application/xml; charset=UTF-8');
    res.setHeader('Content-Disposition',`attachment; filename="${nombre}"`);
    res.send(xml);
  }catch(e){ res.status(500).json({error:e.message}); }
});

// Helper XML escape
function esc2(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&apos;'); }

// ================================================================
// EMPRESA CONFIG — GET y PUT (upsert)
// ================================================================
app.get('/api/empresa', auth, async (req,res)=>{
  try {
    const r = await QR(req,'SELECT * FROM empresa_config ORDER BY id LIMIT 1');
    res.json(r[0] || {});
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.put('/api/empresa', auth, adminOnly, async (req,res)=>{
  try {
    const b = req.body;
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    // Leer columnas SIEMPRE frescas de la BD (sin cache para este endpoint crítico)
    const colRes = await pool.query(
      `SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name='empresa_config'`,
      [schema]);
    const empCols = new Set(colRes.rows.map(r=>r.column_name));
    if(empCols.size === 0) return res.status(500).json({error:'Tabla empresa_config no encontrada'});
    
    const sets=[]; const vals=[];let i=1;
    const add=(col,v,transform)=>{
      if(!empCols.has(col)) return;
      const val = transform ? transform(v) : v;
      if(v!==undefined && v!==null && v!=='' && String(v).trim()!==''){
        sets.push(`${col}=$${i++}`); vals.push(val);
      }
    };
    add('nombre',          b.nombre);
    add('razon_social',    b.razon_social);
    add('rfc',             b.rfc);
    add('regimen_fiscal',  b.regimen_fiscal);
    add('contacto',        b.contacto);
    add('telefono',        b.telefono);
    add('email',           b.email);
    add('direccion',       b.direccion);
    add('ciudad',          b.ciudad);
    add('estado',          b.estado);
    add('cp',              b.cp);
    add('pais',            b.pais);
    add('sitio_web',       b.sitio_web);
    add('moneda_default',  b.moneda_default);
    if(b.iva_default!==undefined&&b.iva_default!==''&&empCols.has('iva_default')){sets.push(`iva_default=$${i++}`);vals.push(parseFloat(b.iva_default)||16);}
    if(b.margen_ganancia!==undefined&&b.margen_ganancia!==''&&empCols.has('margen_ganancia')){sets.push(`margen_ganancia=$${i++}`);vals.push(parseFloat(b.margen_ganancia)||0);}
    add('smtp_host',  b.smtp_host);
    if(b.smtp_port && empCols.has('smtp_port')){sets.push(`smtp_port=$${i++}`);vals.push(parseInt(b.smtp_port)||465);}
    add('smtp_user',  b.smtp_user);
    if(b.smtp_pass && String(b.smtp_pass).trim()) add('smtp_pass', b.smtp_pass);
    add('db_host',    b.db_host);
    if(b.db_port && empCols.has('db_port')){sets.push(`db_port=$${i++}`);vals.push(parseInt(b.db_port)||5432);}
    add('db_name',    b.db_name);
    add('notas_factura',    b.notas_factura);
    add('notas_cotizacion', b.notas_cotizacion);

    if(!sets.length) return res.status(400).json({error:'Nada que actualizar'});
    sets.push(`updated_at=NOW()`);

    const ex = await QR(req,'SELECT id FROM empresa_config LIMIT 1');
    if(ex.length > 0){
      vals.push(ex[0].id);
      await QR(req,`UPDATE empresa_config SET ${sets.join(',')} WHERE id=$${i}`,vals);
    } else {
      // Get company name from public.empresas for this schema
      const empNameR = await pool.query('SELECT nombre FROM public.empresas WHERE id=$1',[req.user.empresa_id||null]);
      const empNameDefault = empNameR.rows[0]?.nombre || 'Mi Empresa';
      await QR(req,`INSERT INTO empresa_config (nombre,pais,moneda_default,iva_default) VALUES ($1,'México','USD',16)`,[empNameDefault]);
      const [ecRow] = await QR(req,'SELECT id FROM empresa_config LIMIT 1');
      vals.push(ecRow?.id);
      await QR(req,`UPDATE empresa_config SET ${sets.join(',')} WHERE id=$${i}`,vals);
    }
    const [updated] = await QR(req,'SELECT * FROM empresa_config ORDER BY id LIMIT 1');
    res.json(updated || {});
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// EMAIL TEST
// ================================================================
app.post('/api/email/test', auth, async (req,res)=>{
  const { to } = req.body;
  if (!to) return res.status(400).json({ error: 'to requerido' });
  try {
    const schema = req.user?.schema || req.user?.schema_name || global._defaultSchema;
    const dynMailerTest = await getMailer(schema);
    const fromEmailTest = await getFromEmail(schema);
    const empTest = (await Q('SELECT nombre,telefono,email,smtp_host,smtp_port,smtp_user FROM empresa_config LIMIT 1',[],schema))[0]||{};
    const nomTest = empTest.nombre || VEF_NOMBRE;
    // Verificar que hay configuración SMTP
    if(!empTest.smtp_host && !process.env.SMTP_HOST){
      return res.status(400).json({error:'No hay servidor SMTP configurado. Ve a Configuración → Correo y guarda los datos SMTP.'});
    }
    if(!empTest.smtp_user && !process.env.SMTP_USER){
      return res.status(400).json({error:'No hay usuario SMTP configurado. Ve a Configuración → Correo y guarda los datos.'});
    }
    await dynMailerTest.sendMail({
      from: `"${nomTest}" <${fromEmailTest}>`,
      to,
      subject: `✅ Prueba de correo — ${nomTest}`,
      html: `<div style="font-family:Arial,sans-serif;max-width:500px">
        <div style="background:#0D2B55;padding:16px;text-align:center">
          <h2 style="color:#fff;margin:0">${nomTest}</h2>
          <p style="color:#A8C5F0;margin:4px 0">Prueba de configuración SMTP</p>
        </div>
        <div style="padding:20px">
          <p>✅ El correo está correctamente configurado.</p>
          <p><b>Servidor:</b> ${empTest.smtp_host||process.env.SMTP_HOST} · Puerto ${empTest.smtp_port||process.env.SMTP_PORT||465}<br>
          <b>Cuenta:</b> ${empTest.smtp_user||process.env.SMTP_USER}<br>
          <b>Enviado a:</b> ${to}<br>
          <b>Fecha:</b> ${new Date().toLocaleString('es-MX')}</p>
        </div>
        <div style="background:#0D2B55;padding:10px;text-align:center;color:#A8C5F0;font-size:12px">
          ${nomTest} · ${empTest.telefono||VEF_TELEFONO} · ${empTest.email||fromEmailTest}
        </div>
      </div>`
    });
    res.json({ ok: true, msg: `Correo enviado a ${to} desde ${fromEmailTest}` });
  } catch(e) {
    console.error('Email test error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ================================================================
// TAREAS — CRUD completo
// ================================================================
app.get('/api/tareas', auth, async (req,res)=>{
  try {
    const rows = await QR(req,`
      SELECT t.*, 
        p.nombre proyecto_nombre,
        u.nombre asignado_nombre,
        c.nombre creador_nombre
      FROM tareas t
      LEFT JOIN proyectos p ON p.id=t.proyecto_id
      LEFT JOIN usuarios u ON u.id=t.asignado_a
      LEFT JOIN usuarios c ON c.id=t.creado_por
      ORDER BY 
        CASE t.prioridad WHEN 'urgente' THEN 1 WHEN 'alta' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END,
        t.fecha_vencimiento ASC NULLS LAST, t.created_at DESC`);
    res.json(rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/tareas', auth, async (req,res)=>{
  try {
    const {titulo,descripcion,proyecto_id,asignado_a,prioridad,estatus,
           fecha_inicio,fecha_vencimiento,notas} = req.body;
    if(!titulo) return res.status(400).json({error:'Título requerido'});
    const rows = await QR(req,`
      INSERT INTO tareas (titulo,descripcion,proyecto_id,asignado_a,creado_por,
        prioridad,estatus,fecha_inicio,fecha_vencimiento,notas)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [titulo,descripcion||null,proyecto_id||null,asignado_a||null,req.user.id,
       prioridad||'normal',estatus||'pendiente',
       fecha_inicio||null,fecha_vencimiento||null,notas||null]);
    res.status(201).json(rows[0]||{});
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.put('/api/tareas/:id', auth, async (req,res)=>{
  try {
    const {titulo,descripcion,proyecto_id,asignado_a,prioridad,estatus,
           fecha_inicio,fecha_vencimiento,notas} = req.body;
    const fechaComp = estatus==='completada' ? 'NOW()' : 'NULL';
    const rows = await QR(req,`
      UPDATE tareas SET titulo=$1,descripcion=$2,proyecto_id=$3,asignado_a=$4,
        prioridad=$5,estatus=$6,fecha_inicio=$7,fecha_vencimiento=$8,notas=$9,
        fecha_completada=${fechaComp},updated_at=NOW()
      WHERE id=$10 RETURNING *`,
      [titulo,descripcion||null,proyecto_id||null,asignado_a||null,
       prioridad||'normal',estatus||'pendiente',
       fecha_inicio||null,fecha_vencimiento||null,notas||null,req.params.id]);
    res.json(rows[0]||{});
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.delete('/api/tareas/:id', auth, async (req,res)=>{
  try { await QR(req,'DELETE FROM tareas WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){ res.status(500).json({error:e.message}); }
});

// ── Lista de empresas (para selector en formulario de usuarios) ──
app.get('/api/empresas-lista', auth, async (req,res)=>{
  try{
    // Admin del sistema ve todas; admin de empresa solo ve la suya
    let rows;
    if(req.user.rol==='admin'){
      rows = await pool.query('SELECT id,nombre,slug,activa FROM public.empresas WHERE activa=true ORDER BY nombre');
    } else {
      rows = await pool.query('SELECT id,nombre,slug FROM public.empresas WHERE id=$1',[req.user.empresa_id]);
    }
    res.json(rows.rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// REPORTES SAT — DIOT, ingresos, egresos
// ================================================================
app.get('/api/reportes/sat/ingresos', auth, async (req,res)=>{
  try {
    const {mes,anio} = req.query;
    const rows = await QR(req,`
      SELECT f.numero_factura, f.fecha_emision,
        COALESCE(cl.nombre,'—') cliente, cl.rfc rfc_cliente,
        f.subtotal, f.iva, f.total, f.moneda, f.estatus
      FROM facturas f
      LEFT JOIN clientes cl ON cl.id=f.cliente_id
      WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM f.fecha_emision)=$1::int)
        AND ($2::int IS NULL OR EXTRACT(YEAR FROM f.fecha_emision)=$2::int)
      ORDER BY f.fecha_emision DESC`,[mes||null,anio||null]);
    res.json(rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/reportes/sat/egresos', auth, async (req,res)=>{
  try {
    const {mes,anio} = req.query;
    // OC Proveedores
    const ocs = await QR(req,`
      SELECT op.numero_op numero_doc, op.fecha_emision fecha,
        COALESCE(pr.nombre,'—') proveedor, pr.rfc rfc_proveedor,
        op.total, op.moneda, op.estatus, 'OC Proveedor' tipo,
        NULL subtotal, NULL iva, NULL categoria
      FROM ordenes_proveedor op
      LEFT JOIN proveedores pr ON pr.id=op.proveedor_id
      WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM op.fecha_emision)=$1::int)
        AND ($2::int IS NULL OR EXTRACT(YEAR FROM op.fecha_emision)=$2::int)
      ORDER BY op.fecha_emision DESC`,[mes||null,anio||null]);
    // Egresos directos
    let egs = [];
    try {
      egs = await QR(req,`
        SELECT COALESCE(e.numero_factura,'—') numero_doc, e.fecha,
          COALESCE(e.proveedor_nombre,'—') proveedor, NULL rfc_proveedor,
          e.total, 'MXN' moneda, 'registrado' estatus, e.categoria tipo,
          e.subtotal, e.iva, e.categoria
        FROM egresos e
        WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM e.fecha)=$1::int)
          AND ($2::int IS NULL OR EXTRACT(YEAR FROM e.fecha)=$2::int)
        ORDER BY e.fecha DESC`,[mes||null,anio||null]);
    } catch(e2){ /* tabla egresos puede no existir aún */ }
    res.json([...ocs, ...egs]);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/reportes/sat/resumen', auth, async (req,res)=>{
  try {
    const {mes,anio} = req.query;
    const [ing,oc,cob,emp] = await Promise.all([
      Q(`SELECT COALESCE(SUM(subtotal),0) sub, COALESCE(SUM(iva),0) iva, COALESCE(SUM(total),0) tot
         FROM facturas WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM fecha_emision)=$1::int)
         AND ($2::int IS NULL OR EXTRACT(YEAR FROM fecha_emision)=$2::int)`,[mes||null,anio||null]),
      Q(`SELECT COALESCE(SUM(total),0) tot FROM ordenes_proveedor
         WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM fecha_emision)=$1::int)
         AND ($2::int IS NULL OR EXTRACT(YEAR FROM fecha_emision)=$2::int)`,[mes||null,anio||null]),
      Q(`SELECT COALESCE(SUM(monto),0) tot FROM pagos
         WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM fecha)=$1::int)
         AND ($2::int IS NULL OR EXTRACT(YEAR FROM fecha)=$2::int)`,[mes||null,anio||null]),
      Q('SELECT * FROM empresa_config LIMIT 1'),
    ]);
    // Sumar egresos directos si la tabla existe
    let egDir = {sub:0, iva:0, tot:0};
    try {
      const egRes = await QR(req,`SELECT COALESCE(SUM(subtotal),0) sub, COALESCE(SUM(iva),0) iva, COALESCE(SUM(total),0) tot
        FROM egresos WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM fecha)=$1::int)
        AND ($2::int IS NULL OR EXTRACT(YEAR FROM fecha)=$2::int)`,[mes||null,anio||null]);
      egDir = egRes[0]||egDir;
    } catch(e2){}
    const totEgresos = parseFloat(oc[0].tot||0) + parseFloat(egDir.tot||0);
    const totIvaEgr  = parseFloat(egDir.iva||0);
    const totSubEgr  = parseFloat(egDir.sub||0);
    res.json({
      ingresos: ing[0],
      egresos: { tot: totEgresos, oc: parseFloat(oc[0].tot||0),
                 egr_sub: totSubEgr, egr_iva: totIvaEgr, egr_tot: parseFloat(egDir.tot||0) },
      cobrado: cob[0],
      empresa: emp[0]||{}, mes, anio
    });
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// PDFS GUARDADOS — listar, descargar, guardar automático
// ================================================================
// ── PDF Reporte de Servicio ──────────────────────────────────────
async function buildPDFReporteServicio(r, emp={}) {
  return new Promise((resolve,reject)=>{
    const doc = new PDFKit({margin:50, size:'A4',
      info:{Title:'Reporte de Servicio '+r.numero_reporte, Author:emp.nombre||VEF_NOMBRE}});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>resolve(Buffer.concat(ch))); doc.on('error',reject);
    const M=50, W=495, AZUL='#0D2B55', AZUL_MED='#1A4A8A', GRIS='#f8fafc', TEXTO='#1e293b';
    const _lp = getLogoPath();
    let secNum = 0;

    // ── PORTADA ──────────────────────────────────────────────
    // Header azul con logo y nombre empresa
    doc.rect(M,30,W,90).fill(AZUL);
    if(_lp){
      doc.rect(M,30,120,90).fill('#fff');
      try{ doc.image(_lp, M+6,34,{fit:[108,82],align:'center',valign:'center'}); }catch(e){}
    }
    const tx = _lp?M+130:M+14;
    const tw = _lp?W-140:W-28;
    const empNom = emp.nombre||VEF_NOMBRE;
    doc.fillColor('#fff').fontSize(13).font('Helvetica-Bold').text(empNom, tx, 44, {width:tw});
    if(emp.rfc) doc.fontSize(8).font('Helvetica').fillColor('#A8C5F0').text('RFC: '+emp.rfc, tx, 60, {width:tw});
    if(emp.telefono||emp.email){
      const contact=[emp.telefono,emp.email].filter(Boolean).join('  |  ');
      doc.fontSize(8).font('Helvetica').fillColor('#A8C5F0').text(contact, tx, 71, {width:tw});
    }

    // Título del documento
    doc.moveDown(2.5);
    doc.fillColor(AZUL).fontSize(22).font('Helvetica-Bold')
       .text('REPORTE DE SERVICIO', M, doc.y, {width:W, align:'center'});
    doc.moveDown(0.4);
    doc.fillColor(AZUL_MED).fontSize(14).font('Helvetica')
       .text(r.titulo||'Sin título', M, doc.y, {width:W, align:'center'});
    doc.moveDown(1.5);

    // ── CAJA DATOS DEL REPORTE ───────────────────────────────
    const bY = doc.y;
    const dataRows=[
      ['No. Reporte:', r.numero_reporte||'—',       'Fecha Reporte:',  fmt(r.fecha_reporte)||'—'],
      ['Fecha Servicio:', fmt(r.fecha_servicio)||'—','Técnico:',        r.tecnico||'—'],
      ['Estatus:',    (r.estatus||'borrador').toUpperCase(), 'Proyecto:', r.proyecto_nombre||'—'],
    ];
    const boxH = 10 + dataRows.length*22 + 8;
    doc.rect(M, bY, W, boxH).fill(GRIS).stroke('#e2e8f0');
    const col1=M+16, col2=M+W/2+16, colW=(W/2)-24;
    let dy = bY+10;
    for(const row of dataRows){
      doc.fillColor(AZUL).fontSize(8).font('Helvetica-Bold').text(row[0],col1,dy,{width:88});
      doc.fillColor(TEXTO).fontSize(8).font('Helvetica').text(row[1],col1+90,dy,{width:colW-90});
      doc.fillColor(AZUL).fontSize(8).font('Helvetica-Bold').text(row[2],col2,dy,{width:88});
      doc.fillColor(TEXTO).fontSize(8).font('Helvetica').text(row[3],col2+90,dy,{width:colW-90});
      dy+=22;
    }
    doc.y = bY+boxH+12;

    // ── CAJA DATOS DEL CLIENTE ────────────────────────────────
    if(r.cliente_nombre){
      const cY = doc.y;
      doc.rect(M, cY, W, 36).fill(AZUL_MED);
      doc.fillColor('#fff').fontSize(9).font('Helvetica-Bold')
         .text('DATOS DEL CLIENTE', M+14, cY+10, {width:W-28});
      doc.y = cY+36;
      const cboxH = 10 + 4*20 + 8;
      doc.rect(M, doc.y, W, cboxH).fill('#f0f4ff').stroke('#bfdbfe');
      const cY2 = doc.y;
      const cRows=[
        ['Cliente / Razón Social:', r.cliente_nombre||'—',   'RFC:',       r.cliente_rfc||'—'],
        ['Contacto:',               r.cliente_contacto||'—', 'Ciudad:',    r.cliente_ciudad||'—'],
        ['Teléfono:',               r.cliente_tel||'—',      'Email:',     r.cliente_email||'—'],
        ['Dirección:',              r.cliente_dir||'—',       '',           ''],
      ];
      let cdy = cY2+10;
      for(const row of cRows){
        doc.fillColor(AZUL).fontSize(8).font('Helvetica-Bold').text(row[0],col1,cdy,{width:110});
        doc.fillColor(TEXTO).fontSize(8).font('Helvetica').text(row[1],col1+112,cdy,{width:colW-112});
        if(row[2]){
          doc.fillColor(AZUL).fontSize(8).font('Helvetica-Bold').text(row[2],col2,cdy,{width:88});
          doc.fillColor(TEXTO).fontSize(8).font('Helvetica').text(row[3],col2+90,cdy,{width:colW-90});
        }
        cdy+=20;
      }
      doc.y = cY2+cboxH+10;
    }

    // ── ÍNDICE ────────────────────────────────────────────────
    doc.addPage();
    doc.fillColor(AZUL).fontSize(16).font('Helvetica-Bold').text('ÍNDICE', M, 50, {width:W});
    doc.moveTo(M,70).lineTo(M+W,70).lineWidth(2).strokeColor(AZUL_MED).stroke();
    doc.moveDown(0.8);

    const secciones = [
      {num:1, titulo:'Introducción',             campo:'introduccion'},
      {num:2, titulo:'Objetivo',                  campo:'objetivo'},
      {num:3, titulo:'Alcance',                   campo:'alcance'},
      {num:4, titulo:'Descripción del Sistema',   campo:'descripcion_sistema'},
      {num:5, titulo:'Arquitectura del Sistema',  campo:'arquitectura'},
      {num:6, titulo:'Desarrollo Técnico',        campo:'desarrollo_tecnico'},
      {num:7, titulo:'Resultados de Pruebas',     campo:'resultados_pruebas'},
      {num:8, titulo:'Problemas Detectados',      campo:'problemas_detectados'},
      {num:9, titulo:'Soluciones Implementadas',  campo:'soluciones_implementadas'},
      {num:10,titulo:'Conclusiones',              campo:'conclusiones'},
      {num:11,titulo:'Recomendaciones',           campo:'recomendaciones'},
      {num:12,titulo:'Anexos',                    campo:'anexos'},
    ].filter(s => r[s.campo] && String(r[s.campo]).trim());

    let iy = doc.y;
    for(const s of secciones){
      const hasContent = r[s.campo]&&String(r[s.campo]).trim();
      if(!hasContent) continue;
      doc.fillColor(AZUL_MED).fontSize(10).font('Helvetica-Bold')
         .text(`${s.num}.`, M, iy, {width:24});
      doc.fillColor(TEXTO).fontSize(10).font('Helvetica')
         .text(s.titulo, M+28, iy, {width:W-80});
      doc.fillColor('#94a3b8').fontSize(10).font('Helvetica')
         .text('..................', M+W-60, iy, {width:60,align:'right'});
      iy += 22;
    }

    // ── SECCIONES DEL REPORTE ────────────────────────────────
    function addSeccion(titulo, contenido){
      if(!contenido||!String(contenido).trim()) return;
      secNum++;
      doc.addPage();
      // Encabezado de sección
      doc.rect(M, 30, W, 36).fill(AZUL);
      doc.fillColor('#fff').fontSize(14).font('Helvetica-Bold')
         .text(`${secNum}. ${titulo}`, M+14, 41, {width:W-28});
      // Número de reporte en esquina
      doc.fillColor('#A8C5F0').fontSize(8).font('Helvetica')
         .text(r.numero_reporte||'', M+W-80, 37, {width:80, align:'right'});
      doc.y = 80;
      doc.fillColor(TEXTO).fontSize(10).font('Helvetica')
         .text(String(contenido).trim(), M, doc.y, {width:W, lineGap:4});
    }

    addSeccion('Introducción',            r.introduccion);
    addSeccion('Objetivo',                r.objetivo);
    addSeccion('Alcance',                 r.alcance);
    addSeccion('Descripción del Sistema', r.descripcion_sistema);
    addSeccion('Arquitectura del Sistema',r.arquitectura);
    addSeccion('Desarrollo Técnico',      r.desarrollo_tecnico);
    addSeccion('Resultados de Pruebas',   r.resultados_pruebas);
    addSeccion('Problemas Detectados',    r.problemas_detectados);
    addSeccion('Soluciones Implementadas',r.soluciones_implementadas);
    addSeccion('Conclusiones',            r.conclusiones);
    addSeccion('Recomendaciones',         r.recomendaciones);
    addSeccion('Anexos',                  r.anexos);

    // ── PIE EN CADA PÁGINA ───────────────────────────────────
    const pages = doc.bufferedPageRange();
    for(let i=0; i<doc._pageBuffer.length; i++){
      doc.switchToPage(i);
      const py = doc.page.height - 40;
      doc.rect(M, py-8, W, 28).fill(AZUL);
      doc.fillColor('#fff').fontSize(8).font('Helvetica-Bold')
         .text(`${empNom}  |  ${r.numero_reporte||''}  |  Pág. ${i+1}`, M, py, {width:W, align:'center'});
    }

    doc.end();
  });
}

const pdfDir = path.join(__dirname,'pdfs_guardados');

// Helper para guardar PDF en disco y registrar en BD
async function savePDFToFile(buf, tipo, refId, numDoc, clienteProv, userId, schema=null) {
  try {
    const sch = schema || global._defaultSchema || 'emp_vef';
    if(!fs.existsSync(pdfDir)) fs.mkdirSync(pdfDir,{recursive:true});
    const ts = new Date().toISOString().replace(/[:.]/g,'-').slice(0,19);
    const nombre = `${tipo}_${numDoc||refId}_${ts}.pdf`.replace(/[^a-zA-Z0-9._-]/g,'_');
    const ruta = path.join(pdfDir, nombre);
    try { fs.writeFileSync(ruta, buf); } catch(fe){ console.warn('PDF disk save:',fe.message); }
    // Detectar columnas reales de pdfs_guardados
    const C = await getCols(sch, 'pdfs_guardados');
    const cols = ['tipo','referencia_id','numero_doc','cliente_proveedor','nombre_archivo','tamanio_bytes'];
    const vals = [tipo, refId, numDoc||String(refId), clienteProv||'—', nombre, buf.length];
    if(C.has('ruta_archivo')){ cols.push('ruta_archivo'); vals.push(ruta); }
    if(C.has('pdf_data'))    { cols.push('pdf_data');     vals.push(buf);  }
    if(C.has('generado_por')){ cols.push('generado_por'); vals.push(userId||null); }
    const ph = vals.map((_,i)=>`$${i+1}`).join(',');
    await Q(`INSERT INTO pdfs_guardados (${cols.join(',')}) VALUES (${ph})`, vals, sch);
    return nombre;
  } catch(e){ console.error('savePDF error:',e.message); return null; }
}

app.get('/api/pdfs', auth, async (req,res)=>{
  try {
    const pdfsCols = await getCols(req.user?.schema||global._defaultSchema||'emp_vef','pdfs_guardados');
    const rutaCol = pdfsCols.has('ruta_archivo') ? 'p.ruta_archivo,' : "'—' AS ruta_archivo,";
    const rows = await QR(req,`SELECT p.id, p.tipo, p.referencia_id, p.numero_doc,
      p.cliente_proveedor, ${rutaCol} p.nombre_archivo, p.tamanio_bytes,
      p.created_at, u.nombre generado_nombre,
      (p.pdf_data IS NOT NULL) AS tiene_dato
      FROM pdfs_guardados p LEFT JOIN usuarios u ON u.id=p.generado_por
      ORDER BY p.created_at DESC LIMIT 200`);
    res.json(rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/pdfs/:id/descargar', auth, async (req,res)=>{
  try {
    const rows = await QR(req,'SELECT * FROM pdfs_guardados WHERE id=$1',[req.params.id]);
    if(!rows.length) return res.status(404).json({error:'PDF no encontrado'});
    const p = rows[0];
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`attachment; filename="${p.nombre_archivo||'documento.pdf'}"`);
    // 1. Intentar desde BD (pdf_data)
    if(p.pdf_data){
      return res.send(Buffer.isBuffer(p.pdf_data)?p.pdf_data:Buffer.from(p.pdf_data));
    }
    // 2. Intentar desde disco
    if(p.ruta_archivo && fs.existsSync(p.ruta_archivo)){
      return res.sendFile(p.ruta_archivo);
    }
    res.status(404).json({error:'Archivo no disponible (no está en BD ni en disco)'});
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.delete('/api/pdfs/:id', auth, adminOnly, async (req,res)=>{
  try {
    const rows = await QR(req,'SELECT * FROM pdfs_guardados WHERE id=$1',[req.params.id]);
    if(rows.length && fs.existsSync(rows[0].ruta_archivo)) {
      try { fs.unlinkSync(rows[0].ruta_archivo); } catch{}
    }
    await QR(req,'DELETE FROM pdfs_guardados WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ── Lista de empresas (para selector en formulario de usuarios) ──
app.get('/api/empresas-lista', auth, async (req,res)=>{
  try{
    // Admin del sistema ve todas; admin de empresa solo ve la suya
    let rows;
    if(req.user.rol==='admin'){
      rows = await pool.query('SELECT id,nombre,slug,activa FROM public.empresas WHERE activa=true ORDER BY nombre');
    } else {
      rows = await pool.query('SELECT id,nombre,slug FROM public.empresas WHERE id=$1',[req.user.empresa_id]);
    }
    res.json(rows.rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// REPORTES DE SERVICIO
// ================================================================
app.get('/api/reportes-servicio', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      ORDER BY rs.created_at DESC`);
    res.json(rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/reportes-servicio/:id', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre, cl.rfc cliente_rfc,
        cl.email cliente_email, cl.telefono cliente_tel,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      WHERE rs.id=$1`,[req.params.id]);
    if(!rows.length) return res.status(404).json({error:'No encontrado'});
    res.json(rows[0]);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/reportes-servicio', auth, async (req,res)=>{
  try{
    const {titulo,cliente_id,proyecto_id,fecha_reporte,fecha_servicio,tecnico,
           introduccion,objetivo,alcance,descripcion_sistema,arquitectura,
           desarrollo_tecnico,resultados_pruebas,problemas_detectados,
           soluciones_implementadas,conclusiones,recomendaciones,anexos} = req.body;
    if(!titulo) return res.status(400).json({error:'Título requerido'});
    const yr = new Date().getFullYear();
    const cnt = await QR(req,'SELECT COUNT(*) val FROM reportes_servicio');
    const num = `RS-${yr}-${String(parseInt(cnt[0]?.val||0)+1).padStart(3,'0')}`;
    const rows = await QR(req,`
      INSERT INTO reportes_servicio (numero_reporte,titulo,cliente_id,proyecto_id,
        fecha_reporte,fecha_servicio,tecnico,estatus,
        introduccion,objetivo,alcance,descripcion_sistema,arquitectura,
        desarrollo_tecnico,resultados_pruebas,problemas_detectados,
        soluciones_implementadas,conclusiones,recomendaciones,anexos,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'borrador',$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
      RETURNING *`,
      [num,titulo,cliente_id||null,proyecto_id||null,
       fecha_reporte||new Date().toISOString().slice(0,10),
       fecha_servicio||null, tecnico||req.user.nombre||'VEF',
       introduccion||null,objetivo||null,alcance||null,descripcion_sistema||null,
       arquitectura||null,desarrollo_tecnico||null,resultados_pruebas||null,
       problemas_detectados||null,soluciones_implementadas||null,
       conclusiones||null,recomendaciones||null,anexos||null,req.user.id]);
    res.status(201).json(rows[0]);
  }catch(e){ console.error('RS POST:',e.message); res.status(500).json({error:e.message}); }
});

app.put('/api/reportes-servicio/:id', auth, async (req,res)=>{
  try{
    const b = req.body;
    const sets=[]; const vals=[]; let i=1;
    const add=(k,v)=>{ sets.push(`${k}=$${i++}`); vals.push(v); };
    if(b.titulo!==undefined)                  add('titulo',b.titulo);
    if(b.cliente_id!==undefined)              add('cliente_id',b.cliente_id||null);
    if(b.proyecto_id!==undefined)             add('proyecto_id',b.proyecto_id||null);
    if(b.fecha_reporte!==undefined)           add('fecha_reporte',b.fecha_reporte);
    if(b.fecha_servicio!==undefined)          add('fecha_servicio',b.fecha_servicio||null);
    if(b.tecnico!==undefined)                 add('tecnico',b.tecnico);
    if(b.estatus!==undefined)                 add('estatus',b.estatus);
    if(b.introduccion!==undefined)            add('introduccion',b.introduccion);
    if(b.objetivo!==undefined)                add('objetivo',b.objetivo);
    if(b.alcance!==undefined)                 add('alcance',b.alcance);
    if(b.descripcion_sistema!==undefined)     add('descripcion_sistema',b.descripcion_sistema);
    if(b.arquitectura!==undefined)            add('arquitectura',b.arquitectura);
    if(b.desarrollo_tecnico!==undefined)      add('desarrollo_tecnico',b.desarrollo_tecnico);
    if(b.resultados_pruebas!==undefined)      add('resultados_pruebas',b.resultados_pruebas);
    if(b.problemas_detectados!==undefined)    add('problemas_detectados',b.problemas_detectados);
    if(b.soluciones_implementadas!==undefined)add('soluciones_implementadas',b.soluciones_implementadas);
    if(b.conclusiones!==undefined)            add('conclusiones',b.conclusiones);
    if(b.recomendaciones!==undefined)         add('recomendaciones',b.recomendaciones);
    if(b.anexos!==undefined)                  add('anexos',b.anexos);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req,`UPDATE reportes_servicio SET ${sets.join(',')} WHERE id=$${i}`,vals);
    const rows = await QR(req,'SELECT * FROM reportes_servicio WHERE id=$1',[req.params.id]);
    res.json(rows[0]||{});
  }catch(e){ console.error('RS PUT:',e.message); res.status(500).json({error:e.message}); }
});

app.delete('/api/reportes-servicio/:id', auth, adminOnly, async (req,res)=>{
  try{ await QR(req,'DELETE FROM reportes_servicio WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){ res.status(500).json({error:e.message}); }
});

// ── PDF del Reporte de Servicio ───────────────────────────────────
app.get('/api/reportes-servicio/:id/pdf', auth, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT rs.*,
        cl.nombre cliente_nombre,cl.rfc cliente_rfc,cl.email cliente_email,
        cl.telefono cliente_tel,cl.direccion cliente_dir,
        cl.contacto cliente_contacto,cl.ciudad cliente_ciudad,
        p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      WHERE rs.id=$1`,[req.params.id]);
    if(!rows.length) return res.status(404).json({error:'No encontrado'});
    const r = rows[0];
    const emp = await getEmpConfig(req.user?.schema);
    const buf = await buildPDFReporteServicio(r, emp);
    savePDFToFile(buf,'reporte_servicio',r.id,r.numero_reporte,r.cliente_nombre,req.user?.id,req.user?.schema).catch(()=>{});
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="RS-${r.numero_reporte||r.id}.pdf"`);
    res.send(buf);
  }catch(e){ console.error('RS PDF:',e.message); res.status(500).json({error:e.message}); }
});

// ================================================================
// LOGO PÚBLICO — sin auth, para mostrarlo en el HTML
// ================================================================
app.get('/logo.png', (req,res)=>{
  const lp = getLogoPath();
  if (!lp) return res.status(404).send('No logo');
  res.sendFile(lp);
});
app.get('/logo.jpg', (req,res)=>{
  const lp = getLogoPath();
  if (!lp) return res.status(404).send('No logo');
  res.sendFile(lp);
});

// ================================================================
// FRONTEND
// ================================================================
// Página de administración exclusiva
app.get('/admin', (req,res)=>{
  res.setHeader('Cache-Control','no-cache,no-store,must-revalidate');
  res.sendFile(path.join(__dirname,'frontend','admin.html'));
});

app.get('/app', (req,res)=>{
  res.setHeader('Cache-Control','no-cache, no-store, must-revalidate');
  res.setHeader('Pragma','no-cache');
  res.setHeader('Expires','0');
  res.sendFile(path.join(__dirname,'frontend','app.html'));
});
// Catch-all: solo para rutas que NO son /api — devuelve app o index según ruta
app.get('*', (req,res)=>{
  if(req.path.startsWith('/api/'))
    return res.status(404).json({error:'Endpoint no encontrado: '+req.path});
  if(req.path==='/app'||req.path.startsWith('/app/'))
    return res.sendFile(path.join(__dirname,'frontend','app.html'));
  if(req.path==='/admin')
    return res.sendFile(path.join(__dirname,'frontend','admin.html'));
  res.sendFile(path.join(__dirname,'frontend','index.html'));
});

// ================================================================
// START
// ================================================================
// ================================================================
// SAT DESCARGA MASIVA — Microservicio Python puerto 5050
// ================================================================
async function satProxy(endpoint, body) {
  const http = require('http');
  const data = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: 'localhost', port: 5050,
      path: endpoint, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) },
      timeout: 90000
    }, (r) => {
      let buf = '';
      r.on('data', c => buf += c);
      r.on('end', () => {
        try { resolve(JSON.parse(buf)); }
        catch(e) { resolve({ ok: false, error: 'Respuesta inválida del servicio SAT' }); }
      });
    });
    req.on('error', e => reject(e));
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout — el servicio SAT no respondió')); });
    req.write(data); req.end();
  });
}

app.post('/api/sat/login', auth, async (req, res) => {
  try {
    const result = await satProxy('/login', req.body);
    res.json(result);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/api/sat/solicitar', auth, async (req, res) => {
  try {
    const result = await satProxy('/solicitar', req.body);
    if (result.ok && result.solicitud?.IdSolicitud) {
      await QR(req, `INSERT INTO sat_solicitudes
        (id_solicitud, fecha_inicio, fecha_fin, tipo, estatus, created_by)
        VALUES ($1,$2,$3,$4,'pendiente',$5)
        ON CONFLICT (id_solicitud) DO NOTHING`,
        [result.solicitud.IdSolicitud, req.body.fecha_inicio,
         req.body.fecha_fin, req.body.tipo||'CFDI', req.user.id]).catch(()=>{});
    }
    res.json(result);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/api/sat/verificar', auth, async (req, res) => {
  try {
    const result = await satProxy('/verificar', req.body);
    if (result.ok && result.listo) {
      await QR(req, `UPDATE sat_solicitudes SET estatus='listo', paquetes=$1 WHERE id_solicitud=$2`,
        [JSON.stringify(result.paquetes), req.body.id_solicitud]).catch(()=>{});
    }
    res.json(result);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/api/sat/descargar', auth, async (req, res) => {
  try {
    const result = await satProxy('/descargar', req.body);
    if (result.ok && result.cfdis) {
      for (const cfdi of result.cfdis) {
        if (!cfdi.uuid) continue;
        await QR(req, `INSERT INTO sat_cfdis
          (uuid,fecha_cfdi,tipo_comprobante,subtotal,total,moneda,
           emisor_rfc,emisor_nombre,receptor_rfc,receptor_nombre,uso_cfdi,xml_content,id_paquete)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
          ON CONFLICT (uuid) DO UPDATE SET updated_at=NOW()`,
          [cfdi.uuid, cfdi.fecha||null, cfdi.tipo_comprobante||null,
           parseFloat(cfdi.subtotal)||0, parseFloat(cfdi.total)||0,
           cfdi.moneda||'MXN', cfdi.emisor_rfc, cfdi.emisor_nombre,
           cfdi.receptor_rfc, cfdi.receptor_nombre, cfdi.uso_cfdi,
           cfdi.xml, req.body.id_paquete]).catch(()=>{});
      }
      await QR(req, `UPDATE sat_solicitudes SET estatus='descargado' WHERE id_solicitud=$1`,
        [req.body.id_solicitud]).catch(()=>{});
    }
    const resp = { ...result };
    if (resp.cfdis) resp.cfdis = resp.cfdis.map(c => ({ ...c, xml: undefined }));
    res.json(resp);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/api/sat/cfdis', auth, async (req, res) => {
  try {
    const { tipo, rfc, desde, hasta } = req.query;
    let where = 'WHERE 1=1'; const vals = []; let i = 1;
    if (tipo)  { where += ` AND tipo_comprobante=$${i++}`; vals.push(tipo); }
    if (rfc)   { where += ` AND (emisor_rfc=$${i++} OR receptor_rfc=$${i++})`; vals.push(rfc,rfc); }
    if (desde) { where += ` AND fecha_cfdi>=$${i++}`; vals.push(desde); }
    if (hasta) { where += ` AND fecha_cfdi<=$${i++}`; vals.push(hasta); }
    const rows = await QR(req, `SELECT id,uuid,fecha_cfdi,tipo_comprobante,subtotal,total,
      moneda,emisor_rfc,emisor_nombre,receptor_rfc,receptor_nombre,uso_cfdi,id_paquete,created_at
      FROM sat_cfdis ${where} ORDER BY fecha_cfdi DESC LIMIT 500`, vals);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sat/cfdis/:uuid/xml', auth, async (req, res) => {
  try {
    const rows = await QR(req, 'SELECT xml_content,uuid FROM sat_cfdis WHERE uuid=$1', [req.params.uuid]);
    if (!rows.length||!rows[0].xml_content) return res.status(404).json({ error: 'No encontrado' });
    res.setHeader('Content-Type','application/xml');
    res.setHeader('Content-Disposition',`attachment; filename="${rows[0].uuid}.xml"`);
    res.send(rows[0].xml_content);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sat/solicitudes', auth, async (req, res) => {
  try {
    res.json(await QR(req, 'SELECT * FROM sat_solicitudes ORDER BY created_at DESC LIMIT 50'));
  } catch(e) { res.status(500).json({ error: e.message }); }
});


app.listen(PORT, async ()=>{
  console.log(`\n${'═'.repeat(50)}`);
  console.log(`  VEF ERP — Puerto ${PORT}`);
  console.log(`  DB: ${process.env.DB_HOST}`);
  console.log('═'.repeat(50)+'\n');
  await autoSetup();
  console.log(`\n🚀 http://localhost:${PORT}\n`);
});
module.exports=app;
