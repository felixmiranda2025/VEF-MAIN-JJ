// ================================================================
// VEF AUTOMATIZACIÓN — ERP Industrial v2.0
// server.js — Multi-empresa | Control de licencias | Admin completo
// ================================================================
// CAMBIOS v2.0:
//  - Corrección bugs críticos pool.query (.rows)
//  - Middleware `licencia` — bloquea acceso sin trial/licencia activa
//  - DELETE /api/admin/empresas/:id — borrar empresa + schema completo
//  - Rutas /api/usuarios solo accesibles para admin
//  - GET /api/licencia — estado de licencia para el frontend
//  - GET /presentacion — sirve pag.html corporativa
//  - Eliminados bloques duplicados (reportes-servicio, empresas-lista)
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

// ── AWS S3 ───────────────────────────────────────────────────────
let s3Client = null;
try {
  const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
  const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

  if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY && process.env.S3_BUCKET) {
    s3Client = new S3Client({
      region: process.env.AWS_REGION || 'us-east-2',
      credentials: {
        accessKeyId:     process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      }
    });
    console.log('☁️  S3 configurado — bucket:', process.env.S3_BUCKET);
  }

  // Subir buffer a S3, retorna URL firmada temporal
  global.s3Upload = async function(buffer, key, contentType = 'application/pdf') {
    if (!s3Client) return null;
    await s3Client.send(new PutObjectCommand({
      Bucket: process.env.S3_BUCKET, Key: key, Body: buffer, ContentType: contentType
    }));
    return key;
  };

  // Obtener URL temporal de descarga (15 min)
  global.s3SignedUrl = async function(key, expires = 900) {
    if (!s3Client || !key) return null;
    return getSignedUrl(s3Client, new GetObjectCommand({
      Bucket: process.env.S3_BUCKET, Key: key
    }), { expiresIn: expires });
  };

  // Eliminar archivo de S3
  global.s3Delete = async function(key) {
    if (!s3Client || !key) return;
    await s3Client.send(new DeleteObjectCommand({ Bucket: process.env.S3_BUCKET, Key: key })).catch(()=>{});
  };

} catch(e) {
  console.warn('S3 no disponible:', e.message);
  global.s3Upload   = async () => null;
  global.s3SignedUrl = async () => null;
  global.s3Delete   = async () => {};
}

// ── DEEPSEEK IA ──────────────────────────────────────────────────
// DeepSeek usa API compatible con OpenAI — no requiere SDK especial
// La API key se puede configurar por empresa en empresa_config.deepseek_api_key
// o globalmente en .env como DEEPSEEK_API_KEY
const _deepseekBaseUrl = 'https://api.deepseek.com/v1';
function getDeepSeekKey(schema) {
  // Primero buscar en empresa_config, luego en .env
  return Q('SELECT deepseek_api_key FROM empresa_config LIMIT 1', [], schema)
    .then(rows => rows[0]?.deepseek_api_key || process.env.DEEPSEEK_API_KEY || null)
    .catch(() => process.env.DEEPSEEK_API_KEY || null);
}
async function deepseekChat(messages, schema, model='deepseek-chat', max_tokens=1500) {
  const apiKey = await getDeepSeekKey(schema);
  if (!apiKey) throw new Error('DeepSeek API Key no configurada. Ve a Configuración → IA para agregarla.');
  const res = await fetch(`${_deepseekBaseUrl}/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
    body: JSON.stringify({ model, messages, max_tokens, temperature: 0.7 })
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error?.message || `DeepSeek error ${res.status}`);
  return data.choices[0]?.message?.content || '';
}
console.log('🤖 DeepSeek IA lista (API key por empresa o .env DEEPSEEK_API_KEY)');

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
      `CREATE TABLE IF NOT EXISTS clientes (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,contacto TEXT,direccion TEXT,telefono TEXT,email TEXT,rfc TEXT,regimen_fiscal TEXT,cp TEXT,ciudad TEXT,tipo_persona VARCHAR(10) DEFAULT 'moral',activo BOOLEAN DEFAULT true,constancia_pdf BYTEA,constancia_nombre TEXT,constancia_fecha TIMESTAMP,estado_cuenta_pdf BYTEA,estado_cuenta_nombre TEXT,estado_cuenta_fecha TIMESTAMP,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS proveedores (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,contacto TEXT,direccion TEXT,telefono TEXT,email TEXT,rfc TEXT,condiciones_pago TEXT,tipo_persona VARCHAR(10) DEFAULT 'moral',activo BOOLEAN DEFAULT true,constancia_pdf BYTEA,constancia_nombre TEXT,constancia_fecha TIMESTAMP,estado_cuenta_pdf BYTEA,estado_cuenta_nombre TEXT,estado_cuenta_fecha TIMESTAMP,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS proyectos (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,cliente_id INTEGER,responsable TEXT,fecha_creacion DATE DEFAULT CURRENT_DATE,estatus TEXT DEFAULT 'activo',created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS cotizaciones (id SERIAL PRIMARY KEY,proyecto_id INTEGER,numero_cotizacion TEXT,fecha_emision DATE DEFAULT CURRENT_DATE,validez_hasta DATE,alcance_tecnico TEXT,notas_importantes TEXT,comentarios_generales TEXT,condiciones_entrega TEXT,condiciones_pago TEXT,garantia TEXT,total NUMERIC(15,2) DEFAULT 0,moneda TEXT DEFAULT 'USD',estatus TEXT DEFAULT 'borrador',created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS items_cotizacion (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,descripcion TEXT,cantidad NUMERIC(10,2),precio_unitario NUMERIC(15,2),total NUMERIC(15,2))`,
      `CREATE TABLE IF NOT EXISTS seguimientos (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,notas TEXT,proxima_accion TEXT)`,
      `CREATE TABLE IF NOT EXISTS ordenes_proveedor (id SERIAL PRIMARY KEY,proveedor_id INTEGER,numero_op TEXT,fecha_emision DATE DEFAULT CURRENT_DATE,fecha_entrega DATE,condiciones_pago TEXT,lugar_entrega TEXT,notas TEXT,total NUMERIC(15,2) DEFAULT 0,moneda TEXT DEFAULT 'USD',estatus TEXT DEFAULT 'borrador',factura_pdf BYTEA,factura_nombre TEXT,cotizacion_pdf BYTEA,cotizacion_nombre TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`,
        `ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS proveedor_nombre TEXT`,
        `CREATE TABLE IF NOT EXISTS items_orden_proveedor (id SERIAL PRIMARY KEY,orden_id INTEGER,descripcion TEXT,cantidad NUMERIC(10,2),precio_unitario NUMERIC(15,2),total NUMERIC(15,2))`,
      `CREATE TABLE IF NOT EXISTS seguimientos_oc (id SERIAL PRIMARY KEY,orden_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,notas TEXT,proxima_accion TEXT)`,
      `CREATE TABLE IF NOT EXISTS facturas (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,numero_factura TEXT,cliente_id INTEGER,moneda TEXT DEFAULT 'USD',subtotal NUMERIC(15,2) DEFAULT 0,iva NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,fecha_emision DATE DEFAULT CURRENT_DATE,fecha_vencimiento DATE,estatus TEXT DEFAULT 'pendiente',notas TEXT,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS pagos (id SERIAL PRIMARY KEY,factura_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),monto NUMERIC(15,2),metodo TEXT,referencia TEXT,notas TEXT,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS inventario (id SERIAL PRIMARY KEY,codigo TEXT,nombre TEXT NOT NULL,descripcion TEXT,categoria TEXT,unidad TEXT DEFAULT 'pza',cantidad_actual NUMERIC(10,2) DEFAULT 0,cantidad_minima NUMERIC(10,2) DEFAULT 0,precio_costo NUMERIC(15,2) DEFAULT 0,precio_venta NUMERIC(15,2) DEFAULT 0,ubicacion TEXT,proveedor_id INTEGER,foto TEXT,notas TEXT,activo BOOLEAN DEFAULT true,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS movimientos_inventario (id SERIAL PRIMARY KEY,producto_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,cantidad NUMERIC(10,2),stock_anterior NUMERIC(10,2) DEFAULT 0,stock_nuevo NUMERIC(10,2) DEFAULT 0,referencia TEXT,notas TEXT,created_by INTEGER)`,
      `CREATE TABLE IF NOT EXISTS tareas (id SERIAL PRIMARY KEY,titulo VARCHAR(300) NOT NULL,descripcion TEXT,proyecto_id INTEGER,asignado_a INTEGER,creado_por INTEGER,prioridad VARCHAR(20) DEFAULT 'normal',estatus VARCHAR(30) DEFAULT 'pendiente',fecha_inicio DATE,fecha_vencimiento DATE,notas TEXT,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS egresos (id SERIAL PRIMARY KEY,fecha DATE NOT NULL DEFAULT CURRENT_DATE,proveedor_id INTEGER,proveedor_nombre VARCHAR(200),categoria VARCHAR(100),descripcion TEXT,subtotal NUMERIC(15,2) DEFAULT 0,iva NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,metodo VARCHAR(50) DEFAULT 'Transferencia',referencia VARCHAR(100),numero_factura VARCHAR(100),factura_pdf BYTEA,factura_nombre TEXT,notas TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS pdfs_guardados (id SERIAL PRIMARY KEY,tipo VARCHAR(30) NOT NULL,referencia_id INTEGER NOT NULL,numero_doc VARCHAR(100),cliente_proveedor VARCHAR(200),nombre_archivo VARCHAR(200),tamanio_bytes INTEGER,pdf_data BYTEA,generado_por INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS crm_oportunidades (id SERIAL PRIMARY KEY,cliente_id INTEGER NOT NULL,nombre TEXT NOT NULL,etapa VARCHAR(30) DEFAULT 'prospecto',valor NUMERIC(15,2) DEFAULT 0,moneda VARCHAR(5) DEFAULT 'MXN',probabilidad INTEGER DEFAULT 20,fecha_cierre_est DATE,responsable TEXT,descripcion TEXT,origen VARCHAR(50),perdida_motivo TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS crm_actividades (id SERIAL PRIMARY KEY,cliente_id INTEGER,oportunidad_id INTEGER,tipo VARCHAR(30) DEFAULT 'nota',titulo TEXT,descripcion TEXT,fecha TIMESTAMP DEFAULT NOW(),proxima_accion TEXT,proxima_fecha DATE,completada BOOLEAN DEFAULT false,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS reportes_servicio (id SERIAL PRIMARY KEY,numero_reporte VARCHAR(50),titulo VARCHAR(300) NOT NULL,cliente_id INTEGER,proyecto_id INTEGER,fecha_reporte DATE DEFAULT CURRENT_DATE,fecha_servicio DATE,tecnico VARCHAR(200),estatus VARCHAR(30) DEFAULT 'borrador',introduccion TEXT,objetivo TEXT,alcance TEXT,descripcion_sistema TEXT,arquitectura TEXT,desarrollo_tecnico TEXT,resultados_pruebas TEXT,problemas_detectados TEXT,soluciones_implementadas TEXT,conclusiones TEXT,recomendaciones TEXT,anexos TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS sat_solicitudes (id SERIAL PRIMARY KEY,id_solicitud VARCHAR(100) UNIQUE,fecha_inicio DATE,fecha_fin DATE,tipo VARCHAR(20) DEFAULT 'CFDI',estatus VARCHAR(30) DEFAULT 'pendiente',paquetes TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS sat_cfdis (id SERIAL PRIMARY KEY,uuid VARCHAR(100) UNIQUE,fecha_cfdi TIMESTAMP,tipo_comprobante VARCHAR(5),subtotal NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,moneda VARCHAR(10) DEFAULT 'MXN',emisor_rfc VARCHAR(20),emisor_nombre VARCHAR(300),receptor_rfc VARCHAR(20),receptor_nombre VARCHAR(300),uso_cfdi VARCHAR(10),forma_pago VARCHAR(5),metodo_pago VARCHAR(5),lugar_expedicion VARCHAR(10),serie VARCHAR(50),folio VARCHAR(50),no_certificado VARCHAR(30),version VARCHAR(5),fecha_timbrado TIMESTAMP,rfc_prov_certif VARCHAR(20),xml_content TEXT,id_paquete VARCHAR(200),estatus_sat VARCHAR(20),monto_sat NUMERIC(15,2),rfc_pac VARCHAR(20),fecha_cancelacion TIMESTAMP,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS solicitudes_autorizacion (id SERIAL PRIMARY KEY,tipo VARCHAR(30) DEFAULT 'envio_cotizacion',referencia_id INTEGER,referencia_num TEXT,solicitante_id INTEGER,solicitante_nombre TEXT,destinatario_email TEXT,destinatario_cc TEXT,asunto TEXT,mensaje TEXT,estatus VARCHAR(20) DEFAULT 'pendiente',autorizado_por INTEGER,fecha_solicitud TIMESTAMP DEFAULT NOW(),fecha_resolucion TIMESTAMP,notas_autorizador TEXT)`,
      `CREATE TABLE IF NOT EXISTS evaluaciones_proveedores (id SERIAL PRIMARY KEY,proveedor_id INTEGER NOT NULL,periodo VARCHAR(100),referencia VARCHAR(100),calidad NUMERIC(3,1),precio NUMERIC(3,1),entrega NUMERIC(3,1),servicio NUMERIC(3,1),documentacion NUMERIC(3,1),garantia NUMERIC(3,1),calificacion_total NUMERIC(4,2),recomendacion VARCHAR(30),notas TEXT,created_by INTEGER,created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS empresa_config (id SERIAL PRIMARY KEY,nombre VARCHAR(200) NOT NULL DEFAULT 'Mi Empresa',razon_social VARCHAR(200),rfc VARCHAR(30),regimen_fiscal VARCHAR(100),contacto VARCHAR(100),telefono VARCHAR(50),email VARCHAR(100),direccion TEXT,ciudad VARCHAR(100),estado VARCHAR(100),cp VARCHAR(10),pais VARCHAR(50) DEFAULT 'México',moneda_default VARCHAR(10) DEFAULT 'USD',iva_default NUMERIC(5,2) DEFAULT 16.00,margen_ganancia NUMERIC(5,2) DEFAULT 0,smtp_host VARCHAR(100),smtp_port INTEGER DEFAULT 465,smtp_user VARCHAR(100),smtp_pass VARCHAR(200),notas_factura TEXT,notas_cotizacion TEXT,logo_data BYTEA,logo_mime VARCHAR(30) DEFAULT 'image/png',sat_fiel_rfc VARCHAR(20),sat_fiel_configurado BOOLEAN DEFAULT false,deepseek_api_key TEXT,updated_at TIMESTAMP DEFAULT NOW())`,
    ];
    for (const sql of tablas) await client.query(sql);
    await client.query(`INSERT INTO empresa_config (nombre) VALUES ($1)`,[nombreEmpresa||'Mi Empresa']);
    console.log('✅ Schema creado:', schema);
    return schema;
  } finally { client.release(); }
}
// Exponer globalmente para stripe_routes y otros módulos externos
global.crearSchemaEmpresa = crearSchemaEmpresa;

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
    } catch(e){
      console.error('DB ERROR ['+s+']:', e.message, '\n  SQL:', sql.slice(0,200));
      throw e;
    }
    finally { client.release(); }
  }
  try { return (await pool.query(sql, p)).rows; }
  catch(e) {
    console.error('DB ERROR [public]:', e.message, '\n  SQL:', sql.slice(0,200));
    throw e;
  }
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
app.set('trust proxy', 1); // Railway usa proxy inverso — necesario para rate-limit y cookies
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors({ origin: '*', credentials: true }));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(express.static(path.join(__dirname, 'frontend')));
// Rate limiting global
app.use('/api', rateLimit({ windowMs:15*60*1000, max:500, standardHeaders:true, legacyHeaders:false,
  message:{ error:'Demasiadas peticiones. Espera un momento.' } }));
// Rate limiting estricto para auth (evita fuerza bruta)
const authLimiter = rateLimit({ windowMs:15*60*1000, max:20, standardHeaders:true, legacyHeaders:false,
  message:{ error:'Demasiados intentos de login. Espera 15 minutos.' } });
app.use('/api/auth', authLimiter);
app.use('/api/usuarios', rateLimit({ windowMs:15*60*1000, max:100 }));

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

// ── Middleware de licencia ─────────────────────────────────────────
// Bloquea acceso a la API si el trial venció o la licencia no está activa.
// El admin siempre puede entrar para renovar o configurar.
async function licencia(req, res, next) {
  if (req.user?.rol === 'admin') return next();
  const empId = req.user?.empresa_id;
  if (!empId) return next();
  try {
    const { rows } = await pool.query(
      'SELECT trial_hasta,suscripcion_estatus,suscripcion_hasta,activa FROM public.empresas WHERE id=$1',
      [empId]);
    if (!rows.length) return next();
    const e = rows[0];
    const hoy = new Date();
    if (e.activa === false)
      return res.status(402).json({ error:'Cuenta inactiva. Contacta al administrador.', codigo:'CUENTA_INACTIVA' });
    if (e.suscripcion_estatus === 'activa' && e.suscripcion_hasta && new Date(e.suscripcion_hasta) >= hoy)
      return next();
    if (e.suscripcion_estatus === 'trial' && e.trial_hasta && new Date(e.trial_hasta) >= hoy)
      return next();
    return res.status(402).json({
      error: e.suscripcion_estatus === 'trial'
        ? 'Tu período de prueba ha vencido. Contacta al administrador para activar tu licencia.'
        : 'Licencia vencida. Contacta al administrador.',
      codigo: 'LICENCIA_VENCIDA',
      requiere_pago: true
    });
  } catch { return next(); }
}

// ── Middleware: bloquea escritura si empresa inactiva ────────────
// Permite GET (lectura) pero bloquea POST/PUT/DELETE/PATCH
async function empresaActiva(req, res, next) {
  // Admin siempre puede
  if (req.user?.rol === 'admin') return next();
  // Solo aplica a métodos de escritura
  if (req.method === 'GET') return next();
  const empId = req.user?.empresa_id;
  if (!empId) return next();
  try {
    const { rows } = await pool.query(
      'SELECT activa FROM public.empresas WHERE id=$1', [empId]);
    if (!rows.length) return next();
    if (rows[0].activa === false)
      return res.status(403).json({
        error: 'Tu empresa está inactiva. Solo puedes consultar información. Contacta al administrador.',
        codigo: 'EMPRESA_INACTIVA'
      });
    return next();
  } catch { return next(); }
}

// ── Middleware: solo lectura inventario para rol soporte ────────
function soloLecturaInventario(req,res,next){
  if(req.method==='GET') return next();
  if(req.user?.rol==='soporte') return res.status(403).json({error:'El rol Soporte solo puede consultar el inventario.',codigo:'SIN_PERMISO_ESCRITURA'});
  return next();
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
  // Logo: preferir logo_data de empresa_config (por empresa), fallback a archivo en disco
  const _logoBuf = emp._logo_data || null;  // Buffer desde BD
  const _lp = !_logoBuf ? getLogoPath() : null;
  const hasLogo = !!(_logoBuf || _lp);
  const LW = 130;

  doc.rect(M, 14, W, H).fill(C.AZUL);

  if (hasLogo) {
    doc.rect(M, 14, LW, H).fill(C.BLANCO);
    try {
      if (_logoBuf) {
        doc.image(_logoBuf, M+6, 18, { fit:[LW-12, H-8], align:'center', valign:'center' });
      } else {
        doc.image(_lp, M+6, 18, { fit:[LW-12, H-8], align:'center', valign:'center' });
      }
    } catch(e){}
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

function pdfWatermark(doc, emp={}) {
  const _logoBuf = emp?._logo_data || null;
  const _lp = !_logoBuf ? getLogoPath() : null;
  if (!_logoBuf && !_lp) return;
  try {
    doc.save(); doc.opacity(0.07);
    if (_logoBuf) doc.image(_logoBuf, 158, 270, { fit:[280,280] });
    else doc.image(_lp, 158, 270, { fit:[280,280] });
    doc.restore();
  } catch(e){}
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
    const sch = schema || global._defaultSchema;
    const rows = await Q('SELECT * FROM empresa_config ORDER BY id LIMIT 1', [], sch);
    const cfg = rows[0] || {};
    // logo_data ya viene como Buffer de PostgreSQL (BYTEA)
    // Si no tiene logo en BD, intentar desde archivo de disco
    if (!cfg._logo_data && !cfg.logo_data) {
      const logoFile = require('path').join(__dirname, 'logo_'+sch+'.png');
      const logoGeneral = getLogoPath();
      if (require('fs').existsSync(logoFile)) {
        cfg._logo_data = require('fs').readFileSync(logoFile);
      } else if (logoGeneral) {
        cfg._logo_data = require('fs').readFileSync(logoGeneral);
      }
    } else if (cfg.logo_data && !cfg._logo_data) {
      cfg._logo_data = cfg.logo_data;  // alias para el PDF
    }
    return cfg;
  } catch(e) { return {}; }
}

async function buildPDFCotizacion(cot, items, schema=null) {
  const emp = await getEmpConfig(schema||cot._schema);
  return new Promise((res,rej)=>{
    const doc=new PDFKit({margin:28,size:'A4'});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>res(Buffer.concat(ch))); doc.on('error',rej);
    pdfWatermark(doc, emp);
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
    pdfWatermark(doc, emp);
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
  // Load full client data if we only have partial
  let cli = {};
  if (f.cliente_id) {
    try {
      const pool2 = pool;
      const sc = schema || global._defaultSchema || 'emp_vef';
      const cliRows = (await pool2.query(
        `SET search_path TO "${sc}",public; SELECT * FROM clientes WHERE id=$1`, [f.cliente_id]
      ).catch(()=>null));
      // Use simple query approach
      const c2 = await pool2.connect();
      try {
        await c2.query(`SET search_path TO "${sc}",public`);
        const r = await c2.query('SELECT * FROM clientes WHERE id=$1',[f.cliente_id]);
        if (r.rows.length) cli = r.rows[0];
      } finally { c2.release(); }
    } catch {}
  }
  // Merge cli data with f (f may already have some from JOIN)
  const clienteNombre    = f.cliente_nombre    || cli.nombre            || '—';
  const clienteRFC       = f.cliente_rfc       || cli.rfc               || '—';
  const clienteEmail     = f.cliente_email     || cli.email             || '—';
  const clienteTel       = f.cliente_tel       || cli.telefono          || '—';
  const clienteRegimen   = f.cliente_regimen   || cli.regimen_fiscal    || '—';
  const clienteCP        = f.cliente_cp        || cli.cp                || '—';
  const clienteUsoCFDI   = f.cliente_uso_cfdi  || cli.uso_cfdi         || '—';
  const clienteTipo      = f.cliente_tipo      || cli.tipo_persona      || 'moral';
  const clienteDireccion = f.cliente_direccion || cli.direccion         || '';
  const clienteCiudad    = f.cliente_ciudad    || cli.ciudad            || '';

  const emp = await getEmpConfig(schema||f._schema);
  const MON = (f.moneda||'MXN') === 'USD' ? 'USD' : 'MXN';
  const SYM = MON === 'USD' ? '$' : 'MX$';
  const mxn = (n) => `${SYM} ${parseFloat(n||0).toLocaleString('es-MX',{minimumFractionDigits:2,maximumFractionDigits:2})}`;

  return new Promise((res,rej)=>{
    const M=28, W=539, COL2=280;
    const doc=new PDFKit({margin:M,size:'A4'});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>res(Buffer.concat(ch))); doc.on('error',rej);

    pdfWatermark(doc, emp);

    // ── HEADER ──────────────────────────────────────────────────
    pdfHeader(doc, 'SOLICITUD DE FACTURA', [
      `No. ${f.numero_factura||'—'}  |  Fecha: ${fmt(f.fecha_emision)}  |  Estatus: ${(f.estatus||'pendiente').toUpperCase()}`,
    ], emp);

    // ── DATOS DEL CLIENTE / RECEPTOR CFDI ───────────────────────
    pdfSec(doc, 'Datos del Cliente / Receptor CFDI');
    const startY = doc.y;
    const rowH = 20;
    const drawRow = (label1, val1, label2, val2, y) => {
      doc.font('Helvetica-Bold').fontSize(9).fillColor(C.TEXTO)
         .text(label1, M+4, y+4, {width:80});
      doc.font('Helvetica').fontSize(9).fillColor(C.TEXTO)
         .text(val1, M+90, y+4, {width:COL2-90-10});
      if (label2) {
        doc.font('Helvetica-Bold').fontSize(9).fillColor(C.TEXTO)
           .text(label2, COL2+4, y+4, {width:80});
        doc.font('Helvetica').fontSize(9).fillColor(C.TEXTO)
           .text(val2||'—', COL2+90, y+4, {width:W-COL2-90});
      }
    };
    // Table rows
    const rows = [
      ['Nombre Cliente:', clienteNombre,       'RFC:',          clienteRFC],
      ['Régimen Fiscal:', clienteRegimen,      'Uso CFDI:',     clienteUsoCFDI],
      ['C.P. Fiscal:',   clienteCP,            'Tipo Persona:', clienteTipo.toUpperCase()],
      ['Correo Envío:',  clienteEmail,         'Teléfono:',     clienteTel],
    ];
    // Draw bordered table
    const tableW = W;
    rows.forEach((row, i) => {
      const y = startY + i * rowH;
      // Alternating rows
      if (i % 2 === 0) doc.rect(M, y, tableW, rowH).fill('#EBF3FB').stroke('#C8DEFF');
      else             doc.rect(M, y, tableW, rowH).fill('#FFFFFF').stroke('#C8DEFF');
      doc.rect(M, y, tableW, rowH).stroke('#C8DEFF');
      // Center divider
      doc.moveTo(COL2, y).lineTo(COL2, y+rowH).strokeColor('#C8DEFF').stroke();
      drawRow(row[0], row[1], row[2], row[3], y);
    });
    // Dirección fiscal row (full width)
    const dirY = startY + rows.length * rowH;
    doc.rect(M, dirY, tableW, rowH).fill('#EBF3FB').stroke('#C8DEFF');
    doc.font('Helvetica-Bold').fontSize(9).fillColor(C.TEXTO).text('Dirección Fiscal:', M+4, dirY+4, {width:90});
    const dirFull = [clienteDireccion, clienteCiudad].filter(Boolean).join(', ');
    doc.font('Helvetica').fontSize(9).fillColor(C.TEXTO).text(dirFull||'—', M+100, dirY+4, {width:tableW-100});
    doc.y = dirY + rowH + 10;
    doc.moveDown(0.5);

    // ── CONCEPTOS ────────────────────────────────────────────────
    pdfSec(doc, 'Conceptos');
    // Header row
    const hY = doc.y;
    const cols_w = [28, 95, 220, 50, 80, 66]; // #, CodSAT/Unidad, Desc, Cant, PrecioUnit, Importe
    let xPos = M;
    const headers = ['#','Código SAT / Unidad','Descripción del Concepto','Cant.','Precio Unit.','Importe'];
    doc.rect(M, hY, W, 18).fill(C.AZUL).stroke(C.AZUL);
    headers.forEach((h, i) => {
      const align = i >= 3 ? 'right' : (i===0 ? 'center' : 'left');
      doc.font('Helvetica-Bold').fontSize(8).fillColor('#FFFFFF')
         .text(h, xPos+2, hY+4, {width:cols_w[i]-4, align});
      xPos += cols_w[i];
    });
    doc.y = hY + 20;

    // Item rows
    (items.length ? items : [{
      descripcion: f.notas || 'Servicio', cantidad:1,
      precio_unitario: f.subtotal||f.total||0,
      total: f.subtotal||f.total||0,
      clave_prod_serv: '', clave_unidad: 'H87',
    }]).forEach((it, idx) => {
      const iy = doc.y;
      const bg = idx%2===0 ? '#F5F9FF' : '#FFFFFF';
      doc.rect(M, iy, W, 30).fill(bg).stroke('#C8DEFF');
      xPos = M;
      // #
      doc.font('Helvetica-Bold').fontSize(8).fillColor(C.TEXTO)
         .text(String(idx+1), xPos+2, iy+5, {width:cols_w[0]-4, align:'center'});
      xPos += cols_w[0];
      // Código SAT / Unidad
      const codSAT = it.clave_prod_serv || '';
      const unidad = it.clave_unidad || it.unidad || 'H87';
      doc.font('Helvetica').fontSize(8).fillColor(C.TEXTO)
         .text(codSAT, xPos+2, iy+5, {width:cols_w[1]-4});
      doc.font('Helvetica').fontSize(8).fillColor('#666666')
         .text('Unidad: '+unidad, xPos+2, iy+16, {width:cols_w[1]-4});
      xPos += cols_w[1];
      // Descripción
      doc.font('Helvetica').fontSize(9).fillColor(C.TEXTO)
         .text(it.descripcion||'—', xPos+2, iy+5, {width:cols_w[2]-4});
      xPos += cols_w[2];
      // Cantidad
      doc.font('Helvetica').fontSize(9).fillColor(C.TEXTO)
         .text(String(parseFloat(it.cantidad||1)), xPos+2, iy+5, {width:cols_w[3]-4, align:'right'});
      xPos += cols_w[3];
      // Precio unit
      doc.font('Helvetica').fontSize(9).fillColor(C.TEXTO)
         .text(mxn(it.precio_unitario||0).replace(/MX\$/,'$'), xPos+2, iy+5, {width:cols_w[4]-4, align:'right'});
      xPos += cols_w[4];
      // Importe
      doc.font('Helvetica-Bold').fontSize(9).fillColor(C.AZUL)
         .text(mxn(it.total||0).replace(/MX\$/,'$'), xPos+2, iy+5, {width:cols_w[5]-4, align:'right'});
      doc.y = iy + 32;
    });
    doc.moveDown(0.5);

    // ── TOTALES ──────────────────────────────────────────────────
    pdfSec(doc, 'Totales');
    const sub  = parseFloat(f.subtotal||f.monto||f.total||0);
    const iva  = parseFloat(f.iva||0);
    const risr = parseFloat(f.retencion_isr||0);
    const riva = parseFloat(f.retencion_iva||0);
    const tot  = parseFloat(f.total||f.monto||0);

    const totRows = [
      ['Subtotal antes de impuestos', mxn(sub),  '#FFFFFF', C.TEXTO, false],
      ['IVA 16%',                     mxn(iva),  '#FFFFFF', C.TEXTO, false],
    ];
    if (risr > 0) totRows.push(['ISR Retenido 1.25%', '-'+mxn(risr), '#FFFFFF', '#D97706', false]);
    if (riva > 0) totRows.push(['Retención IVA',       '-'+mxn(riva), '#FFFFFF', '#7C3AED', false]);
    totRows.push(['TOTAL', mxn(tot), C.AZUL, '#FFFFFF', true]);

    const tW = 250, tX = M + W - tW;
    totRows.forEach(([label, val, bg, txtColor, bold]) => {
      const ty = doc.y;
      doc.rect(tX, ty, tW, 18).fill(bg).stroke('#C8DEFF');
      doc.font(bold?'Helvetica-Bold':'Helvetica').fontSize(9).fillColor(txtColor)
         .text(label, tX+6, ty+4, {width:130});
      doc.font(bold?'Helvetica-Bold':'Helvetica').fontSize(9).fillColor(txtColor)
         .text(val, tX+6, ty+4, {width:tW-12, align:'right'});
      doc.y = ty + 20;
    });
    doc.moveDown(0.8);

    // ── DATOS DE PAGO ────────────────────────────────────────────
    pdfSec(doc, 'Datos de Pago');
    const notasRaw = f.notas||'';
    // Parse method/forma from notas field or from dedicated fields
    const metodoPago = f.metodo_pago || (notasRaw.includes('PPD') ? 'PPD — Pago en Parcialidades o Diferido' : notasRaw.includes('PUE') ? 'PUE — Pago en Una Sola Exhibición' : 'PPD — Pago en Parcialidades o Diferido');
    const formaPago  = f.forma_pago  || (notasRaw.includes('03') || notasRaw.includes('Transferencia') ? '03 — Transferencia electrónica' : '03 — Transferencia electrónica');

    const pagoData = [
      ['Método de Pago:', metodoPago, 'Forma de Pago:', formaPago],
      ['Moneda:',        MON,          'Vencimiento:',  fmt(f.fecha_vencimiento)],
    ];
    const pY0 = doc.y;
    pagoData.forEach((row, i) => {
      const y = pY0 + i * rowH;
      const bg2 = i % 2 === 0 ? '#F5F9FF' : '#FFFFFF';
      doc.rect(M, y, W, rowH).fill(bg2).stroke('#C8DEFF');
      doc.moveTo(COL2, y).lineTo(COL2, y+rowH).strokeColor('#C8DEFF').stroke();
      drawRow(row[0], row[1], row[2], row[3], y);
    });
    doc.y = pY0 + pagoData.length * rowH + 8;
    doc.moveDown(0.3);

    // Nota al pie sobre constancia
    doc.font('Helvetica').fontSize(8).fillColor('#888888')
       .text('Para la correcta emisión de factura es preferible entregar la constancia de situación fiscal de nuevos clientes.',
             M, doc.y, {width:W, align:'center'});

    pdfPie(doc,emp);
    doc.end();
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
        await client.query(`SET search_path TO "${schema}", public`);
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
        `CREATE TABLE IF NOT EXISTS empresa_config (id SERIAL PRIMARY KEY,nombre VARCHAR(200) DEFAULT 'VEF Automatización',razon_social VARCHAR(200),rfc VARCHAR(30),regimen_fiscal VARCHAR(100),contacto VARCHAR(100),telefono VARCHAR(50),email VARCHAR(100),direccion TEXT,ciudad VARCHAR(100),estado VARCHAR(100),cp VARCHAR(10),pais VARCHAR(50) DEFAULT 'México',sitio_web VARCHAR(150),moneda_default VARCHAR(10) DEFAULT 'USD',iva_default NUMERIC(5,2) DEFAULT 16,margen_ganancia NUMERIC(5,2) DEFAULT 0,smtp_host VARCHAR(100),smtp_port INTEGER DEFAULT 465,smtp_user VARCHAR(100),smtp_pass VARCHAR(200),notas_factura TEXT,notas_cotizacion TEXT,logo_data BYTEA,logo_mime VARCHAR(30) DEFAULT 'image/png',updated_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS clientes (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,contacto TEXT,direccion TEXT,telefono TEXT,email TEXT,rfc TEXT,regimen_fiscal TEXT,cp TEXT,ciudad TEXT,tipo_persona VARCHAR(10) DEFAULT 'moral',activo BOOLEAN DEFAULT true,constancia_pdf BYTEA,constancia_nombre TEXT,constancia_fecha TIMESTAMP,estado_cuenta_pdf BYTEA,estado_cuenta_nombre TEXT,estado_cuenta_fecha TIMESTAMP,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS proveedores (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,contacto TEXT,direccion TEXT,telefono TEXT,email TEXT,rfc TEXT,condiciones_pago TEXT,tipo_persona VARCHAR(10) DEFAULT 'moral',activo BOOLEAN DEFAULT true,constancia_pdf BYTEA,constancia_nombre TEXT,constancia_fecha TIMESTAMP,estado_cuenta_pdf BYTEA,estado_cuenta_nombre TEXT,estado_cuenta_fecha TIMESTAMP,created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS proyectos (id SERIAL PRIMARY KEY,nombre TEXT NOT NULL,cliente_id INTEGER,responsable TEXT,fecha_creacion DATE DEFAULT CURRENT_DATE,estatus TEXT DEFAULT 'activo',created_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS cotizaciones (id SERIAL PRIMARY KEY,proyecto_id INTEGER,numero_cotizacion TEXT UNIQUE,fecha_emision DATE DEFAULT CURRENT_DATE,validez_hasta DATE,alcance_tecnico TEXT,notas_importantes TEXT,comentarios_generales TEXT,servicio_postventa TEXT,condiciones_entrega TEXT,condiciones_pago TEXT,garantia TEXT,responsabilidad TEXT,validez TEXT,fuerza_mayor TEXT,ley_aplicable TEXT,total NUMERIC(15,2) DEFAULT 0,moneda TEXT DEFAULT 'USD',estatus TEXT DEFAULT 'borrador',created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
        `CREATE TABLE IF NOT EXISTS items_cotizacion (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,descripcion TEXT,cantidad NUMERIC(10,2),precio_unitario NUMERIC(15,2),total NUMERIC(15,2))`,
        `CREATE TABLE IF NOT EXISTS seguimientos (id SERIAL PRIMARY KEY,cotizacion_id INTEGER,fecha TIMESTAMP DEFAULT NOW(),tipo TEXT,notas TEXT,proxima_accion TEXT)`,
        `CREATE TABLE IF NOT EXISTS crm_oportunidades (
          id SERIAL PRIMARY KEY,
          cliente_id INTEGER NOT NULL,
          nombre TEXT NOT NULL,
          etapa VARCHAR(30) DEFAULT 'prospecto',
          valor NUMERIC(15,2) DEFAULT 0,
          moneda VARCHAR(5) DEFAULT 'MXN',
          probabilidad INTEGER DEFAULT 20,
          fecha_cierre_est DATE,
          responsable TEXT,
          descripcion TEXT,
          origen VARCHAR(50),
          perdida_motivo TEXT,
          created_by INTEGER,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        )`,
        `CREATE TABLE IF NOT EXISTS crm_actividades (
          id SERIAL PRIMARY KEY,
          cliente_id INTEGER,
          oportunidad_id INTEGER,
          tipo VARCHAR(30) DEFAULT 'nota',
          titulo TEXT,
          descripcion TEXT,
          fecha TIMESTAMP DEFAULT NOW(),
          proxima_accion TEXT,
          proxima_fecha DATE,
          completada BOOLEAN DEFAULT false,
          created_by INTEGER,
          created_at TIMESTAMP DEFAULT NOW()
        )`,
        `ALTER TABLE clientes ADD COLUMN IF NOT EXISTS industria VARCHAR(100)`,
        `ALTER TABLE clientes ADD COLUMN IF NOT EXISTS sitio_web VARCHAR(200)`,
        `ALTER TABLE clientes ADD COLUMN IF NOT EXISTS linkedin VARCHAR(200)`,
        `ALTER TABLE clientes ADD COLUMN IF NOT EXISTS notas_crm TEXT`,
        `ALTER TABLE clientes ADD COLUMN IF NOT EXISTS etiquetas TEXT`,
        `ALTER TABLE clientes ADD COLUMN IF NOT EXISTS ultima_actividad TIMESTAMP`,
        `ALTER TABLE clientes ADD COLUMN IF NOT EXISTS valor_total_historico NUMERIC(15,2) DEFAULT 0`,
        `ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`,
        `ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS proveedor_nombre TEXT`,
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
        // RFQ — Solicitudes de cotización
      `CREATE TABLE IF NOT EXISTS rfq (
        id SERIAL PRIMARY KEY, numero_rfq VARCHAR(50),
        descripcion TEXT NOT NULL, proyecto_nombre VARCHAR(200),
        prioridad VARCHAR(20) DEFAULT 'media', fecha_limite DATE,
        presupuesto_max NUMERIC(15,2), moneda VARCHAR(10) DEFAULT 'MXN',
        condiciones_pago TEXT, lugar_entrega TEXT, criterios_eval TEXT,
        notas TEXT, terminos TEXT, estatus VARCHAR(30) DEFAULT 'borrador',
        proveedor_ids JSONB DEFAULT '[]', items JSONB DEFAULT '[]',
        created_by INTEGER, created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW())`,
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
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS metodo_pago VARCHAR(5) DEFAULT 'PPD'",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS forma_pago VARCHAR(5) DEFAULT '03'",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS retencion_isr NUMERIC(15,2) DEFAULT 0",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS retencion_iva NUMERIC(15,2) DEFAULT 0",
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
          forma_pago VARCHAR(5),
          metodo_pago VARCHAR(5),
          lugar_expedicion VARCHAR(10),
          serie VARCHAR(50),
          folio VARCHAR(50),
          no_certificado VARCHAR(30),
          version VARCHAR(5),
          fecha_timbrado TIMESTAMP,
          rfc_prov_certif VARCHAR(20),
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
        // SAT timbrado — columnas necesarias para CFDI 4.0
        "ALTER TABLE items_cotizacion ADD COLUMN IF NOT EXISTS clave_prod_serv VARCHAR(20)",
        "ALTER TABLE items_cotizacion ADD COLUMN IF NOT EXISTS clave_unidad VARCHAR(10)",
        "ALTER TABLE items_cotizacion ADD COLUMN IF NOT EXISTS objeto_imp VARCHAR(5) DEFAULT '02'",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS uso_cfdi VARCHAR(10) DEFAULT 'G03'",
        "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS cp VARCHAR(10)",
        // RFQ table
        `CREATE TABLE IF NOT EXISTS rfq (id SERIAL PRIMARY KEY,numero_rfq VARCHAR(50),descripcion TEXT NOT NULL,proyecto_nombre VARCHAR(200),prioridad VARCHAR(20) DEFAULT 'media',fecha_limite DATE,presupuesto_max NUMERIC(15,2),moneda VARCHAR(10) DEFAULT 'MXN',condiciones_pago TEXT,lugar_entrega TEXT,criterios_eval TEXT,notas TEXT,terminos TEXT,estatus VARCHAR(30) DEFAULT 'borrador',proveedor_ids JSONB DEFAULT '[]',items JSONB DEFAULT '[]',created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
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
    // 4. Aplicar migraciones a TODOS los schemas de empresa
    //    Esto asegura que empresas nuevas también tengan
    //    todas las columnas actualizadas
    // ══════════════════════════════════════════════════════
    const allEmpresas = await pool.query(`SELECT slug FROM public.empresas WHERE activa=true`);
    const migraciones = [
      // Clientes
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
      // Proveedores
      "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS tipo_persona VARCHAR(10) DEFAULT 'moral'",
      "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS constancia_pdf BYTEA",
      "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS constancia_nombre TEXT",
      "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS constancia_fecha TIMESTAMP",
      "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS estado_cuenta_pdf BYTEA",
      "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS estado_cuenta_nombre TEXT",
      "ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS estado_cuenta_fecha TIMESTAMP",
      // Facturas
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS cliente_id INTEGER",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS subtotal NUMERIC(15,2) DEFAULT 0",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS iva NUMERIC(15,2) DEFAULT 0",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS retencion_isr NUMERIC(15,2) DEFAULT 0",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS retencion_iva NUMERIC(15,2) DEFAULT 0",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS estatus_pago VARCHAR(30)",
      "ALTER TABLE facturas ADD COLUMN IF NOT EXISTS moneda TEXT DEFAULT 'USD'",
      // Inventario
      "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS fecha_ultima_entrada DATE",
      "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS foto TEXT",
      "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS notas TEXT",
      "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS activo BOOLEAN DEFAULT true",
      "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS stock_actual NUMERIC(10,2) DEFAULT 0",
      "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS stock_minimo NUMERIC(10,2) DEFAULT 0",
      "ALTER TABLE inventario ADD COLUMN IF NOT EXISTS precio_venta NUMERIC(15,2) DEFAULT 0",
      // Proyectos
      "ALTER TABLE proyectos ADD COLUMN IF NOT EXISTS responsable TEXT",
      // Ordenes proveedor
      "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS subtotal NUMERIC(15,2) DEFAULT 0",
      "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS iva NUMERIC(15,2) DEFAULT 0",
      "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS factura_fecha TIMESTAMP",
      "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS factura_nombre TEXT",
      "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS cotizacion_nombre TEXT",
      "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS numero_op TEXT",
      "ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS created_by INTEGER",
      // Egresos
      "ALTER TABLE egresos ADD COLUMN IF NOT EXISTS proveedor_id INTEGER",
      "ALTER TABLE egresos ADD COLUMN IF NOT EXISTS factura_pdf BYTEA",
      "ALTER TABLE egresos ADD COLUMN IF NOT EXISTS factura_nombre TEXT",
      // Empresa config
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS sitio_web VARCHAR(150)",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS smtp_host VARCHAR(100)",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS smtp_port INTEGER DEFAULT 465",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS smtp_user VARCHAR(100)",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS smtp_pass VARCHAR(200)",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS sat_fiel_rfc VARCHAR(20)",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS logo_data BYTEA",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS logo_mime VARCHAR(30) DEFAULT 'image/png'",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS sat_fiel_configurado BOOLEAN DEFAULT false",
      "ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS deepseek_api_key TEXT",
      // SAT tables
      `CREATE TABLE IF NOT EXISTS sat_solicitudes (
        id SERIAL PRIMARY KEY, id_solicitud VARCHAR(100) UNIQUE,
        fecha_inicio DATE, fecha_fin DATE, tipo VARCHAR(20) DEFAULT 'CFDI',
        estatus VARCHAR(30) DEFAULT 'pendiente', paquetes TEXT,
        created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS sat_cfdis (
        id SERIAL PRIMARY KEY, uuid VARCHAR(100) UNIQUE,
        fecha_cfdi TIMESTAMP, tipo_comprobante VARCHAR(5),
        subtotal NUMERIC(15,2) DEFAULT 0, total NUMERIC(15,2) DEFAULT 0,
        moneda VARCHAR(10) DEFAULT 'MXN', emisor_rfc VARCHAR(20),
        emisor_nombre VARCHAR(300), receptor_rfc VARCHAR(20),
        receptor_nombre VARCHAR(300), uso_cfdi VARCHAR(10),
        forma_pago VARCHAR(5), metodo_pago VARCHAR(5),
        lugar_expedicion VARCHAR(10), serie VARCHAR(50), folio VARCHAR(50),
        no_certificado VARCHAR(30), version VARCHAR(5),
        fecha_timbrado TIMESTAMP, rfc_prov_certif VARCHAR(20),
        xml_content TEXT, id_paquete VARCHAR(200),
        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())`,
      // Migración: agregar columnas nuevas a sat_cfdis en schemas existentes
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS forma_pago VARCHAR(5)`,
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS metodo_pago VARCHAR(5)`,
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS lugar_expedicion VARCHAR(10)`,
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS serie VARCHAR(50)`,
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS folio VARCHAR(50)`,
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS no_certificado VARCHAR(30)`,
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS version VARCHAR(5)`,
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS fecha_timbrado TIMESTAMP`,
      `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS rfc_prov_certif VARCHAR(20)`,
      // SAT timbrado — columnas CFDI 4.0 en items_cotizacion y clientes
      `ALTER TABLE items_cotizacion ADD COLUMN IF NOT EXISTS clave_prod_serv VARCHAR(20)`,
      `ALTER TABLE items_cotizacion ADD COLUMN IF NOT EXISTS clave_unidad VARCHAR(10)`,
      `ALTER TABLE items_cotizacion ADD COLUMN IF NOT EXISTS objeto_imp VARCHAR(5) DEFAULT '02'`,
      `ALTER TABLE clientes ADD COLUMN IF NOT EXISTS uso_cfdi VARCHAR(10) DEFAULT 'G03'`,
      // RFQ
      `CREATE TABLE IF NOT EXISTS rfq (id SERIAL PRIMARY KEY,numero_rfq VARCHAR(50),descripcion TEXT NOT NULL,proyecto_nombre VARCHAR(200),prioridad VARCHAR(20) DEFAULT 'media',fecha_limite DATE,presupuesto_max NUMERIC(15,2),moneda VARCHAR(10) DEFAULT 'MXN',condiciones_pago TEXT,lugar_entrega TEXT,criterios_eval TEXT,notas TEXT,terminos TEXT,estatus VARCHAR(30) DEFAULT 'borrador',proveedor_ids JSONB DEFAULT '[]',items JSONB DEFAULT '[]',created_by INTEGER,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`,
      // Solicitudes de autorización para envío de cotizaciones
      `CREATE TABLE IF NOT EXISTS solicitudes_autorizacion (
        id SERIAL PRIMARY KEY,
        tipo VARCHAR(30) DEFAULT 'envio_cotizacion',
        referencia_id INTEGER,
        referencia_num TEXT,
        solicitante_id INTEGER,
        solicitante_nombre TEXT,
        destinatario_email TEXT,
        destinatario_cc TEXT,
        asunto TEXT,
        mensaje TEXT,
        estatus VARCHAR(20) DEFAULT 'pendiente',
        autorizado_por INTEGER,
        fecha_solicitud TIMESTAMP DEFAULT NOW(),
        fecha_resolucion TIMESTAMP,
        notas_autorizador TEXT
      )`,
    ];

    for(const empRow of allEmpresas.rows) {
      const empSchema = 'emp_' + empRow.slug.replace(/[^a-z0-9]/g,'_');
      // Verificar que el schema existe
      const schemaExists = await pool.query(
        `SELECT schema_name FROM information_schema.schemata WHERE schema_name=$1`, [empSchema]);
      if(!schemaExists.rows.length) continue;
      const mc = await pool.connect();
      try {
        await mc.query(`SET search_path TO "${empSchema}", public`);
        for(const sql of migraciones) {
          try { await mc.query(sql); } catch(e) { /* columna ya existe, ignorar */ }
        }
        console.log('  ✅ Migraciones aplicadas a schema:', empSchema);
      } finally { mc.release(); }
    }
    clearColCache(); // Limpiar cache de columnas tras migraciones

    // ══════════════════════════════════════════════════════
    // 5. Leer columnas de emp_vef para has()
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
      {u:'soporte',  n:'Técnico de Soporte',  r:'soporte', p:'soporte123'},
    ];
    for(const u of USERS){
      try {
        const hash = await bcrypt.hash(u.p, 10);
        const ex = await pool.query(`SELECT id FROM public.usuarios WHERE username=$1`,[u.u]);
        if(!ex.rows.length){
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

    // Agregar columnas SAT a facturas si no existen
    for(const col of [
      ['uuid_sat',            'TEXT'],
      ['xml_cfdi',            'TEXT'],
      ['uuid_cfdi_proveedor', 'TEXT'],
    ]){
      try{
        await pool.query(`ALTER TABLE emp_vef.facturas ADD COLUMN IF NOT EXISTS ${col[0]} ${col[1]}`);
        await pool.query(`ALTER TABLE emp_vef.ordenes_proveedor ADD COLUMN IF NOT EXISTS ${col[0]} ${col[1]}`);
      }catch{}
    }
    clearColCache(); // Invalidar cache de columnas tras setup
    console.log('\n✅ Setup VEF ERP v2.0 completo');
    console.log('   Empresa: VEF Automatización → schema: emp_vef');
    console.log('   Login: admin / admin123');
    console.log('   Trial activo: 30 días');
    console.log('   Control de licencias: ACTIVO');
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
    const empId = emp.rows[0]?.id;
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

    // ── Notificación a VEF por nuevo registro público ─────────────
    try {
      const ahora = new Date().toLocaleString('es-MX',{timeZone:'America/Mexico_City',
        day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'});
      await mailer.sendMail({
        from: `"${VEF_NOMBRE}" <${process.env.SMTP_USER}>`,
        to: VEF_CORREO,
        subject: `🆕 Nuevo registro: ${empresa_nombre} — ${fullName}`,
        html: `
          <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden">
            <div style="background:linear-gradient(135deg,#059669,#047857);padding:24px 28px">
              <div style="color:#fff;font-size:1.3rem;font-weight:700">🆕 Nuevo Cliente Registrado</div>
              <div style="color:rgba(255,255,255,.6);font-size:.85rem;margin-top:4px">${VEF_NOMBRE} — ERP Industrial</div>
            </div>
            <div style="padding:24px 28px;background:#fff">
              <table style="width:100%;border-collapse:collapse;font-size:.9rem">
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b;width:140px">Empresa</td>
                  <td style="padding:10px 0;font-weight:700;color:#0D2B55">${empresa_nombre}</td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b">Contacto</td>
                  <td style="padding:10px 0">${fullName}</td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b">Email</td>
                  <td style="padding:10px 0"><a href="mailto:${email}" style="color:#2563eb">${email}</a></td>
                </tr>
                ${telefono ? `<tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b">Teléfono</td>
                  <td style="padding:10px 0">${telefono}</td>
                </tr>` : ''}
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b">Schema BD</td>
                  <td style="padding:10px 0;font-family:monospace;font-size:.85rem;color:#6366f1">${schema}</td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b">Trial hasta</td>
                  <td style="padding:10px 0">
                    <span style="background:#fef9c3;color:#92400e;padding:2px 10px;border-radius:10px;font-size:.82rem;font-weight:700">
                      ⏳ ${trialHasta.toISOString().slice(0,10)}
                    </span>
                  </td>
                </tr>
                <tr>
                  <td style="padding:10px 0;color:#64748b">Fecha/Hora</td>
                  <td style="padding:10px 0;color:#94a3b8;font-size:.85rem">${ahora}</td>
                </tr>
              </table>
            </div>
            <div style="background:#f8fafc;padding:14px 28px;font-size:.78rem;color:#94a3b8;text-align:center">
              Registro automático desde el portal — ${VEF_NOMBRE} ERP
            </div>
          </div>`
      });
      console.log('📧 Notificación de nuevo registro enviada a', VEF_CORREO);
    } catch(emailErr) {
      console.warn('⚠️  No se pudo enviar notificación de registro:', emailErr.message);
    }
    // ─────────────────────────────────────────────────────────────

    res.status(201).json({
      ok:true,
      mensaje:'Cuenta creada exitosamente. Trial de 30 días activo.',
      empresa:{id:empId,nombre:empresa_nombre,slug,trial_hasta:trialHasta.toISOString().slice(0,10)},
      usuario:{id:usr.rows[0]?.id,nombre:fullName,email}
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
    res.status(201).json({...emp.rows[0], schema});
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
    res.json(r.rows[0]);
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
    // Buscar por username O por email — buscar TODOS los matches (puede haber mismo username en distintas empresas)
    let result = await pool.query('SELECT * FROM public.usuarios WHERE username=$1 ORDER BY id',[username]);
    if(!result.rows.length){
      result = await pool.query('SELECT * FROM public.usuarios WHERE email=$1 ORDER BY id',[username]).catch(()=>({rows:[]}));
    }
    if(!result.rows.length) return res.status(401).json({error:'Usuario no encontrado'});

    // Si vino empresa_id en el body, filtrar por esa empresa
    let user = null;
    if(empresa_id){
      user = result.rows.find(u => String(u.empresa_id) === String(empresa_id));
    }
    // Si no se especificó empresa o no encontró, tomar el primero que tenga contraseña correcta
    if(!user){
      for(const u of result.rows){
        const h = u.password_hash || u.password || u.contrasena || '';
        if(h && await bcrypt.compare(password, h)){ user = u; break; }
      }
    }
    if(!user) return res.status(401).json({error:'Usuario no encontrado'});
    if(user.activo===false) return res.status(401).json({error:'Usuario desactivado'});

    // Verificar contraseña
    const hash = user.password_hash || user.password || user.contrasena || '';
    if(!hash) return res.status(401).json({error:'Sin contraseña configurada. Ejecuta /api/fix?key=vef2025'});
    const passOk = await bcrypt.compare(password, hash);
    if(!passOk) return res.status(401).json({error:'Contraseña incorrecta'});

    // Actualizar último acceso
    try { await pool.query('UPDATE public.usuarios SET ultimo_acceso=NOW() WHERE id=$1',[user.id]); } catch{}

    // Empresa del usuario — SIEMPRE derivar schema del slug real en la BD
    let empId = user.empresa_id || global._defaultEmpresaId;
    if(!empId) return res.status(400).json({error:'Usuario sin empresa asignada. Contacta al administrador.'});

    const empRow = await pool.query('SELECT id,nombre,slug,activa FROM public.empresas WHERE id=$1 LIMIT 1',[empId]);
    if(!empRow.rows[0]) return res.status(400).json({error:'Empresa no encontrada en la BD.'});

    const empSlug   = empRow.rows[0].slug;
    const empNombre = empRow.rows[0].nombre;
    // Schema SIEMPRE derivado del slug — esto garantiza aislamiento total entre empresas
    const schema = 'emp_' + empSlug.replace(/[^a-z0-9]/g,'_');

    // Actualizar schema_name del usuario si está desactualizado
    if(user.schema_name !== schema){
      console.log('Login: actualizando schema de usuario', user.username, user.schema_name, '→', schema);
      try { await pool.query('UPDATE public.usuarios SET schema_name=$1,empresa_id=$2 WHERE id=$3',[schema,empId,user.id]); } catch{}
    }
    console.log('Login OK:', user.username, '| empresa:', empNombre, '| schema:', schema);

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
app.get('/api/dashboard/metrics', auth, licencia, async (req,res)=>{
  const schema = (req.user?.schema || req.user?.schema_name || global._defaultSchema || 'emp_vef').replace(/["']/g,'');
  const mes_actual = new Date().toLocaleDateString('es-MX',{month:'long',year:'numeric'});
  const base = {cotizaciones_activas:0,clientes:0,proveedores:0,items_inventario:0,
    facturas_pendientes:0,tareas_pendientes:0,ing_mes:0,egr_mes:0,cob_mes:0,
    empresa:{nombre:'VEF Automatización'},cot_recientes:[],fac_vencer:[],inv_bajo:[],mes_actual};

  // Timeout duro 25s — RDS Ohio puede tener latencia alta
  const tid = setTimeout(()=>{ if(!res.headersSent) res.json({...base,_warn:'timeout'}); },25000);
  res.on('finish',()=>clearTimeout(tid));

  const M = new Date().getMonth()+1, Y = new Date().getFullYear();

  // Helper: ejecuta una query con su propia conexión del pool y search_path
  const qp = (sql, p=[]) => pool.query(
    `SET search_path TO ${schema},public; ${sql}`, p
  ).then(r => Array.isArray(r) ? r[r.length-1].rows : r.rows).catch(()=>[]);

  // Un solo cliente para la query grande de estadísticas
  let client;
  try { client = await pool.connect(); }
  catch(e){ clearTimeout(tid); return res.json({...base,_warn:'no_conn:'+e.message}); }

  try {
    await client.query('SET search_path TO '+schema+',public');

    // Una sola query para todos los conteos — evita múltiples round-trips
    const statsRow = await client.query(`SELECT
      (SELECT COUNT(*)       FROM cotizaciones)::int                                    cots,
      (SELECT COUNT(*)       FROM clientes     WHERE COALESCE(activo,true)=true)::int   clts,
      (SELECT COUNT(*)       FROM proveedores  WHERE COALESCE(activo,true)=true)::int   provs,
      (SELECT COUNT(DISTINCT nombre) FROM inventario WHERE COALESCE(activo,true)=true)::int prods,
      (SELECT COUNT(*)       FROM facturas     WHERE estatus IN ('pendiente','parcial'))::int facts,
      (SELECT COUNT(*)       FROM tareas       WHERE estatus!='completada')::int         tar,
      (SELECT COALESCE(SUM(total),0) FROM facturas
         WHERE EXTRACT(MONTH FROM fecha_emision)=${M} AND EXTRACT(YEAR FROM fecha_emision)=${Y}) ing,
      (SELECT COALESCE(SUM(monto),0) FROM pagos
         WHERE EXTRACT(MONTH FROM fecha)=${M} AND EXTRACT(YEAR FROM fecha)=${Y}) cob,
      (SELECT COALESCE(SUM(total),0) FROM egresos
         WHERE EXTRACT(MONTH FROM fecha)=${M} AND EXTRACT(YEAR FROM fecha)=${Y}) egr_dir,
      (SELECT COALESCE(SUM(total),0) FROM ordenes_proveedor
         WHERE estatus IN ('aprobada','recibida','pagada')
           AND EXTRACT(MONTH FROM fecha_emision)=${M} AND EXTRACT(YEAR FROM fecha_emision)=${Y}) egr_oc,
      (SELECT COUNT(*) FROM ordenes_proveedor
         WHERE estatus IN ('borrador','enviada','pendiente'))::int oc_pend,
      (SELECT COALESCE(SUM(total),0) FROM ordenes_proveedor
         WHERE EXTRACT(MONTH FROM fecha_emision)=${M} AND EXTRACT(YEAR FROM fecha_emision)=${Y}) oc_mes
    `).then(r=>r.rows[0]).catch(()=>({}));

    // Queries de listas en paralelo usando el mismo cliente (secuencial pero rápido)
    const emp   = await client.query('SELECT nombre,rfc,telefono,email,ciudad FROM empresa_config LIMIT 1').then(r=>r.rows[0]||{}).catch(()=>({}));
    const cots5 = await client.query('SELECT c.numero_cotizacion,c.total,c.moneda,c.estatus,c.created_at,cl.nombre cliente_nombre FROM cotizaciones c LEFT JOIN proyectos p ON p.id=c.proyecto_id LEFT JOIN clientes cl ON cl.id=p.cliente_id ORDER BY c.created_at DESC LIMIT 5').then(r=>r.rows).catch(()=>[]);
    const facs5 = await client.query("SELECT f.numero_factura,f.total,f.moneda,f.estatus,f.fecha_vencimiento,cl.nombre cliente_nombre FROM facturas f LEFT JOIN clientes cl ON cl.id=f.cliente_id WHERE f.estatus IN ('pendiente','parcial') ORDER BY f.fecha_vencimiento LIMIT 5").then(r=>r.rows).catch(()=>[]);
    const inv6  = await client.query("SELECT DISTINCT ON (nombre) nombre,unidad,COALESCE(cantidad_actual,stock_actual,0) stock,COALESCE(cantidad_minima,stock_minimo,0) minimo FROM inventario WHERE COALESCE(activo,true)=true AND COALESCE(cantidad_actual,stock_actual,0)<=COALESCE(cantidad_minima,stock_minimo,0) ORDER BY nombre LIMIT 6").then(r=>r.rows).catch(()=>[]);

    res.json({
      cotizaciones_activas: parseInt(statsRow?.cots||0),
      clientes:             parseInt(statsRow?.clts||0),
      proveedores:          parseInt(statsRow?.provs||0),
      items_inventario:     parseInt(statsRow?.prods||0),
      facturas_pendientes:  parseInt(statsRow?.facts||0),
      tareas_pendientes:    parseInt(statsRow?.tar||0),
      ing_mes:    parseFloat(statsRow?.ing||0),
      cob_mes:    parseFloat(statsRow?.cob||0),
      egr_mes:    parseFloat(statsRow?.egr_dir||0) + parseFloat(statsRow?.egr_oc||0),
      egr_directo:parseFloat(statsRow?.egr_dir||0),
      egr_oc:     parseFloat(statsRow?.egr_oc||0),
      oc_pendientes:parseInt(statsRow?.oc_pend||0),
      oc_mes:     parseFloat(statsRow?.oc_mes||0),
      iva_mes: await client.query(`SELECT COALESCE(SUM(iva),0) v FROM facturas WHERE EXTRACT(MONTH FROM fecha_emision)=${M} AND EXTRACT(YEAR FROM fecha_emision)=${Y}`).then(r=>parseFloat(r.rows[0]?.v||0)).catch(()=>0),
      isr_mes: await client.query(`SELECT COALESCE(SUM(retencion_isr),0) v FROM facturas WHERE EXTRACT(MONTH FROM fecha_emision)=${M} AND EXTRACT(YEAR FROM fecha_emision)=${Y}`).then(r=>parseFloat(r.rows[0]?.v||0)).catch(()=>0),
      empresa: emp,
      cot_recientes: cots5,
      fac_vencer:    facs5,
      inv_bajo:      inv6,
      mes_actual
    });
  } catch(e){
    console.error('dashboard err:', e.message);
    if(!res.headersSent) res.json({...base, _error:e.message});
  } finally {
    try{ client.release(); }catch{}
    clearTimeout(tid);
  }
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
    res.json({ok:true, empresa:r.rows[0]});
  } catch(e){ res.status(500).json({error:e.message}); }
});

// Eliminar empresa completa (admin global) — borra schema + usuarios + registro
app.delete('/api/admin/empresas/:id', auth, async (req,res)=>{
  if(req.user.rol !== 'admin') return res.status(403).json({error:'Solo admin'});
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Obtener datos de la empresa
    const empR = await client.query('SELECT slug,nombre FROM public.empresas WHERE id=$1',[req.params.id]);
    if(!empR.rows.length){ await client.query('ROLLBACK'); return res.status(404).json({error:'Empresa no encontrada'}); }
    const { slug, nombre } = empR.rows[0];
    const schema = 'emp_' + slug.replace(/[^a-z0-9]/g,'_');
    // No permitir borrar empresa VEF principal
    if(slug === 'vef') { await client.query('ROLLBACK'); return res.status(400).json({error:'No se puede eliminar la empresa principal del sistema'}); }
    // 1. Borrar usuarios de la empresa
    const delUsers = await client.query('DELETE FROM public.usuarios WHERE empresa_id=$1',[req.params.id]);
    // 2. Borrar schema completo de la empresa (CASCADE borra todas sus tablas)
    let schemaDropped = false;
    try {
      await client.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      schemaDropped = true;
    } catch(e){ console.warn('Schema drop error:', e.message); }
    // 3. Borrar registro de empresa
    await client.query('DELETE FROM public.empresas WHERE id=$1',[req.params.id]);
    await client.query('COMMIT');
    console.log(`🗑  Empresa eliminada: ${nombre} (schema: ${schema}, usuarios: ${delUsers.rowCount})`);
    res.json({ ok:true, mensaje:`Empresa "${nombre}" eliminada correctamente`, schema_eliminado:schemaDropped, usuarios_eliminados:delUsers.rowCount });
  } catch(e){
    await client.query('ROLLBACK');
    console.error('delete empresa:', e.message);
    res.status(500).json({error:e.message});
  } finally { client.release(); }
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

    // ── Notificación por correo a VEF ────────────────────────────
    try {
      const fechaVence = new Date();
      fechaVence.setDate(fechaVence.getDate() + dias);
      const fechaStr = fechaVence.toLocaleDateString('es-MX',{day:'2-digit',month:'long',year:'numeric'});
      const creadoPor = req.user?.nombre || req.user?.username || 'Admin';
      const ahora = new Date().toLocaleString('es-MX',{timeZone:'America/Mexico_City',
        day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'});
      await mailer.sendMail({
        from: `"${VEF_NOMBRE}" <${process.env.SMTP_USER}>`,
        to: VEF_CORREO,
        subject: `🏢 Nueva empresa registrada: ${nombre}`,
        html: `
          <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden">
            <div style="background:linear-gradient(135deg,#0D2B55,#1a3a6b);padding:24px 28px">
              <div style="color:#fff;font-size:1.3rem;font-weight:700">🏢 Nueva Empresa Creada</div>
              <div style="color:rgba(255,255,255,.6);font-size:.85rem;margin-top:4px">${VEF_NOMBRE} — ERP Industrial</div>
            </div>
            <div style="padding:24px 28px;background:#fff">
              <table style="width:100%;border-collapse:collapse;font-size:.9rem">
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b;width:140px">Empresa</td>
                  <td style="padding:10px 0;font-weight:700;color:#0D2B55">${nombre}</td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b">Schema BD</td>
                  <td style="padding:10px 0;font-family:monospace;font-size:.85rem;color:#6366f1">${schema}</td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b">Trial</td>
                  <td style="padding:10px 0">
                    <span style="background:#fef9c3;color:#92400e;padding:2px 10px;border-radius:10px;font-size:.82rem;font-weight:700">
                      ⏳ ${dias} días — vence ${fechaStr}
                    </span>
                  </td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9">
                  <td style="padding:10px 0;color:#64748b">Creado por</td>
                  <td style="padding:10px 0">${creadoPor}</td>
                </tr>
                <tr>
                  <td style="padding:10px 0;color:#64748b">Fecha/Hora</td>
                  <td style="padding:10px 0;color:#94a3b8;font-size:.85rem">${ahora}</td>
                </tr>
              </table>
            </div>
            <div style="background:#f8fafc;padding:14px 28px;font-size:.78rem;color:#94a3b8;text-align:center">
              Notificación automática — ${VEF_NOMBRE} ERP
            </div>
          </div>`
      });
      console.log('📧 Notificación de nueva empresa enviada a', VEF_CORREO);
    } catch(emailErr) {
      console.warn('⚠️  No se pudo enviar notificación de nueva empresa:', emailErr.message);
    }
    // ─────────────────────────────────────────────────────────────

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
    const schema = 'emp_'+(emp.rows[0]?.slug||'').replace(/[^a-z0-9]/g,'_');
    await pool.query('UPDATE public.usuarios SET empresa_id=$1,schema_name=$2 WHERE id=$3',[empresa_id,schema,req.params.id]);
    res.json({ok:true, schema, empresa:emp.rows[0]?.nombre});
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
app.get('/api/reportes/cotizaciones', auth, licencia, async (req,res)=>{
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
app.get('/api/reportes/facturas-pendientes', auth, licencia, async (req,res)=>{
  const estCol=has('facturas','estatus_pago')?'f.estatus_pago':'f.estatus';
  res.json(await QR(req,`
    SELECT f.numero_factura, COALESCE(f.total,f.monto,0) monto, ${estCol} estatus,
           TO_CHAR(f.fecha_emision,'DD/MM/YYYY') fecha
    FROM facturas f WHERE ${estCol}='pendiente' ORDER BY f.id DESC`));
});
app.get('/api/reportes/proyectos-activos', auth, licencia, async (req,res)=>{
  const respCol=has('proyectos','responsable')?"p.responsable":"'VEF Automatización'";
  res.json(await QR(req,`
    SELECT p.nombre, c.nombre cliente, ${respCol} responsable,
           TO_CHAR(COALESCE(p.fecha_creacion,NOW()::date),'DD/MM/YYYY') fecha
    FROM proyectos p JOIN clientes c ON p.cliente_id=c.id
    WHERE p.estatus='activo' ORDER BY p.nombre`));
});
app.get('/api/reportes/inventario-bajo', auth, licencia, async (req,res)=>{
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
// ═══════════════════════════════════════════════════════════════
// CRM — Oportunidades y Actividades
// ═══════════════════════════════════════════════════════════════

app.get('/api/crm/oportunidades', auth, licencia, async (req,res)=>{
  try{
    const rows = await QR(req,`
      SELECT o.*, cl.nombre cliente_nombre, cl.tipo_persona, cl.email cliente_email
      FROM crm_oportunidades o
      LEFT JOIN clientes cl ON cl.id=o.cliente_id
      ORDER BY o.updated_at DESC`);
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/crm/oportunidades', auth, empresaActiva, licencia, async (req,res)=>{
  try{
    const {cliente_id,nombre,etapa,valor,moneda,probabilidad,fecha_cierre_est,
           responsable,descripcion,origen}=req.body;
    if(!cliente_id||!nombre) return res.status(400).json({error:'cliente_id y nombre requeridos'});
    const rows=await QR(req,`INSERT INTO crm_oportunidades
      (cliente_id,nombre,etapa,valor,moneda,probabilidad,fecha_cierre_est,
       responsable,descripcion,origen,created_by)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [cliente_id,nombre,etapa||'prospecto',parseFloat(valor)||0,moneda||'MXN',
       parseInt(probabilidad)||20,fecha_cierre_est||null,
       responsable||null,descripcion||null,origen||null,req.user?.id]);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.put('/api/crm/oportunidades/:id', auth, empresaActiva, licencia, async (req,res)=>{
  try{
    const b=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(k,v)=>{if(v!==undefined){sets.push(`${k}=$${i++}`);vals.push(v);}};
    add('nombre',          b.nombre);
    add('etapa',           b.etapa);
    add('valor',           b.valor!==undefined?parseFloat(b.valor)||0:undefined);
    add('moneda',          b.moneda);
    add('probabilidad',    b.probabilidad!==undefined?parseInt(b.probabilidad)||0:undefined);
    add('fecha_cierre_est',b.fecha_cierre_est||null);
    add('responsable',     b.responsable!==undefined?b.responsable:undefined);
    add('descripcion',     b.descripcion!==undefined?b.descripcion||null:undefined);
    add('origen',          b.origen!==undefined?b.origen||null:undefined);
    add('perdida_motivo',  b.perdida_motivo!==undefined?b.perdida_motivo||null:undefined);
    if(!sets.length) return res.status(400).json({error:'Nada que actualizar'});
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE crm_oportunidades SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/crm/oportunidades/:id', auth, empresaActiva, async (req,res)=>{
  try{
    await QR(req,'DELETE FROM crm_actividades WHERE oportunidad_id=$1',[req.params.id]);
    await QR(req,'DELETE FROM crm_oportunidades WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/crm/actividades', auth, licencia, async (req,res)=>{
  try{
    const where = req.query.cliente_id ? 'WHERE a.cliente_id=$1' : '';
    const vals  = req.query.cliente_id ? [req.query.cliente_id] : [];
    const rows  = await QR(req,`
      SELECT a.*, cl.nombre cliente_nombre, o.nombre oportunidad_nombre
      FROM crm_actividades a
      LEFT JOIN clientes cl ON cl.id=a.cliente_id
      LEFT JOIN crm_oportunidades o ON o.id=a.oportunidad_id
      ${where} ORDER BY a.fecha DESC LIMIT 100`,vals);
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/crm/actividades', auth, empresaActiva, licencia, async (req,res)=>{
  try{
    const {cliente_id,oportunidad_id,tipo,titulo,descripcion,
           proxima_accion,proxima_fecha,fecha}=req.body;
    const rows=await QR(req,`INSERT INTO crm_actividades
      (cliente_id,oportunidad_id,tipo,titulo,descripcion,proxima_accion,proxima_fecha,fecha,created_by)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [cliente_id||null,oportunidad_id||null,tipo||'nota',titulo||null,
       descripcion||null,proxima_accion||null,proxima_fecha||null,
       fecha||new Date().toISOString(),req.user?.id]);
    // Update cliente ultima_actividad
    if(cliente_id){
      await QR(req,'UPDATE clientes SET ultima_actividad=NOW() WHERE id=$1',[cliente_id]).catch(()=>{});
    }
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.put('/api/crm/actividades/:id', auth, async (req,res)=>{
  try{
    const {completada}=req.body;
    const rows=await QR(req,'UPDATE crm_actividades SET completada=$1 WHERE id=$2 RETURNING *',
      [completada,req.params.id]);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/crm/dashboard', auth, licencia, async (req,res)=>{
  try{
    const ops = await QR(req,`SELECT etapa, COUNT(*) cnt, SUM(valor) valor
      FROM crm_oportunidades GROUP BY etapa`);
    const acts = await QR(req,`SELECT COUNT(*) cnt FROM crm_actividades
      WHERE completada=false AND proxima_fecha <= CURRENT_DATE + 7`);
    const topClis = await QR(req,`SELECT cl.nombre, COUNT(o.id) oportunidades, SUM(o.valor) valor
      FROM crm_oportunidades o JOIN clientes cl ON cl.id=o.cliente_id
      WHERE o.etapa NOT IN ('perdida')
      GROUP BY cl.id,cl.nombre ORDER BY valor DESC NULLS LAST LIMIT 5`);
    res.json({pipeline:ops, actividades_pendientes:parseInt(acts[0]?.cnt||0), top_clientes:topClis});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/clientes', auth, licencia, async (req,res)=>{
  const w=has('clientes','activo')?'WHERE activo=true':'';
  res.json(await QR(req,`SELECT * FROM clientes ${w} ORDER BY nombre`));
});
app.get('/api/clientes/:id', auth, licencia, async (req,res)=>{
  const r=await QR(req,'SELECT * FROM clientes WHERE id=$1',[req.params.id]);
  r[0]?res.json(r[0]):res.status(404).json({error:'No encontrado'});
});
app.post('/api/clientes', auth, empresaActiva, licencia, async (req,res)=>{
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
app.put('/api/clientes/:id', auth, empresaActiva, licencia, async (req,res)=>{
  try {
    const b=req.body;
    const sets=[]; const vals=[]; let i=1;
    const add=(c,v)=>{if(v!==undefined){sets.push(`${c}=$${i++}`);vals.push(v);}};
    add('nombre',           b.nombre);
    add('contacto',         b.contacto!==undefined?b.contacto:undefined);
    add('direccion',        b.direccion!==undefined?b.direccion:undefined);
    add('telefono',         b.telefono!==undefined?b.telefono:undefined);
    add('email',            b.email!==undefined?b.email:undefined);
    if(has('clientes','rfc'))            add('rfc', b.rfc?.toUpperCase()||null);
    if(has('clientes','regimen_fiscal')) add('regimen_fiscal', b.regimen_fiscal||null);
    if(has('clientes','cp'))             add('cp', b.cp||null);
    if(has('clientes','ciudad'))         add('ciudad', b.ciudad||null);
    if(has('clientes','tipo_persona'))   add('tipo_persona', b.tipo_persona||'moral');
    if(has('clientes','uso_cfdi'))       { if(b.uso_cfdi!==undefined) add('uso_cfdi', b.uso_cfdi); }
    if(has('clientes','limite_credito')) { if(b.limite_credito!==undefined) add('limite_credito', parseFloat(b.limite_credito)||0); }
    if(!sets.length) return res.status(400).json({error:'Nada que actualizar'});
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE clientes SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/clientes/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
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
app.get('/api/clientes/:id/pdf', auth, licencia, async (req,res)=>{
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
// ── Evaluación de Proveedores ─────────────────────────────────────
app.post('/api/proveedores/:id/evaluacion', auth, empresaActiva, async (req,res)=>{
  try{
    // Ensure table exists
    await QR(req,`CREATE TABLE IF NOT EXISTS evaluaciones_proveedores (
      id SERIAL PRIMARY KEY, proveedor_id INTEGER NOT NULL,
      periodo VARCHAR(100), referencia VARCHAR(100),
      calidad NUMERIC(3,1), precio NUMERIC(3,1), entrega NUMERIC(3,1),
      servicio NUMERIC(3,1), documentacion NUMERIC(3,1), garantia NUMERIC(3,1),
      calificacion_total NUMERIC(4,2), recomendacion VARCHAR(30),
      notas TEXT, created_by INTEGER, created_at TIMESTAMP DEFAULT NOW()
    )`).catch(()=>{});

    const {periodo,referencia,notas,recomendacion,calidad,precio,entrega,
           servicio,documentacion,garantia,calificacion_total}=req.body;
    await QR(req,`INSERT INTO evaluaciones_proveedores
      (proveedor_id,periodo,referencia,calidad,precio,entrega,servicio,documentacion,garantia,calificacion_total,recomendacion,notas,created_by)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [req.params.id,periodo||null,referencia||null,
       parseFloat(calidad)||0,parseFloat(precio)||0,parseFloat(entrega)||0,
       parseFloat(servicio)||0,parseFloat(documentacion)||0,parseFloat(garantia)||0,
       parseFloat(calificacion_total)||0,recomendacion||'mantener',notas||null,req.user?.id]);

    // Update promedio in proveedores
    const rows = await QR(req,`SELECT AVG(calificacion_total) prom, COUNT(*) cnt
      FROM evaluaciones_proveedores WHERE proveedor_id=$1`,[req.params.id]);
    const prom = parseFloat(rows[0]?.prom||0);
    const cnt  = parseInt(rows[0]?.cnt||0);
    await QR(req,`UPDATE proveedores SET calificacion_promedio=$1 WHERE id=$2`,
      [Math.round(prom*10)/10, req.params.id]).catch(()=>{});
    // Try with num_evaluaciones column
    await QR(req,`ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS calificacion_promedio NUMERIC(3,1)`).catch(()=>{});
    await QR(req,`ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS num_evaluaciones INTEGER DEFAULT 0`).catch(()=>{});
    await QR(req,`UPDATE proveedores SET calificacion_promedio=$1, num_evaluaciones=$2 WHERE id=$3`,
      [Math.round(prom*10)/10, cnt, req.params.id]).catch(()=>{});

    res.json({ok:true,calificacion_total:Math.round(prom*10)/10});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/proveedores/:id/evaluaciones', auth, async (req,res)=>{
  try{
    const rows = await QR(req,
      'SELECT * FROM evaluaciones_proveedores WHERE proveedor_id=$1 ORDER BY created_at DESC',
      [req.params.id]);
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/proveedores', auth, licencia, async (req,res)=>{
  const w=has('proveedores','activo')?'WHERE activo=true':'';
  res.json(await QR(req,`SELECT * FROM proveedores ${w} ORDER BY nombre`));
});
app.post('/api/proveedores', auth, empresaActiva, licencia, async (req,res)=>{
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
app.put('/api/proveedores/:id', auth, empresaActiva, licencia, async (req,res)=>{
  try {
    const b=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(c,v)=>{if(v!==undefined){sets.push(`${c}=$${i++}`);vals.push(v);}};
    add('nombre',    b.nombre);
    if(b.contacto!==undefined)  add('contacto',   b.contacto);
    if(b.direccion!==undefined) add('direccion',  b.direccion);
    if(b.telefono!==undefined)  add('telefono',   b.telefono);
    if(b.email!==undefined)     add('email',      b.email);
    if(has('proveedores','rfc'))              { if(b.rfc!==undefined) add('rfc', b.rfc?.toUpperCase()||null); }
    if(has('proveedores','condiciones_pago')) { if(b.condiciones_pago!==undefined) add('condiciones_pago', b.condiciones_pago||null); }
    if(has('proveedores','tipo_persona'))     { if(b.tipo_persona!==undefined) add('tipo_persona', b.tipo_persona||'moral'); }
    if(has('proveedores','notas'))            { if(b.notas!==undefined) add('notas', b.notas||null); }
    if(has('proveedores','banco'))            { if(b.banco!==undefined) add('banco', b.banco||null); }
    if(has('proveedores','cuenta_bancaria'))  { if(b.cuenta_bancaria!==undefined) add('cuenta_bancaria', b.cuenta_bancaria||null); }
    if(!sets.length) return res.status(400).json({error:'Nada que actualizar'});
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE proveedores SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/proveedores/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
  if(has('proveedores','activo')) await QR(req,'UPDATE proveedores SET activo=false WHERE id=$1',[req.params.id]);
  else await QR(req,'DELETE FROM proveedores WHERE id=$1',[req.params.id]);
  res.json({ok:true});
});

app.get('/api/proveedores/:id/pdf', auth, licencia, async (req,res)=>{
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
app.get('/api/proyectos', auth, licencia, async (req,res)=>{
  const respCol  = has('proyectos','responsable') ? 'p.responsable,' : '';
  const fechaCol = has('proyectos','fecha')        ? 'p.fecha,'        : "COALESCE(p.fecha_creacion,p.created_at) fecha,";
  const extraCols = [
    has('proyectos','fecha_fin')    ? 'p.fecha_fin,'    : '',
    has('proyectos','avance')       ? 'p.avance,'       : '',
    has('proyectos','presupuesto')  ? 'p.presupuesto,'  : '',
    has('proyectos','descripcion')  ? 'p.descripcion,'  : '',
  ].join('');
  res.json(await QR(req,`
    SELECT p.id, p.nombre, p.cliente_id, ${respCol} p.estatus,
           ${fechaCol} ${extraCols}
           c.nombre cliente_nombre
    FROM proyectos p LEFT JOIN clientes c ON c.id=p.cliente_id
    ORDER BY p.id DESC`));
});
app.post('/api/proyectos', auth, empresaActiva, licencia, async (req,res)=>{
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
app.put('/api/proyectos/:id', auth, empresaActiva, licencia, async (req,res)=>{
  try {
    const b=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(c,v)=>{if(v!==undefined){sets.push(`${c}=$${i++}`);vals.push(v);}};
    add('nombre',      b.nombre);
    add('estatus',     b.estatus);
    add('cliente_id',  b.cliente_id!==undefined ? (parseInt(b.cliente_id)||null) : undefined);
    if(has('proyectos','responsable'))  add('responsable',  b.responsable!==undefined?b.responsable:undefined);
    if(has('proyectos','fecha'))        add('fecha',         b.fecha||null);
    if(has('proyectos','fecha_fin'))    add('fecha_fin',     b.fecha_fin||null);
    if(has('proyectos','avance'))       add('avance',        b.avance!==undefined?parseInt(b.avance)||0:undefined);
    if(has('proyectos','presupuesto'))  add('presupuesto',   b.presupuesto!==undefined?parseFloat(b.presupuesto)||null:undefined);
    if(has('proyectos','descripcion'))  add('descripcion',   b.descripcion!==undefined?b.descripcion:undefined);
    if(has('proyectos','updated_at'))   { sets.push(`updated_at=NOW()`); }
    if(!sets.length) return res.status(400).json({error:'Nada que actualizar'});
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE proyectos SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/proyectos/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
  await QR(req,'DELETE FROM proyectos WHERE id=$1',[req.params.id]); res.json({ok:true});
});

// ================================================================
// COTIZACIONES
// ================================================================
app.get('/api/cotizaciones', auth, licencia, async (req,res)=>{
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

app.get('/api/cotizaciones/:id', auth, licencia, async (req,res)=>{
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

app.post('/api/cotizaciones', auth, empresaActiva, licencia, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
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
      if(has('items_cotizacion','clave_prod_serv')){
        await client.query(
          'INSERT INTO items_cotizacion (cotizacion_id,descripcion,clave_prod_serv,clave_unidad,objeto_imp,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
          [cot.id, it.descripcion, it.clave_prod_serv||null, it.clave_unidad||'H87', it.objeto_imp||'02',
           it.cantidad, it.precio_unitario, (parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0)]);
      } else {
        await client.query(
          'INSERT INTO items_cotizacion (cotizacion_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
          [cot.id, it.descripcion, it.cantidad, it.precio_unitario, (parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0)]);
      }
    }
    await client.query('COMMIT');
    res.status(201).json(cot);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.put('/api/cotizaciones/:id', auth, empresaActiva, licencia, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
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
      for(const it of items){
        const tot=(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0);
        if(has('items_cotizacion','clave_prod_serv')){
          await client.query(
            'INSERT INTO items_cotizacion (cotizacion_id,descripcion,clave_prod_serv,clave_unidad,objeto_imp,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
            [req.params.id, it.descripcion, it.clave_prod_serv||null, it.clave_unidad||'H87', it.objeto_imp||'02',
             it.cantidad, it.precio_unitario, tot]);
        } else {
          await client.query(
            'INSERT INTO items_cotizacion (cotizacion_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
            [req.params.id, it.descripcion, it.cantidad, it.precio_unitario, tot]);
        }
      }
    }
    await client.query('COMMIT');
    res.json((await QR(req,'SELECT * FROM cotizaciones WHERE id=$1',[req.params.id]))[0]);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.delete('/api/cotizaciones/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
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

app.get('/api/cotizaciones/:id/pdf', auth, licencia, async (req,res)=>{
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

app.post('/api/cotizaciones/:id/email', auth, empresaActiva, licencia, async (req,res)=>{
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
app.get('/api/ordenes-proveedor', auth, licencia, async (req,res)=>{
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

app.get('/api/ordenes-proveedor/:id', auth, licencia, async (req,res)=>{
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

app.post('/api/ordenes-proveedor', auth, empresaActiva, licencia, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
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

app.put('/api/ordenes-proveedor/:id', auth, empresaActiva, licencia, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
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
    // Get updated OC with proveedor_nombre via JOIN
    const updArr=await QR(req,`SELECT op.*, p.nombre proveedor_nombre FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id WHERE op.id=$1`,[req.params.id]);
    const [updated]=updArr;

    // ── AUTO-DESCUENTO TESORERÍA al marcar como pagada ────────────────────────
    // Ensure column exists (runs fast if already exists)
    await Q(`ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`, [], schema).catch(()=>{});
    if (estatus === 'pagada' && updated) {
      try {
        // Verificar si ya existe un movimiento ligado a esta OC
        const movExiste = await Q(`
          SELECT id FROM movimientos_bancarios
          WHERE orden_compra_id=$1 LIMIT 1`, [req.params.id], schema);

        if (!movExiste.length) {
          // Buscar la primera cuenta activa para el egreso
          const cuentas = await Q(
            `SELECT id, saldo_actual, moneda FROM cuentas_bancarias WHERE activo=true ORDER BY id LIMIT 1`,
            [], schema).catch(() => []);

          const monto = parseFloat(updated.total || 0);
          const concepto = `Pago OC ${updated.numero_op||'#'+req.params.id} — ${updated.proveedor_nombre||'Proveedor'}`;

          if (cuentas.length && monto > 0) {
            const cuenta = cuentas[0];
            const nuevoSaldo = parseFloat(cuenta.saldo_actual || 0) - monto;

            // Crear egreso en tesorería
            await Q(`ALTER TABLE movimientos_bancarios ADD COLUMN IF NOT EXISTS orden_compra_id INTEGER`, [], schema).catch(() => {});
            await Q(`
              INSERT INTO movimientos_bancarios
                (cuenta_id, tipo_operacion, fecha, concepto, monto, moneda,
                 categoria, beneficiario, saldo_posterior, orden_compra_id, created_by)
              VALUES ($1,'egreso',CURRENT_DATE,$2,$3,$4,'Pago Proveedor',$5,$6,$7,$8)`,
              [cuenta.id, concepto, monto, updated.moneda || cuenta.moneda || 'MXN',
               updated.proveedor_nombre || null, nuevoSaldo,
               req.params.id, req.user?.id], schema).catch(() => {});

            // Actualizar saldo de la cuenta
            await Q(
              `UPDATE cuentas_bancarias SET saldo_actual=$1 WHERE id=$2`,
              [nuevoSaldo, cuenta.id], schema).catch(() => {});

            console.log(`💳 OC ${updated.numero_op} pagada → tesorería descontado ${monto} de cuenta ${cuenta.id}`);
          } else if (monto > 0) {
            // Sin cuenta bancaria: registrar solo como egreso directo
            await Q(`
              INSERT INTO egresos
                (fecha, proveedor_nombre, categoria, descripcion,
                 total, metodo, referencia, created_by)
              VALUES (CURRENT_DATE,$1,'Pago Proveedor',$2,$3,'Transferencia',$4,$5)`,
              [updated.proveedor_nombre || 'Proveedor', concepto,
               monto, updated.numero_op || null, req.user?.id], schema).catch(() => {});
            console.log(`📋 OC ${updated.numero_op} pagada → egreso directo registrado (sin cuenta bancaria)`);
          }
        }
      } catch(tesoErr) {
        console.warn('Auto-tesorería OC:', tesoErr.message);
      }
    }
    // ──────────────────────────────────────────────────────────────────────────

    res.json(updated);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

// ── Pago explícito de OC con selección de cuenta bancaria ────────────────────
app.post('/api/ordenes-proveedor/:id/pagar', auth, empresaActiva, licencia, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query('BEGIN');

    const { cuenta_id, monto_pagado, metodo, referencia, notas, fecha } = req.body;

    // Ensure ordenes_proveedor has updated_at column (migration safety)
    await client.query(`ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`).catch(()=>{});
    await client.query(`ALTER TABLE ordenes_proveedor ADD COLUMN IF NOT EXISTS proveedor_nombre TEXT`).catch(()=>{});

    // Get OC details
    const ocR = await client.query(
      `SELECT op.*, p.nombre proveedor_nombre
       FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id
       WHERE op.id=$1`, [req.params.id]);
    if (!ocR.rows.length) return res.status(404).json({ error: 'OC no encontrada' });
    const oc = ocR.rows[0];

    const monto = parseFloat(monto_pagado || oc.total || 0);
    if (!monto || monto <= 0) return res.status(400).json({ error: 'Monto inválido' });

    // Mark OC as paid
    await client.query(
      `UPDATE ordenes_proveedor SET estatus='pagada' WHERE id=$1`,
      [req.params.id]);

    // Ensure tesoreria tables exist
    await client.query(`CREATE TABLE IF NOT EXISTS cuentas_bancarias(
      id SERIAL PRIMARY KEY, nombre TEXT NOT NULL, banco VARCHAR(100),
      tipo VARCHAR(30) DEFAULT 'cheques', numero_cuenta VARCHAR(50), clabe VARCHAR(20),
      moneda VARCHAR(5) DEFAULT 'MXN', saldo_actual NUMERIC(15,2) DEFAULT 0,
      saldo_minimo NUMERIC(15,2) DEFAULT 0, titular TEXT,
      activo BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())`).catch(()=>{});
    await client.query(`CREATE TABLE IF NOT EXISTS movimientos_bancarios(
      id SERIAL PRIMARY KEY, cuenta_id INTEGER NOT NULL, cuenta_destino_id INTEGER,
      tipo_operacion VARCHAR(20) NOT NULL, fecha DATE NOT NULL DEFAULT CURRENT_DATE,
      concepto TEXT NOT NULL, monto NUMERIC(15,2) NOT NULL, moneda VARCHAR(5) DEFAULT 'MXN',
      categoria VARCHAR(100), beneficiario TEXT, referencia VARCHAR(100),
      saldo_posterior NUMERIC(15,2), conciliado BOOLEAN DEFAULT false,
      orden_compra_id INTEGER, notas TEXT,
      created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`).catch(()=>{});
    await client.query(`ALTER TABLE movimientos_bancarios ADD COLUMN IF NOT EXISTS orden_compra_id INTEGER`).catch(()=>{});

    let nuevoSaldo = null;
    let cuentaUsada = null;

    if (cuenta_id) {
      // Use specified account
      const ctaR = await client.query(
        'SELECT * FROM cuentas_bancarias WHERE id=$1', [cuenta_id]);
      if (!ctaR.rows.length) return res.status(404).json({ error: 'Cuenta bancaria no encontrada' });
      const cta = ctaR.rows[0];

      if (parseFloat(cta.saldo_actual) < monto) {
        return res.status(400).json({
          error: `Saldo insuficiente. Disponible: $${parseFloat(cta.saldo_actual).toLocaleString('es-MX',{minimumFractionDigits:2})}`,
          saldo_disponible: parseFloat(cta.saldo_actual)
        });
      }

      nuevoSaldo = parseFloat(cta.saldo_actual) - monto;
      cuentaUsada = cta;

      // Update account balance
      await client.query(
        'UPDATE cuentas_bancarias SET saldo_actual=$1 WHERE id=$2',
        [nuevoSaldo, cuenta_id]);
    }

    const concepto = `Pago OC ${oc.numero_op||'#'+req.params.id} — ${oc.proveedor_nombre||'Proveedor'}`;
    const fechaPago = fecha || new Date().toISOString().slice(0,10);

    // Register bank movement
    if (cuenta_id) {
      await client.query(`
        INSERT INTO movimientos_bancarios
          (cuenta_id, tipo_operacion, fecha, concepto, monto, moneda,
           categoria, beneficiario, saldo_posterior, orden_compra_id, referencia, notas, created_by)
        VALUES ($1,'egreso',$2,$3,$4,$5,'Pago Proveedor',$6,$7,$8,$9,$10,$11)`,
        [cuenta_id, fechaPago, concepto, monto, oc.moneda || 'MXN',
         oc.proveedor_nombre || null, nuevoSaldo, req.params.id,
         referencia || null, notas || null, req.user?.id]);
    }

    // Always register in egresos for financial tracking
    await client.query(`
      INSERT INTO egresos
        (fecha, proveedor_nombre, categoria, descripcion,
         subtotal, iva, total, metodo, referencia, numero_factura, notas, created_by)
      VALUES ($1,$2,'Pago Proveedor',$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [fechaPago, oc.proveedor_nombre || 'Proveedor', concepto,
       parseFloat(oc.subtotal || oc.total || 0),
       parseFloat(oc.iva || 0), monto,
       metodo || 'Transferencia', referencia || null,
       oc.numero_op || null, notas || null, req.user?.id]).catch(() => {});

    // Register in contabilidad if cat_cuentas exist
    await client.query(`
      INSERT INTO polizas (fecha, tipo_pol, concepto, created_by)
      VALUES ($1,'E',$2,$3)`, [fechaPago, concepto, req.user?.id])
      .then(async (r) => {
        const polId = r.rows?.[0]?.id;
        if (!polId) return;
        await client.query(
          `INSERT INTO polizas_detalle (poliza_id,num_cta,concepto,debe,haber) VALUES ($1,'201',$2,$3,0)`,
          [polId, concepto, monto]);
        await client.query(
          `INSERT INTO polizas_detalle (poliza_id,num_cta,concepto,debe,haber) VALUES ($1,'102',$2,0,$3)`,
          [polId, concepto, monto]);
      }).catch(() => {});

    await client.query('COMMIT');

    res.json({
      ok: true,
      mensaje: cuenta_id
        ? `OC pagada. ${monto.toLocaleString('es-MX',{minimumFractionDigits:2})} ${oc.moneda||'MXN'} descontados de ${cuentaUsada?.nombre}.`
        : `OC marcada como pagada. Egreso registrado en contabilidad.`,
      saldo_nuevo: nuevoSaldo,
      cuenta: cuentaUsada?.nombre || null,
      monto_pagado: monto,
    });
  } catch(e) {
    await client.query('ROLLBACK');
    console.error('OC pagar:', e.message);
    res.status(500).json({ error: e.message });
  } finally { client.release(); }
});

app.delete('/api/ordenes-proveedor/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
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

app.get('/api/ordenes-proveedor/:id/pdf', auth, licencia, async (req,res)=>{
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

app.post('/api/ordenes-proveedor/:id/email', auth, empresaActiva, licencia, async (req,res)=>{
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
app.get('/api/facturas', auth, licencia, async (req,res)=>{
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

app.post('/api/facturas', auth, empresaActiva, licencia, async (req,res)=>{
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

app.put('/api/facturas/:id', auth, empresaActiva, licencia, async (req,res)=>{
  try {
    const b=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(col,val)=>{if(val!==undefined&&has('facturas',col)){sets.push(`${col}=$${i++}`);vals.push(val);}};
    // Estatus (both columns for compat)
    if(b.estatus!==undefined){
      if(has('facturas','estatus')){sets.push(`estatus=$${i++}`);vals.push(b.estatus);}
      if(has('facturas','estatus_pago')){sets.push(`estatus_pago=$${i++}`);vals.push(b.estatus);}
    }
    add('notas',           b.notas!==undefined ? b.notas : undefined);
    add('numero_factura',  b.numero_factura||undefined);
    add('cliente_id',      b.cliente_id!==undefined ? (parseInt(b.cliente_id)||null) : undefined);
    add('moneda',          b.moneda||undefined);
    add('subtotal',        b.subtotal!==undefined ? parseFloat(b.subtotal)||0 : undefined);
    add('iva',             b.iva!==undefined ? parseFloat(b.iva)||0 : undefined);
    add('retencion_isr',   b.retencion_isr!==undefined ? parseFloat(b.retencion_isr)||0 : undefined);
    add('retencion_iva',   b.retencion_iva!==undefined ? parseFloat(b.retencion_iva)||0 : undefined);
    add('total',           b.total!==undefined ? parseFloat(b.total)||0 : undefined);
    add('monto',           b.total!==undefined ? parseFloat(b.total)||0 : undefined);
    add('fecha_emision',   b.fecha_emision||undefined);
    add('fecha_vencimiento', b.fecha_vencimiento!==undefined ? b.fecha_vencimiento||null : undefined);
    add('metodo_pago',     b.metodo_pago!==undefined ? b.metodo_pago||null : undefined);
    add('forma_pago',      b.forma_pago!==undefined ? b.forma_pago||null : undefined);
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
    await client.query(`SET search_path TO "${schema}", public`);
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

app.delete('/api/facturas/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
  try {
    await QR(req,'DELETE FROM pagos WHERE factura_id=$1',[req.params.id]);
    await QR(req,'DELETE FROM facturas WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/facturas/:id/pdf', auth, licencia, async (req,res)=>{
  try {
    const [f]=await QR(req,`
      SELECT f.*,
             cl.nombre          cliente_nombre,
             cl.rfc             cliente_rfc,
             cl.email           cliente_email,
             cl.telefono        cliente_tel,
             cl.regimen_fiscal  cliente_regimen,
             cl.cp              cliente_cp,
             cl.uso_cfdi        cliente_uso_cfdi,
             cl.tipo_persona    cliente_tipo,
             cl.direccion       cliente_direccion,
             cl.ciudad          cliente_ciudad
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
app.get('/api/inventario', auth, licencia, async (req,res)=>{
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

app.post('/api/inventario', auth, soloLecturaInventario, empresaActiva, licencia, async (req,res)=>{
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

app.put('/api/inventario/:id', auth, soloLecturaInventario, empresaActiva, licencia, async (req,res)=>{
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

app.post('/api/inventario/:id/movimiento', auth, soloLecturaInventario, async (req,res)=>{
  const client=await pool.connect();
  try {
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
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

app.delete('/api/inventario/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
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

// ================================================================
// REPORTES DE SERVICIO
// ================================================================
app.get('/api/reportes-servicio', auth, licencia, async (req,res)=>{
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

app.post('/api/reportes-servicio', auth, empresaActiva, licencia, async (req,res)=>{
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

app.put('/api/reportes-servicio/:id', auth, empresaActiva, licencia, async (req,res)=>{
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

app.delete('/api/reportes-servicio/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
  try{ await QR(req,'DELETE FROM reportes_servicio WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){ res.status(500).json({error:e.message}); }
});

// ── PDF del Reporte de Servicio ───────────────────────────────────
app.get('/api/reportes-servicio/:id/pdf', auth, licencia, async (req,res)=>{
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

// ── Enviar Reporte de Servicio por email ─────────────────────────
app.post('/api/reportes-servicio/:id/email', auth, empresaActiva, licencia, async (req,res)=>{
  try{
    const {to,cc,asunto,mensaje}=req.body;
    if(!to) return res.status(400).json({error:'to requerido'});
    const rows = await QR(req,`
      SELECT rs.*,cl.nombre cliente_nombre,cl.rfc cliente_rfc,cl.email cliente_email,
        cl.telefono cliente_tel,cl.direccion cliente_dir,cl.contacto cliente_contacto,
        cl.ciudad cliente_ciudad,p.nombre proyecto_nombre
      FROM reportes_servicio rs
      LEFT JOIN clientes cl ON cl.id=rs.cliente_id
      LEFT JOIN proyectos p ON p.id=rs.proyecto_id
      WHERE rs.id=$1`,[req.params.id]);
    if(!rows.length) return res.status(404).json({error:'No encontrado'});
    const r=rows[0]; const emp=await getEmpConfig(req.user?.schema);
    const buf=await buildPDFReporteServicio(r,emp);
    const dynMailer=await getMailer(req.user?.schema);
    const fromEmail=await getFromEmail(req.user?.schema);
    const empCfg=(await Q('SELECT nombre,telefono,email FROM empresa_config LIMIT 1',[],req.user?.schema))[0]||{};
    const nomEmp=empCfg.nombre||VEF_NOMBRE;
    const msgHtml=(mensaje||`Estimado/a ${r.cliente_nombre||'Cliente'},\n\nAdjunto el Reporte de Servicio.\n\nSaludos,`)
      .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\n/g,'<br>');
    await dynMailer.sendMail({
      from:`"${nomEmp}" <${fromEmail}>`,to,cc:cc||undefined,
      subject:asunto||`Reporte de Servicio ${r.numero_reporte} — ${nomEmp}`,
      html:`<div style="font-family:Arial,sans-serif;max-width:600px;margin:auto"><div style="background:#0D2B55;padding:20px 24px"><h2 style="color:#fff;margin:0">${nomEmp}</h2></div><div style="background:#1A4A8A;padding:10px 24px"><span style="color:#A8C5F0;font-size:11px">REPORTE DE SERVICIO</span><div style="color:#fff;font-weight:700">${r.numero_reporte||'—'}</div></div><div style="padding:24px">${msgHtml}</div></div>`,
      attachments:[{filename:`RS-${r.numero_reporte||r.id}.pdf`,content:buf}]
    });
    res.json({ok:true,msg:`Correo enviado a ${to}`});
  }catch(e){console.error('RS email:',e.message);res.status(500).json({error:e.message});}
});

// ================================================================
// LOGO
// ================================================================
app.get('/api/logo/status', auth, async (req,res)=>{
  try {
    // Verificar logo en empresa_config del schema del usuario
    const rows = await QR(req,'SELECT logo_data IS NOT NULL AS has_logo, logo_mime FROM empresa_config LIMIT 1');
    const hasLogo = rows[0]?.has_logo || false;
    // Fallback al archivo en disco
    const lp = getLogoPath();
    res.json({ found: hasLogo || !!lp, source: hasLogo ? 'db' : (lp ? 'file' : 'none') });
  } catch(e) {
    const lp=getLogoPath();
    res.json({found:!!lp, source: lp?'file':'none'});
  }
});

// ── Servir logo por schema (dinámico por empresa) ────────────────
app.get('/api/logo', async (req,res)=>{
  try {
    // Token puede venir como query param o header
    const t = req.headers.authorization?.split(' ')[1] || req.query.token;
    let schema = global._defaultSchema || 'emp_vef';
    if (t) {
      try {
        const jwt = require('jsonwebtoken');
        const dec = jwt.verify(t, process.env.JWT_SECRET || 'vef_secret_2025');
        schema = dec.schema || dec.schema_name || schema;
      } catch {}
    }
    const rows = await Q('SELECT logo_data, logo_mime FROM empresa_config WHERE logo_data IS NOT NULL LIMIT 1', [], schema);
    if (rows.length && rows[0].logo_data) {
      const mime = rows[0].logo_mime || 'image/png';
      res.setHeader('Content-Type', mime);
      res.setHeader('Cache-Control', 'public, max-age=3600');
      return res.send(rows[0].logo_data);
    }
    // Fallback: archivo en disco
    const lp = getLogoPath();
    if (lp) return res.sendFile(lp);
    res.status(404).send('No logo');
  } catch(e) {
    const lp = getLogoPath();
    if (lp) return res.sendFile(lp);
    res.status(404).send('No logo');
  }
});

// ================================================================
// LOGO UPLOAD (base64) — guarda como logo.png en raíz del proyecto
// ================================================================
app.post('/api/logo/upload', auth, adminOnly, async (req,res)=>{
  try {
    const { data, mime, ext } = req.body;
    if (!data) return res.status(400).json({ error: 'data requerido' });
    const mimeType = mime || (ext==='jpg'||ext==='jpeg' ? 'image/jpeg' : 'image/png');
    const buf = Buffer.from(data, 'base64');
    if (buf.length > 3 * 1024 * 1024) return res.status(400).json({ error: 'Archivo muy grande (máx 3MB)' });

    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';

    // Guardar logo en empresa_config del schema de esta empresa
    const colRes = await pool.query(
      `SELECT column_name FROM information_schema.columns
       WHERE table_schema=$1 AND table_name='empresa_config'
       AND column_name IN ('logo_data','logo_mime')`, [schema]);
    const hasCols = colRes.rows.map(r=>r.column_name);

    if (hasCols.includes('logo_data')) {
      // Guardar en BD (por empresa)
      const ex = await Q('SELECT id FROM empresa_config LIMIT 1', [], schema);
      if (ex.length) {
        let upSql = `UPDATE empresa_config SET logo_data=$1`;
        const upVals = [buf];
        if (hasCols.includes('logo_mime')) { upSql += `,logo_mime=$2 WHERE id=$3`; upVals.push(mimeType, ex[0].id); }
        else { upSql += ` WHERE id=$2`; upVals.push(ex[0].id); }
        await Q(upSql, upVals, schema);
      } else {
        await Q(`INSERT INTO empresa_config(nombre,logo_data,logo_mime) VALUES('Mi Empresa',$1,$2)`, [buf, mimeType], schema);
      }
      console.log('🖼  Logo guardado en BD para schema:', schema, buf.length, 'bytes');
    } else {
      // Fallback: guardar en disco (compatibilidad)
      const dest = path.join(__dirname, 'logo.png');
      fs.writeFileSync(dest, buf);
      global._logoPathOverride = dest;
    }

    // También guardar en disco como respaldo (para PDFs)
    try {
      const dest = path.join(__dirname, 'logo_'+schema+'.png');
      fs.writeFileSync(dest, buf);
    } catch {}

    res.json({ ok: true, size: buf.length, schema });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// EGRESOS — CRUD completo
// ================================================================
// Cache de schemas donde ya se corrieron las migraciones de egresos
const _egMigDone = new Set();
async function ensureEgresosCols(schema) {
  if (_egMigDone.has(schema)) return;
  const sqls = [
    'ALTER TABLE egresos ADD COLUMN IF NOT EXISTS proveedor_id INTEGER',
    'ALTER TABLE egresos ADD COLUMN IF NOT EXISTS factura_pdf BYTEA',
    'ALTER TABLE egresos ADD COLUMN IF NOT EXISTS factura_nombre TEXT',
    'ALTER TABLE egresos ADD COLUMN IF NOT EXISTS created_by INTEGER',
  ];
  for (const sql of sqls) {
    try {
      const client = await pool.connect();
      try {
        await client.query(`SET search_path TO "${schema}", public`);
        await client.query(sql);
      } finally { client.release(); }
    } catch(e) { /* columna ya existe */ }
  }
  _egMigDone.add(schema);
  console.log('✅ egresos columnas verificadas para schema:', schema);
}

app.get('/api/egresos', auth, licencia, async (req,res)=>{
  try {
    const schema = req.user?.schema || req.user?.schema_name || global._defaultSchema;
    await ensureEgresosCols(schema);

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
  }catch(e){ 
    console.error('GET egresos:', e.message);
    res.status(500).json({error:e.message}); 
  }
});

app.get('/api/egresos/categorias', auth, async (req,res)=>{
  const rows = await QR(req,"SELECT DISTINCT categoria FROM egresos WHERE categoria IS NOT NULL ORDER BY categoria");
  res.json(rows.map(r=>r.categoria));
});

app.post('/api/egresos', auth, empresaActiva, licencia, async (req,res)=>{
  try {
    const {fecha,proveedor_id,proveedor_nombre,categoria,descripcion,
           subtotal,iva,total,metodo,referencia,numero_factura,notas} = req.body;
    if(!fecha) return res.status(400).json({error:'Fecha requerida'});

    const schema = req.user?.schema || req.user?.schema_name || global._defaultSchema;
    await ensureEgresosCols(schema);

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

app.put('/api/egresos/:id', auth, empresaActiva, licencia, async (req,res)=>{
  try {
    const b = req.body;
    const {fecha,proveedor_id,proveedor_nombre,categoria,descripcion,
           subtotal,iva,total,metodo,referencia,numero_factura,notas} = b;

    const schema = req.user?.schema || req.user?.schema_name || global._defaultSchema;
    await ensureEgresosCols(schema);

    const rows = await QR(req,`
      UPDATE egresos SET
        fecha=COALESCE($1,fecha),
        proveedor_id=$2,proveedor_nombre=$3,
        categoria=COALESCE($4,categoria),
        descripcion=COALESCE($5,descripcion),
        subtotal=COALESCE($6,subtotal),iva=COALESCE($7,iva),total=COALESCE($8,total),
        metodo=COALESCE($9,metodo),referencia=$10,
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

app.delete('/api/egresos/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
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
    res.json(r.rows[0]);
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
    const pol = pr.rows[0];
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
app.get('/api/tareas', auth, licencia, async (req,res)=>{
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

app.post('/api/tareas', auth, empresaActiva, licencia, async (req,res)=>{
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

app.put('/api/tareas/:id', auth, empresaActiva, licencia, async (req,res)=>{
  try {
    const {titulo,descripcion,proyecto_id,asignado_a,prioridad,estatus,
           fecha_inicio,fecha_vencimiento,notas} = req.body;
    const fechaComp = estatus==='completada' ? 'NOW()' : 'NULL';
    const rows = await QR(req,`
      UPDATE tareas SET
        titulo=COALESCE($1,titulo),descripcion=$2,proyecto_id=$3,asignado_a=$4,
        prioridad=COALESCE($5,prioridad),estatus=COALESCE($6,estatus),
        fecha_inicio=$7,fecha_vencimiento=$8,notas=$9,
        fecha_completada=${fechaComp},updated_at=NOW()
      WHERE id=$10 RETURNING *`,
      [titulo||null,descripcion||null,proyecto_id||null,asignado_a||null,
       prioridad||null,estatus||null,
       fecha_inicio||null,fecha_vencimiento||null,notas||null,req.params.id]);
    res.json(rows[0]||{});
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.delete('/api/tareas/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
  try { await QR(req,'DELETE FROM tareas WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){ res.status(500).json({error:e.message}); }
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
    const doc = new PDFKit({
      margin:28, size:'A4', bufferPages:true,
      info:{Title:'Reporte de Servicio '+(r.numero_reporte||''), Author:emp.nombre||VEF_NOMBRE}
    });
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>resolve(Buffer.concat(ch))); doc.on('error',reject);

    const M=28, W=539, PH=842;
    // Pie siempre a Y fija — deja 52px al fondo para el pie
    const PIE_Y   = PH - 52;   // donde empieza el pie
    const SAFE_Y  = PIE_Y - 6; // límite máximo de contenido
    const AZUL=C.AZUL, AZUL_MED=C.AZUL_MED, GRIS=C.GRIS, GRIS_B=C.GRIS_B, TEXTO=C.TEXTO;
    const empNom = emp.nombre||emp.razon_social||VEF_NOMBRE;
    const bgs = [GRIS, C.BLANCO];

    // ── Mini-header compacto para páginas 2+ ────────────────────
    function miniHeader() {
      const _logoBuf = emp._logo_data||null;
      const _lp = !_logoBuf ? getLogoPath() : null;
      doc.rect(M,14,W,22).fill(AZUL);
      if(_logoBuf||_lp){
        doc.rect(M,14,58,22).fill('#fff');
        try{
          if(_logoBuf) doc.image(_logoBuf,M+2,15,{fit:[54,20],align:'center',valign:'center'});
          else         doc.image(_lp,     M+2,15,{fit:[54,20],align:'center',valign:'center'});
        }catch(e){}
        doc.fillColor('#fff').fontSize(7).font('Helvetica-Bold')
           .text(empNom+'  |  Reporte '+(r.numero_reporte||''), M+62, 21, {width:W-68, lineBreak:false});
      } else {
        doc.fillColor('#fff').fontSize(7).font('Helvetica-Bold')
           .text(empNom+'  |  Reporte '+(r.numero_reporte||''), M+6, 21, {width:W-12, lineBreak:false});
      }
      doc.y = 44;
    }

    // ── Salto de página si no cabe 'needed' px ──────────────────
    function checkSpace(needed) {
      if (doc.y + needed > SAFE_Y) {
        doc.addPage();
        miniHeader();
      }
    }

    // ── Fila dos columnas con altura dinámica ───────────────────
    function row2(lbl1,val1,lbl2,val2,bg) {
      const LW=108, VW=(W/2)-LW-6, C2=M+W/2;
      const v1=String(val1||'—'), v2=String(val2||'');
      const est = (txt,w) => Math.max(1,Math.ceil(txt.length/(w/5.2)));
      const h = Math.max(18, Math.max(est(v1,VW), lbl2?est(v2,VW):0)*11+8);
      checkSpace(h);
      const y=doc.y;
      doc.rect(M,y,W,h).fill(bg||GRIS);
      doc.rect(M,y,W,h).lineWidth(0.3).strokeColor(GRIS_B).stroke();
      doc.moveTo(C2,y).lineTo(C2,y+h).lineWidth(0.3).strokeColor(GRIS_B).stroke();
      doc.fillColor(AZUL).fontSize(8).font('Helvetica-Bold')
         .text(lbl1, M+5, y+5, {width:LW-4, lineBreak:false});
      doc.fillColor(TEXTO).fontSize(8.5).font('Helvetica')
         .text(v1, M+LW+2, y+4, {width:VW, lineGap:1});
      if(lbl2){
        doc.fillColor(AZUL).fontSize(8).font('Helvetica-Bold')
           .text(lbl2, C2+5, y+5, {width:LW-4, lineBreak:false});
        doc.fillColor(TEXTO).fontSize(8.5).font('Helvetica')
           .text(String(val2||'—'), C2+LW+2, y+4, {width:VW, lineGap:1});
      }
      doc.y = y+h;
    }

    // ── Barra de título de sección ──────────────────────────────
    function secBar(titulo) {
      checkSpace(20);
      const y=doc.y;
      doc.rect(M,y,W,17).fill(AZUL_MED);
      doc.fillColor('#fff').fontSize(8.5).font('Helvetica-Bold')
         .text(titulo.toUpperCase(), M+7, y+4, {width:W-14, lineBreak:false});
      doc.y = y+17;
    }

    // ── Sección de texto (flujo continuo) ───────────────────────
    function secTexto(titulo, contenido) {
      if(!contenido||!String(contenido).trim()) return;
      checkSpace(30);
      secBar(titulo);
      doc.fillColor(TEXTO).fontSize(9.5).font('Helvetica')
         .text(String(contenido).trim(), M+4, doc.y+3, {
           width:W-8, lineGap:2, paragraphGap:0, align:'justify'
         });
      doc.y += 5;
    }

    // ═══════════════════════════════════════════════════════════
    // PÁGINA 1 — PORTADA
    // Header idéntico a cotizaciones (logo BD o disco, RFC, dirección)
    // ═══════════════════════════════════════════════════════════
    pdfWatermark(doc, emp);
    pdfHeader(doc, 'REPORTE DE SERVICIO', [
      'No. '+(r.numero_reporte||'—')+'  |  Fecha: '+(fmt(r.fecha_reporte)||'—')+'  |  Técnico: '+(r.tecnico||'—'),
      'Proyecto: '+(r.proyecto_nombre||'—')+'  |  Estatus: '+((r.estatus||'borrador').toUpperCase()),
    ], emp);

    doc.moveDown(0.3);
    doc.fillColor(AZUL).fontSize(13).font('Helvetica-Bold')
       .text(r.titulo||'Sin título', M, doc.y, {width:W, align:'center', lineGap:2});
    doc.moveDown(0.4);

    // ── DATOS DEL REPORTE ──────────────────────────────────────
    secBar('Datos del Reporte');
    let bi=0;
    row2('No. Reporte:',    r.numero_reporte||'—',              'Fecha Reporte:', fmt(r.fecha_reporte)||'—', bgs[bi++%2]);
    row2('Fecha Servicio:', fmt(r.fecha_servicio)||'—',         'Técnico:',       r.tecnico||'—',            bgs[bi++%2]);
    row2('Estatus:',        (r.estatus||'borrador').toUpperCase(), 'Proyecto:',   r.proyecto_nombre||'—',    bgs[bi++%2]);
    doc.y += 5;

    // ── DATOS DEL CLIENTE ──────────────────────────────────────
    if(r.cliente_nombre){
      secBar('Datos del Cliente');
      let ci=0;
      row2('Cliente:',  r.cliente_nombre||'—',   'RFC:',    r.cliente_rfc||'—',    bgs[ci++%2]);
      row2('Contacto:', r.cliente_contacto||'—', 'Ciudad:', r.cliente_ciudad||'—', bgs[ci++%2]);
      row2('Teléfono:', r.cliente_tel||'—',       'Email:',  r.cliente_email||'—', bgs[ci++%2]);
      if(r.cliente_dir) row2('Dirección:', r.cliente_dir||'—', '', '', bgs[ci++%2]);
      doc.y += 5;
    }

    // ── ÍNDICE DE CONTENIDO ────────────────────────────────────
    const todasSecciones = [
      {titulo:'Introducción',             campo:'introduccion'},
      {titulo:'Objetivo',                  campo:'objetivo'},
      {titulo:'Alcance',                   campo:'alcance'},
      {titulo:'Descripción del Sistema',   campo:'descripcion_sistema'},
      {titulo:'Arquitectura del Sistema',  campo:'arquitectura'},
      {titulo:'Desarrollo Técnico',        campo:'desarrollo_tecnico'},
      {titulo:'Resultados de Pruebas',     campo:'resultados_pruebas'},
      {titulo:'Problemas Detectados',      campo:'problemas_detectados'},
      {titulo:'Soluciones Implementadas',  campo:'soluciones_implementadas'},
      {titulo:'Conclusiones',              campo:'conclusiones'},
      {titulo:'Recomendaciones',           campo:'recomendaciones'},
      {titulo:'Anexos',                    campo:'anexos'},
    ].filter(sc => r[sc.campo] && String(r[sc.campo]).trim());

    if(todasSecciones.length){
      // El índice completo debe caber junto — si no hay espacio, nueva página
      checkSpace(20 + todasSecciones.length * 17);
      secBar('Índice de Contenido');
      todasSecciones.forEach((sc, idx) => {
        const y = doc.y;
        const bg = idx%2===0 ? GRIS : C.BLANCO;
        doc.rect(M, y, W, 16).fill(bg);
        doc.rect(M, y, W, 16).lineWidth(0.3).strokeColor(GRIS_B).stroke();
        doc.rect(M, y, 24, 16).fill(AZUL_MED);
        doc.fillColor('#fff').fontSize(8).font('Helvetica-Bold')
           .text(String(idx+1), M, y+4, {width:24, align:'center', lineBreak:false});
        doc.fillColor(TEXTO).fontSize(9).font('Helvetica')
           .text(sc.titulo, M+30, y+4, {width:W-60, lineBreak:false});
        doc.fillColor('#94a3b8').fontSize(8).font('Helvetica')
           .text('· · · · ·', M+W-44, y+4, {width:44, align:'right', lineBreak:false});
        doc.y = y + 16;
      });
      doc.y += 8;
    }

    // ── SECCIONES TÉCNICAS ─────────────────────────────────────
    secTexto('Introducción',             r.introduccion);
    secTexto('Objetivo',                 r.objetivo);
    secTexto('Alcance',                  r.alcance);
    secTexto('Descripción del Sistema',  r.descripcion_sistema);
    secTexto('Arquitectura del Sistema', r.arquitectura);
    secTexto('Desarrollo Técnico',       r.desarrollo_tecnico);
    secTexto('Resultados de Pruebas',    r.resultados_pruebas);
    secTexto('Problemas Detectados',     r.problemas_detectados);
    secTexto('Soluciones Implementadas', r.soluciones_implementadas);
    secTexto('Conclusiones',             r.conclusiones);
    secTexto('Recomendaciones',          r.recomendaciones);
    secTexto('Anexos',                   r.anexos);

    // ── FIRMAS ─────────────────────────────────────────────────
    checkSpace(100);
    doc.y += 8;
    secBar('Conformidad y Firmas');
    doc.y += 14;

    const fW  = (W - 30) / 2;
    const fY  = doc.y;
    const fX1 = M;
    const fX2 = M + fW + 30;

    // Técnico
    doc.rect(fX1, fY, fW, 62).fill(GRIS).stroke(GRIS_B);
    doc.moveTo(fX1+16, fY+42).lineTo(fX1+fW-16, fY+42).lineWidth(1).strokeColor('#94a3b8').stroke();
    doc.fillColor(AZUL).fontSize(8).font('Helvetica-Bold')
       .text('TÉCNICO RESPONSABLE', fX1, fY+8, {width:fW, align:'center', lineBreak:false});
    doc.fillColor(TEXTO).fontSize(8.5).font('Helvetica')
       .text(r.tecnico||'________________________________', fX1, fY+45, {width:fW, align:'center', lineBreak:false});
    doc.fillColor('#64748b').fontSize(7).font('Helvetica')
       .text('Fecha: _____ / _____ / _______', fX1, fY+54, {width:fW, align:'center', lineBreak:false});

    // Cliente
    doc.rect(fX2, fY, fW, 62).fill(GRIS).stroke(GRIS_B);
    doc.moveTo(fX2+16, fY+42).lineTo(fX2+fW-16, fY+42).lineWidth(1).strokeColor('#94a3b8').stroke();
    doc.fillColor(AZUL).fontSize(8).font('Helvetica-Bold')
       .text('CLIENTE / RECEPTOR', fX2, fY+8, {width:fW, align:'center', lineBreak:false});
    doc.fillColor(TEXTO).fontSize(8.5).font('Helvetica')
       .text(r.cliente_contacto||r.cliente_nombre||'________________________________', fX2, fY+45, {width:fW, align:'center', lineBreak:false});
    doc.fillColor('#64748b').fontSize(7).font('Helvetica')
       .text('Fecha: _____ / _____ / _______', fX2, fY+54, {width:fW, align:'center', lineBreak:false});

    doc.y = fY + 70;
    doc.fillColor('#64748b').fontSize(7.5).font('Helvetica')
       .text('La firma de este documento confirma que los trabajos descritos fueron realizados a conformidad del cliente.',
         M, doc.y, {width:W, align:'center', lineBreak:false});

    // ── PIE FIJO AL FONDO DE CADA PÁGINA ──────────────────────
    // Se dibuja en Y absoluta (PIE_Y) para que nunca interfiera con el contenido
    const pages = doc.bufferedPageRange();
    const nom  = emp.nombre||VEF_NOMBRE;
    const tel  = emp.telefono||VEF_TELEFONO;
    const mail = emp.email||VEF_CORREO;
    const rfc  = emp.rfc ? '  |  RFC: '+emp.rfc : '';
    for(let i = pages.start; i < pages.start + pages.count; i++){
      doc.switchToPage(i);
      // Línea separadora
      doc.moveTo(M, PIE_Y).lineTo(M+W, PIE_Y).lineWidth(0.8).strokeColor(AZUL_MED).stroke();
      // Caja azul del pie
      doc.rect(M, PIE_Y+1, W, 34).fill(AZUL);
      // Nombre + RFC
      doc.fillColor(C.BLANCO).fontSize(8).font('Helvetica-Bold')
         .text(nom+rfc, M, PIE_Y+7, {width:W, align:'center', lineBreak:false});
      // Tel + email
      doc.fillColor('#A8C5F0').fontSize(7.5).font('Helvetica')
         .text('Tel: '+tel+'   |   '+mail, M, PIE_Y+18, {width:W, align:'center', lineBreak:false});
      // Número de página
      doc.fillColor('#A8C5F0').fontSize(7).font('Helvetica')
         .text('Pág. '+(i - pages.start + 1)+' / '+pages.count, M, PIE_Y+28, {width:W, align:'center', lineBreak:false});
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

// ── Rutas alternativas del logo — /imagen/VEF.png, /images/, /img/ ──
// Cualquier variante de ruta para el logo sirve el mismo archivo
['VEF.png','VEF.jpg','logo.png','logo.jpg','vef.png','vef.jpg'].forEach(name=>{
  ['/imagen/','/images/','/img/','/assets/'].forEach(prefix=>{
    app.get(prefix+name, (req,res)=>{
      // 1. Intentar desde carpeta imagen/ en raíz del proyecto
      const imgDir = path.join(__dirname,'imagen');
      const fromDir = path.join(imgDir, req.path.split('/').pop());
      if(require('fs').existsSync(fromDir)) return res.sendFile(fromDir);
      // 2. Fallback: usar el logo detectado automáticamente
      const lp = getLogoPath();
      if(lp) return res.sendFile(lp);
      // 3. Sin logo — devolver 404 limpio (no activa el fallback ⚡ del HTML)
      res.status(404).json({error:'Logo no encontrado. Coloca tu imagen en /imagen/VEF.png o en la raíz como logo.png'});
    });
  });
});

// Servir carpeta imagen/ como estática (para /imagen/VEF.png directo)
const _imgFolder = path.join(__dirname,'imagen');
if(require('fs').existsSync(_imgFolder)){
  app.use('/imagen', require('express').static(_imgFolder));
}

// Servir pag.html desde raíz del proyecto
app.get('/presentacion', (req,res)=>{
  const p = path.join(__dirname,'pag.html');
  if(require('fs').existsSync(p)) return res.sendFile(p);
  res.status(404).send('Presentación no encontrada');
});

// ================================================================
// LICENCIA — estado público de la suscripción del usuario actual
// ================================================================
app.get('/api/licencia', auth, async (req,res)=>{
  try {
    const empId = req.user?.empresa_id;
    if (!empId) return res.json({ ok:true, estatus:'sin_empresa' });
    const { rows } = await pool.query(
      'SELECT nombre,activa,trial_hasta,suscripcion_estatus,suscripcion_hasta FROM public.empresas WHERE id=$1',
      [empId]);
    if (!rows.length) return res.json({ ok:true, estatus:'empresa_no_encontrada' });
    const e = rows[0];
    const hoy = new Date();
    let ok = false, estatus = 'inactiva', dias_restantes = 0;
    if (e.activa === false) {
      estatus = 'inactiva';
    } else if (e.suscripcion_estatus === 'activa' && e.suscripcion_hasta && new Date(e.suscripcion_hasta) >= hoy) {
      ok = true; estatus = 'activa';
      dias_restantes = Math.ceil((new Date(e.suscripcion_hasta) - hoy) / 86400000);
    } else if (e.suscripcion_estatus === 'trial' && e.trial_hasta && new Date(e.trial_hasta) >= hoy) {
      ok = true; estatus = 'trial';
      dias_restantes = Math.ceil((new Date(e.trial_hasta) - hoy) / 86400000);
    } else if (e.suscripcion_estatus === 'trial') {
      estatus = 'trial_vencido';
    }
    res.json({ ok, estatus, dias_restantes, empresa_nombre:e.nombre,
      trial_hasta: e.trial_hasta, suscripcion_hasta: e.suscripcion_hasta });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ================================================================
// SAT CFDI — Timbrado, Cancelación, Validación, Vinculación
// Proxy al microservicio Python (sat_api.py puerto 5050)
// ================================================================

// Estado del PAC configurado
app.get('/api/sat/pac-info', auth, async (req, res) => {
  try {
    const r = await satProxy('/pac-info', {});
    res.json(r);
  } catch(e) { res.json({ok:false, pac:'No configurado', configurado:false}); }
});

// Solicitar descarga de CFDIs EMITIDOS
app.post('/api/sat/solicitar-emitidos', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const r = await satProxy('/solicitar-emitidos', req.body);
    res.json(r);
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});


// Descargar XML de un CFDI específico por UUID (proceso automático)
app.post('/api/sat/descargar-uuid', auth, empresaActiva, licencia, async (req, res) => {
  try {
    // Este endpoint puede tardar hasta 3 minutos — timeout extendido
    res.setTimeout(200000);
    const r = await satProxy('/descargar-uuid', req.body, 190000);
    res.json(r);
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});

// Validar CFDI por UUID — consulta pública SAT sin FIEL
app.post('/api/sat/validar-cfdi', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const r = await satProxy('/validar-cfdi', req.body);
    res.json(r);
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});

// Generar / Timbrar CFDI 4.0
// Si no hay PAC configurado: genera el XML firmado para revisión (sin timbre fiscal)
// Si hay PAC: timbra y devuelve el XML con UUID
app.post('/api/sat/timbrar', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    const { factura_id, cer, key, password, ambiente } = req.body;

    if (!factura_id) return res.status(400).json({error:'factura_id requerido'});
    if (!cer || !key) return res.status(400).json({error:'Se requiere el certificado CSD o FIEL (cer y key en base64)'});

    // ── Leer factura completa de la BD ──────────────────────────────
    const fRow = await Q(`
      SELECT f.*,
             c.nombre       cliente_nombre,
             c.rfc          cliente_rfc,
             c.cp           cliente_cp,
             c.regimen_fiscal cliente_regimen,
             c.uso_cfdi     cliente_uso_cfdi
      FROM facturas f
      LEFT JOIN clientes c ON c.id = f.cliente_id
      WHERE f.id = $1
    `, [factura_id], schema);
    if (!fRow.rows?.length) return res.status(404).json({error:'Factura no encontrada'});
    const fac = fRow.rows[0];

    // ── Leer conceptos ───────────────────────────────────────────────
    // Intentar desde items_cotizacion o cotizaciones si existe
    let conceptos = [];
    if (fac.cotizacion_id) {
      // Primero intentar con columnas SAT; si no existen, query básica
      const icRows = await Q(`
        SELECT descripcion, cantidad, precio_unitario,
               COALESCE(clave_prod_serv,'84111506') clave_prod_serv,
               COALESCE(clave_unidad,'E48') clave_unidad,
               COALESCE(objeto_imp,'02') objeto_imp
        FROM items_cotizacion WHERE cotizacion_id = $1
      `, [fac.cotizacion_id], schema).catch(() =>
        Q('SELECT descripcion,cantidad,precio_unitario FROM items_cotizacion WHERE cotizacion_id=$1',
          [fac.cotizacion_id], schema).catch(()=>[])
      );
      conceptos = Array.isArray(icRows) ? icRows : (icRows.rows || []);
      // Asegurar defaults para columnas SAT faltantes
      conceptos = conceptos.map(c => ({
        ...c,
        clave_prod_serv: c.clave_prod_serv || '84111506',
        clave_unidad:    c.clave_unidad    || 'E48',
        objeto_imp:      c.objeto_imp      || '02',
      }));
    }
    // Si no hay conceptos de cotización, usar datos de la factura directa
    if (!conceptos.length) {
      conceptos = [{
        descripcion:      fac.notas || 'Servicios de automatización industrial',
        cantidad:         1,
        precio_unitario:  parseFloat(fac.subtotal || 0),
        clave_prod_serv:  '84111506',
        clave_unidad:     'E48',
        objeto_imp:       '02'
      }];
    }

    // ── Leer configuración de empresa ────────────────────────────────
    const empRow = await Q(`
      SELECT nombre, razon_social, rfc, regimen_fiscal, cp,
             ciudad, direccion, iva_default
      FROM empresa_config LIMIT 1
    `, [], schema);
    const emp = empRow.rows?.[0] || {};

    // ── Construir payload para sat_api.py ────────────────────────────
    const iva_pct = parseFloat(emp.iva_default || 16) / 100;

    const payload = {
      cer, key, password,
      ambiente: ambiente || 'pruebas',
      factura: {
        // Emisor (empresa)
        emisor_nombre:  emp.razon_social || emp.nombre || '',
        regimen_fiscal: emp.regimen_fiscal || '601',
        cp_expedicion:  emp.cp || '',
        // Receptor (cliente)
        rfc_receptor:   fac.cliente_rfc   || 'XAXX010101000',
        nombre_receptor:fac.cliente_nombre|| '',
        cp_receptor:    fac.cliente_cp    || emp.cp || '',
        regimen_receptor:fac.cliente_regimen || '616',
        uso_cfdi:       fac.cliente_uso_cfdi || 'G03',
        // Comprobante
        moneda:         fac.moneda || 'MXN',
        tipo:           'I',
        forma_pago:     '99',  // Por definir
        metodo_pago:    'PPD',
        serie:          fac.numero_factura?.replace(/[0-9]/g,'') || 'A',
        folio:          fac.numero_factura?.replace(/[^0-9]/g,'') || String(fac.id),
        // Conceptos
        conceptos: conceptos.map(c => ({
          descripcion:      c.descripcion || '',
          cantidad:         parseFloat(c.cantidad || 1),
          precio_unitario:  parseFloat(c.precio_unitario || 0),
          clave_prod_serv:  c.clave_prod_serv || '84111506',
          clave_unidad:     c.clave_unidad    || 'E48',
          tasa_iva:         iva_pct,
          objeto_imp:       c.objeto_imp      || '02',
        }))
      }
    };

    const r = await satProxy('/timbrar', payload);

    // ── Guardar resultado en BD ──────────────────────────────────────
    if (r.ok && r.xml) {
      await Q(`
        UPDATE facturas SET
          xml_cfdi    = $1,
          uuid_sat    = $2,
          estatus     = CASE WHEN $2 != '' THEN 'timbrada' ELSE estatus END,
          estatus_pago= CASE WHEN $2 != '' THEN 'pendiente' ELSE estatus_pago END
        WHERE id = $3
      `, [r.xml, r.uuid || '', factura_id], schema)
      .catch(e => console.warn('Guardar XML:', e.message));
    }

    res.json(r);
  } catch(e) {
    console.error('Error /api/sat/timbrar:', e.message);
    res.status(500).json({ok:false, error:e.message});
  }
});

// Generar XML CFDI 4.0 (sin timbrar — para revisión de estructura)
// Toma datos reales de la factura en BD y genera el XML firmado con CSD/FIEL
app.post('/api/sat/generar-xml', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const { factura_id, cer, key, password } = req.body;
    if (!factura_id) return res.status(400).json({ error: 'factura_id requerido' });
    if (!cer || !key)  return res.status(400).json({ error: 'Se requieren cer y key (CSD o FIEL)' });

    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';

    // Cargar factura con datos del cliente
    const [f] = await QR(req, `
      SELECT f.*,
        cl.nombre  cliente_nombre, cl.rfc       cliente_rfc,
        cl.cp      cliente_cp,     cl.regimen_fiscal cliente_regimen,
        cl.uso_cfdi uso_cfdi_default
      FROM facturas f
      LEFT JOIN clientes cl ON cl.id = f.cliente_id
      WHERE f.id = $1
    `, [factura_id]);

    if (!f) return res.status(404).json({ error: 'Factura no encontrada' });

    // Cargar items desde cotizacion si existe
    let items = [];
    if (f.cotizacion_id) {
      items = await QR(req,
        'SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',
        [f.cotizacion_id]
      );
    }

    // Cargar config de empresa (CP de expedición, régimen fiscal emisor)
    const [emp] = await QR(req,
      'SELECT * FROM empresa_config LIMIT 1'
    ).catch(() => [{}]);

    // Construir objeto factura para sat_api.py
    const factura_data = {
      emisor_nombre:    emp?.nombre    || '',
      regimen_fiscal:   emp?.regimen_fiscal || '601',
      cp_expedicion:    emp?.cp        || '',
      rfc_receptor:     f.cliente_rfc  || 'XAXX010101000',
      nombre_receptor:  f.cliente_nombre || '',
      cp_receptor:      f.cliente_cp   || '',
      regimen_receptor: f.cliente_regimen || '616',
      uso_cfdi:         f.uso_cfdi_default || 'G03',
      moneda:           f.moneda === 'USD' ? 'USD' : 'MXN',
      serie:            'A',
      folio:            f.numero_factura || String(f.id),
      forma_pago:       '99',  // Por definir
      metodo_pago:      'PPD', // Pago en parcialidades o diferido
      tipo:             'I',   // Ingreso
      conceptos: items.length ? items.map(i => ({
        descripcion:      i.descripcion || 'Servicio',
        cantidad:         parseFloat(i.cantidad) || 1,
        precio_unitario:  parseFloat(i.precio_unitario) || 0,
        clave_prod_serv:  i.clave_prod_serv || '84111506', // Servicios de ingeniería
        clave_unidad:     i.clave_unidad || 'E48',         // Unidad de servicio
        tasa_iva:         '0.16',
      })) : [{
        descripcion:     f.notas || 'Servicio profesional',
        cantidad:        1,
        precio_unitario: parseFloat(f.subtotal) || parseFloat(f.total) || 0,
        clave_prod_serv: '84111506',
        clave_unidad:    'E48',
        tasa_iva:        '0.16',
      }],
    };

    const payload = { cer, key, password, factura: factura_data };
    const r = await satProxy('/generar-xml', payload);
    res.json(r);
  } catch(e) {
    console.error('generar-xml:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});


// Cancelar CFDI emitido
app.post('/api/sat/cancelar', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const r = await satProxy('/cancelar', req.body);
    if (r.ok && req.body.factura_id) {
      const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
      await Q(`UPDATE facturas SET estatus='cancelada', estatus_pago='cancelada' WHERE id=$1`,
        [req.body.factura_id], schema).catch(()=>{});
    }
    res.json(r);
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});

// Vincular XML de proveedor a Orden de Compra
app.post('/api/sat/vincular-cfdi', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const { xml, orden_id } = req.body;
    if (!xml) return res.status(400).json({error:'xml requerido'});
    const cfdi = await satProxy('/vincular-cfdi', {xml});
    if (!cfdi.ok) return res.status(400).json(cfdi);
    if (orden_id) {
      const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
      const buf = Buffer.from(xml, 'utf-8');
      await Q(
        `UPDATE ordenes_proveedor SET
           factura_pdf=$1, factura_nombre=$2, factura_fecha=NOW(),
           uuid_cfdi_proveedor=$3 WHERE id=$4`,
        [buf, 'CFDI-' + (cfdi.uuid||'proveedor') + '.xml', cfdi.uuid, orden_id], schema
      ).catch(e => console.warn('vincular-cfdi BD:', e.message));
    }
    res.json({ok:true, cfdi});
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});

// Validar lote de CFDIs de proveedores
app.post('/api/sat/validar-lote', auth, empresaActiva, licencia, async (req, res) => {
  const { cfdis } = req.body;
  if (!Array.isArray(cfdis)||!cfdis.length)
    return res.status(400).json({error:'Se requiere array de cfdis'});
  try {
    const resultados = await Promise.allSettled(
      cfdis.map(c => satProxy('/validar-cfdi', c).catch(e=>({ok:false,uuid:c.uuid,error:e.message})))
    );
    res.json({ok:true, resultados: resultados.map(r=>r.status==='fulfilled'?r.value:{ok:false,error:r.reason?.message})});
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});


// ================================================================
// FRONTEND
// ================================================================
// Página de administración exclusiva
app.get('/admin', (req,res)=>{
  res.setHeader('Cache-Control','no-cache,no-store,must-revalidate');
  res.sendFile(path.join(__dirname,'frontend','admin.html'));
});


// ═══════════════════════════════════════════════════════════════
// TESORERÍA — Cuentas bancarias, movimientos, conciliación
// ═══════════════════════════════════════════════════════════════

// Routes to serve new HTML pages
app.get('/tesoreria', (req,res)=>res.sendFile(path.join(__dirname,'frontend','tesoreria.html')));
app.get('/nomina',    (req,res)=>res.sendFile(path.join(__dirname,'frontend','nomina.html')));

// Create tables
async function ensureTesNomTables(schema){
  const Q2=(sql)=>pool.query(`SET search_path TO ${schema},public;${sql}`).catch(()=>{});
  await Q2(`CREATE TABLE IF NOT EXISTS cuentas_bancarias(
    id SERIAL PRIMARY KEY, nombre TEXT NOT NULL, banco VARCHAR(100),
    tipo VARCHAR(30) DEFAULT 'cheques', numero_cuenta VARCHAR(50), clabe VARCHAR(20),
    moneda VARCHAR(5) DEFAULT 'MXN', saldo_actual NUMERIC(15,2) DEFAULT 0,
    saldo_minimo NUMERIC(15,2) DEFAULT 0, saldo_maximo NUMERIC(15,2),
    titular TEXT, rfc_titular VARCHAR(20), notas TEXT, activo BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW())`);
  await Q2(`CREATE TABLE IF NOT EXISTS movimientos_bancarios(
    id SERIAL PRIMARY KEY, cuenta_id INTEGER NOT NULL, cuenta_destino_id INTEGER,
    tipo_operacion VARCHAR(20) NOT NULL, fecha DATE NOT NULL DEFAULT CURRENT_DATE,
    concepto TEXT NOT NULL, monto NUMERIC(15,2) NOT NULL, moneda VARCHAR(5) DEFAULT 'MXN',
    categoria VARCHAR(100), beneficiario TEXT, referencia VARCHAR(100),
    numero_cheque VARCHAR(50), saldo_posterior NUMERIC(15,2),
    conciliado BOOLEAN DEFAULT false, notas TEXT,
    created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`);
  await Q2(`CREATE TABLE IF NOT EXISTS empleados(
    id SERIAL PRIMARY KEY, nombre TEXT NOT NULL, rfc VARCHAR(14), curp VARCHAR(18),
    fecha_nacimiento DATE, telefono VARCHAR(20), email TEXT, direccion TEXT,
    puesto TEXT, departamento VARCHAR(100), tipo_contrato VARCHAR(50),
    fecha_ingreso DATE, fecha_baja DATE, periodicidad_pago VARCHAR(20) DEFAULT 'Quincenal',
    jornada VARCHAR(20) DEFAULT 'completa', salario_diario NUMERIC(10,2) DEFAULT 0,
    salario_bruto NUMERIC(12,2) DEFAULT 0, salario_base_cotizacion NUMERIC(10,2),
    dias_vacaciones INTEGER DEFAULT 6, prima_vacacional INTEGER DEFAULT 25,
    numero_imss VARCHAR(20), numero_infonavit VARCHAR(20),
    banco_clabe VARCHAR(20), banco VARCHAR(100), notas TEXT,
    activo BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())`);
  await Q2(`CREATE TABLE IF NOT EXISTS pagos_nomina(
    id SERIAL PRIMARY KEY, empleado_id INTEGER NOT NULL, fecha DATE NOT NULL,
    monto NUMERIC(12,2) NOT NULL, tipo VARCHAR(50) DEFAULT 'nomina',
    periodo VARCHAR(50), metodo VARCHAR(50) DEFAULT 'Transferencia',
    referencia VARCHAR(100), notas TEXT,
    created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`);
}

// ── Cuentas Bancarias ──────────────────────────────────────────
app.get('/api/tesoreria/cuentas', auth, licencia, async (req,res)=>{
  try{
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await ensureTesNomTables(schema);
    const rows=await QR(req,'SELECT * FROM cuentas_bancarias WHERE activo=true ORDER BY nombre');
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/tesoreria/cuentas', auth, empresaActiva, async (req,res)=>{
  try{
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await ensureTesNomTables(schema);
    const {nombre,banco,tipo,numero_cuenta,clabe,moneda,saldo_actual,saldo_minimo,titular,rfc_titular,notas}=req.body;
    if(!nombre)return res.status(400).json({error:'Nombre requerido'});
    const rows=await QR(req,`INSERT INTO cuentas_bancarias(nombre,banco,tipo,numero_cuenta,clabe,moneda,saldo_actual,saldo_minimo,titular,rfc_titular,notas)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [nombre,banco||null,tipo||'cheques',numero_cuenta||null,clabe||null,moneda||'MXN',parseFloat(saldo_actual)||0,parseFloat(saldo_minimo)||0,titular||null,rfc_titular||null,notas||null]);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/tesoreria/cuentas/:id', auth, empresaActiva, async (req,res)=>{
  try{
    const {nombre,banco,tipo,numero_cuenta,clabe,moneda,saldo_actual,saldo_minimo,titular,rfc_titular,notas}=req.body;
    const rows=await QR(req,`UPDATE cuentas_bancarias SET nombre=$1,banco=$2,tipo=$3,numero_cuenta=$4,clabe=$5,moneda=$6,saldo_actual=$7,saldo_minimo=$8,titular=$9,rfc_titular=$10,notas=$11 WHERE id=$12 RETURNING *`,
      [nombre,banco,tipo,numero_cuenta||null,clabe||null,moneda||'MXN',parseFloat(saldo_actual)||0,parseFloat(saldo_minimo)||0,titular||null,rfc_titular||null,notas||null,req.params.id]);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/tesoreria/cuentas/:id/ajuste', auth, empresaActiva, async (req,res)=>{
  try{
    const {saldo_nuevo,notas}=req.body;
    const [c]=await QR(req,'SELECT saldo_actual,moneda FROM cuentas_bancarias WHERE id=$1',[req.params.id]);
    if(!c)return res.status(404).json({error:'Cuenta no encontrada'});
    const diff=parseFloat(saldo_nuevo)-parseFloat(c.saldo_actual||0);
    await QR(req,'UPDATE cuentas_bancarias SET saldo_actual=$1 WHERE id=$2',[parseFloat(saldo_nuevo),req.params.id]);
    // Record adjustment movement
    await QR(req,`INSERT INTO movimientos_bancarios(cuenta_id,tipo_operacion,concepto,monto,moneda,categoria,conciliado,notas,created_by)
      VALUES($1,$2,'Ajuste de conciliación',$3,$4,'Ajuste',true,$5,$6)`,
      [req.params.id,diff>=0?'ingreso':'egreso',Math.abs(diff),c.moneda||'MXN',notas||null,req.user?.id]);
    res.json({ok:true,saldo_nuevo:parseFloat(saldo_nuevo)});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── Movimientos Bancarios ──────────────────────────────────────
app.get('/api/tesoreria/movimientos', auth, licencia, async (req,res)=>{
  try{
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await ensureTesNomTables(schema);
    const where=req.query.cuenta_id?'WHERE m.cuenta_id=$1':'';
    const vals=req.query.cuenta_id?[req.query.cuenta_id]:[];
    const rows=await QR(req,`SELECT m.*,cb.nombre cuenta_nombre,cb.moneda
      FROM movimientos_bancarios m LEFT JOIN cuentas_bancarias cb ON cb.id=m.cuenta_id
      ${where} ORDER BY m.fecha DESC,m.id DESC LIMIT 500`,vals);
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/tesoreria/movimientos', auth, empresaActiva, async (req,res)=>{
  try{
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await ensureTesNomTables(schema);
    const {cuenta_id,tipo_operacion,fecha,concepto,monto,categoria,beneficiario,referencia,numero_cheque,cuenta_destino_id,notas}=req.body;
    if(!cuenta_id||!concepto||!monto)return res.status(400).json({error:'Campos requeridos faltantes'});
    const [ct]=await QR(req,'SELECT saldo_actual,moneda FROM cuentas_bancarias WHERE id=$1',[cuenta_id]);
    if(!ct)return res.status(404).json({error:'Cuenta no encontrada'});
    const montoNum=parseFloat(monto);
    const nuevoSaldo=tipo_operacion==='ingreso'?parseFloat(ct.saldo_actual)+montoNum:parseFloat(ct.saldo_actual)-montoNum;
    // Update saldo
    await QR(req,'UPDATE cuentas_bancarias SET saldo_actual=$1 WHERE id=$2',[nuevoSaldo,cuenta_id]);
    if(tipo_operacion==='transferencia'&&cuenta_destino_id){
      const [cd]=await QR(req,'SELECT saldo_actual FROM cuentas_bancarias WHERE id=$1',[cuenta_destino_id]);
      if(cd)await QR(req,'UPDATE cuentas_bancarias SET saldo_actual=$1 WHERE id=$2',[parseFloat(cd.saldo_actual)+montoNum,cuenta_destino_id]);
    }
    // Ensure orden_compra_id column exists
    await QR(req,'ALTER TABLE movimientos_bancarios ADD COLUMN IF NOT EXISTS orden_compra_id INTEGER').catch(()=>{});
    const {orden_compra_id} = req.body;
    const rows=await QR(req,`INSERT INTO movimientos_bancarios(cuenta_id,cuenta_destino_id,tipo_operacion,fecha,concepto,monto,moneda,categoria,beneficiario,referencia,numero_cheque,saldo_posterior,notas,orden_compra_id,created_by)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING *`,
      [cuenta_id,cuenta_destino_id||null,tipo_operacion,fecha||new Date().toISOString().slice(0,10),concepto,montoNum,ct.moneda||'MXN',categoria||null,beneficiario||null,referencia||null,numero_cheque||null,nuevoSaldo,notas||null,orden_compra_id||null,req.user?.id]);
    // If linked to OC, optionally mark OC as paid
    if(orden_compra_id && tipo_operacion==='egreso'){
      await QR(req,`UPDATE ordenes_proveedor SET estatus='pagada' WHERE id=$1 AND estatus IN ('aprobada','recibida')`,[orden_compra_id]).catch(()=>{});
    }
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/tesoreria/movimientos/:id/conciliar', auth, async (req,res)=>{
  try{
    const rows=await QR(req,'UPDATE movimientos_bancarios SET conciliado=$1 WHERE id=$2 RETURNING *',[req.body.conciliado,req.params.id]);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── DELETE movimiento bancario — revierte saldo de cuenta ──────────
app.delete('/api/tesoreria/movimientos/:id', auth, empresaActiva, async (req,res)=>{
  const client = await pool.connect();
  try{
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query('BEGIN');
    // Get movement to know account and amount
    const {rows:[mov]} = await client.query(
      'SELECT * FROM movimientos_bancarios WHERE id=$1', [req.params.id]);
    if(!mov) return res.status(404).json({error:'Movimiento no encontrado'});
    // Permitir eliminar cualquier movimiento (conciliado o no)
    // La advertencia se muestra en el frontend antes de confirmar
    // Reverse the balance: if it was egreso → add back; if ingreso → subtract
    const monto = parseFloat(mov.monto||0);
    const ajuste = mov.tipo_operacion === 'ingreso' ? -monto : monto;
    await client.query(
      'UPDATE cuentas_bancarias SET saldo_actual = saldo_actual + $1 WHERE id=$2',
      [ajuste, mov.cuenta_id]);
    // Also reverse destination account for transfers
    if(mov.tipo_operacion === 'transferencia' && mov.cuenta_destino_id){
      await client.query(
        'UPDATE cuentas_bancarias SET saldo_actual = saldo_actual - $1 WHERE id=$2',
        [monto, mov.cuenta_destino_id]);
    }
    await client.query('DELETE FROM movimientos_bancarios WHERE id=$1', [req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true, mensaje:'Movimiento eliminado y saldo revertido'});
  }catch(e){
    await client.query('ROLLBACK');
    res.status(500).json({error:e.message});
  }finally{client.release();}
});
app.get('/api/tesoreria/flujo', auth, licencia, async (req,res)=>{
  try{
    const M=new Date().getMonth()+1,Y=new Date().getFullYear();
    const rows=await QR(req,`SELECT EXTRACT(MONTH FROM fecha) mes,EXTRACT(YEAR FROM fecha) anio,
      tipo_operacion,SUM(monto) total FROM movimientos_bancarios
      WHERE fecha >= (CURRENT_DATE - INTERVAL '6 months')
      GROUP BY mes,anio,tipo_operacion ORDER BY anio,mes`);
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// ═══════════════════════════════════════════════════════════════
// NÓMINA / RRHH — Empleados, cálculos, historial
// ═══════════════════════════════════════════════════════════════
app.get('/api/nomina/empleados', auth, licencia, async (req,res)=>{
  try{
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await ensureTesNomTables(schema);
    const rows=await QR(req,'SELECT * FROM empleados ORDER BY nombre');
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/nomina/empleados', auth, empresaActiva, async (req,res)=>{
  try{
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await ensureTesNomTables(schema);
    const b=req.body;
    if(!b.nombre||!b.puesto)return res.status(400).json({error:'Nombre y puesto requeridos'});
    const cols=['nombre','puesto','departamento','tipo_contrato','fecha_ingreso','periodicidad_pago','jornada','salario_diario','salario_bruto','salario_base_cotizacion','dias_vacaciones','prima_vacacional','rfc','curp','fecha_nacimiento','telefono','email','direccion','numero_imss','numero_infonavit','banco_clabe','banco','notas'];
    const vals=cols.map(c=>{
      if(['salario_diario','salario_bruto','salario_base_cotizacion'].includes(c))return parseFloat(b[c])||0;
      if(['dias_vacaciones','prima_vacacional'].includes(c))return parseInt(b[c])||6;
      return b[c]||null;
    });
    const ph=cols.map((_,i)=>`$${i+1}`).join(',');
    const rows=await QR(req,`INSERT INTO empleados(${cols.join(',')}) VALUES(${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/nomina/empleados/:id', auth, empresaActiva, async (req,res)=>{
  try{
    const b=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(c,v)=>{if(v!==undefined){sets.push(`${c}=$${i++}`);vals.push(v);}};
    ['nombre','puesto','departamento','tipo_contrato','fecha_ingreso','periodicidad_pago','jornada','rfc','curp','fecha_nacimiento','telefono','email','direccion','numero_imss','numero_infonavit','banco_clabe','banco','notas'].forEach(c=>add(c,b[c]||null));
    ['salario_diario','salario_bruto','salario_base_cotizacion'].forEach(c=>add(c,parseFloat(b[c])||0));
    ['dias_vacaciones','prima_vacacional'].forEach(c=>add(c,parseInt(b[c])||6));
    if(b.activo!==undefined)add('activo',b.activo);
    if(b.activo===false)add('fecha_baja',new Date().toISOString().slice(0,10));
    vals.push(req.params.id);
    const rows=await QR(req,`UPDATE empleados SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/nomina/pagos', auth, licencia, async (req,res)=>{
  try{
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await ensureTesNomTables(schema);
    const rows=await QR(req,`SELECT p.*,e.nombre empleado_nombre FROM pagos_nomina p
      LEFT JOIN empleados e ON e.id=p.empleado_id ORDER BY p.fecha DESC LIMIT 200`);
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/nomina/pagos', auth, empresaActiva, async (req,res)=>{
  try{
    const schema=req.user?.schema||global._defaultSchema||'emp_vef';
    await ensureTesNomTables(schema);
    const {empleado_id,monto,fecha,tipo,periodo,metodo,referencia,notas}=req.body;
    if(!empleado_id||!monto)return res.status(400).json({error:'empleado_id y monto requeridos'});
    const rows=await QR(req,`INSERT INTO pagos_nomina(empleado_id,fecha,monto,tipo,periodo,metodo,referencia,notas,created_by)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [empleado_id,fecha||new Date().toISOString().slice(0,10),parseFloat(monto),tipo||'nomina',periodo||null,metodo||'Transferencia',referencia||null,notas||null,req.user?.id]);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/proveedores', (req,res)=>{
  res.sendFile(path.join(__dirname,'frontend','proveedores.html'));
});

app.get('/app', (req,res)=>{
  const fs   = require('fs');
  const fp   = require('path').join(__dirname,'frontend','app.html');
  try {
    let h = fs.readFileSync(fp,'utf8');
    // Eliminar SOLO el script externo de Cloudflare manteniendo el resto intacto
    // Patron: <script ...cdn-cgi...></script>  seguido de <script> principal
    h = h.replace(/<script[^>]*cdn-cgi[^>]*><\/script>/gi, '');
    // Limpiar email obfuscation
    h = h.replace(/<a[^>]*cdn-cgi[^>]*>[^<]*<\/a>/gi, 'soporte.ventas@vef-automatizacion.com');
    h = h.replace(/data-cfemail="[^"]*"/gi, '');
    res.setHeader('Cache-Control','no-cache,no-store,must-revalidate');
    res.setHeader('Content-Type','text/html; charset=utf-8');
    res.setHeader('Content-Security-Policy',"script-src 'self' 'unsafe-inline'");
    // Guardar en disco
    try{fs.writeFileSync(fp,h,'utf8');}catch(e){}
    res.send(h);
  } catch(e){ res.sendFile(fp); }
});

app.get('/api/sat/health', async (req, res) => {  // sin auth — el frontend lo llama antes del login
  // Ping directo al servicio Python — acepta cualquier respuesta HTTP 2xx
  const http = require('http');
  const ok = await new Promise((resolve) => {
    const r = http.request(
      { hostname:'127.0.0.1', port:5050, path:'/health', method:'GET', timeout:4000 },
      (resp) => {
        resp.resume(); // consumir body sin parsear
        resolve(resp.statusCode >= 200 && resp.statusCode < 300);
      }
    );
    r.on('error', () => resolve(false));
    r.on('timeout', () => { r.destroy(); resolve(false); });
    r.end();
  });

  // Si no responde, intentar arrancar el servicio automaticamente
  if (!ok) startSatService();

  global._satPythonOk = ok;
  res.json({
    ok,
    corriendo: ok,
    puerto: 5050,
    mensaje: ok
      ? 'SAT corriendo correctamente en puerto 5050'
      : 'SAT no disponible. Ejecuta: python sat_service/sat_api.py'
  });
});
;



// Guardar configuración de IA (DeepSeek API Key)
app.post('/api/ia/guardar-config', auth, async (req, res) => {
  try {
    const { deepseek_api_key, deepseek_modelo } = req.body;
    await QR(req, `ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS deepseek_api_key TEXT`).catch(()=>{});
    await QR(req, `ALTER TABLE empresa_config ADD COLUMN IF NOT EXISTS deepseek_modelo VARCHAR(50)`).catch(()=>{});
    const updates = [];
    const vals = [];
    let i = 1;
    if (deepseek_api_key) { updates.push(`deepseek_api_key=$${i++}`); vals.push(deepseek_api_key); }
    if (deepseek_modelo)  { updates.push(`deepseek_modelo=$${i++}`);  vals.push(deepseek_modelo); }
    if (!updates.length) return res.status(400).json({ error: 'Nada que actualizar' });
    await QR(req, `UPDATE empresa_config SET ${updates.join(',')}`, vals);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/api/ia/estado', auth, async (req, res) => {
  try {
    const apiKey = await getDeepSeekKey(req.user.schema);
    res.json({
      ok: !!apiKey,
      modelo: 'deepseek-chat',
      proveedor: 'DeepSeek',
      configurada: !!apiKey,
      mensaje: apiKey ? 'DeepSeek configurado y listo' : 'Configura tu API Key en Configuración → IA'
    });
  } catch(e) { res.json({ ok: false, configurada: false }); }
});

app.post('/api/ia/generar-cotizacion', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const { descripcion, cliente, tipo_servicio, urgente } = req.body;
    if (!descripcion) return res.status(400).json({ error: 'Descripción requerida' });

    const ejemplos = await QR(req, `
      SELECT c.numero_cotizacion, c.total, c.moneda
      FROM cotizaciones c WHERE c.total > 0
      ORDER BY c.created_at DESC LIMIT 5`).catch(()=>[]);

    const empCfg = (await Q('SELECT nombre,rfc FROM empresa_config LIMIT 1',[],req.user.schema))[0]||{};

    let ejemplosCtx = '';
    if (ejemplos.length) {
      ejemplosCtx = 'Cotizaciones de referencia:\n' +
        ejemplos.map(e => '- ' + e.numero_cotizacion + ': $' + e.total + ' ' + e.moneda).join('\n') + '\n';
    }

    const prompt = 'Eres el asistente de ' + (empCfg.nombre||'VEF Automatización') + ', empresa de automatización industrial en México.\n\n' +
      (ejemplosCtx || '') +
      'Genera una cotización para:\n' +
      'Cliente: ' + (cliente||'Por definir') + '\n' +
      'Descripción: ' + descripcion + '\n' +
      'Tipo: ' + (tipo_servicio||'Automatización') + '\n' +
      'Urgente: ' + (urgente?'Sí':'No') + '\n\n' +
      'Responde SOLO en JSON válido con esta estructura exacta: {"titulo":"...","resumen":"...","items":[{"descripcion":"...","cantidad":1,"unidad":"pza","precio_unitario":0}],"notas":"...","moneda":"USD","tiempo_entrega":"..."}';

    const text = await deepseekChat(
      [{ role: 'user', content: prompt }],
      req.user.schema, 'deepseek-chat', 1500
    );

    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return res.status(400).json({ error: 'DeepSeek no generó respuesta válida', raw: text });

    const data = JSON.parse(jsonMatch[0]);
    res.json({ ok: true, cotizacion: data });

  } catch(e) {
    console.error('IA DeepSeek error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/ia/mejorar-descripcion', auth, empresaActiva, licencia, async (req, res) => {
  // DeepSeek - key validated per request
  try {
    const { texto, tipo = 'descripcion' } = req.body;
    if (!texto) return res.status(400).json({ error: 'texto requerido' });

    const prompts = {
      descripcion: `Mejora esta descripción de servicio/producto para una cotización industrial. Hazla más profesional y clara. Solo devuelve el texto mejorado:

${texto}`,
      notas:       `Mejora estas notas/condiciones comerciales para una cotización. Solo devuelve el texto:

${texto}`,
      asunto:      `Crea un asunto de email profesional para enviar esta cotización. Solo el asunto:

${texto}`,
    };

    const result = await deepseekChat(
      [{ role: 'user', content: prompts[tipo] || prompts.descripcion }],
      req.user.schema, 'deepseek-chat', 400
    );

    res.json({ ok: true, texto: result.trim() || texto });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// S3 — Archivos PDF en la nube
// ================================================================
app.post('/api/s3/subir-pdf', auth, async (req, res) => {
  try {
    const { nombre, datos_base64, tipo = 'cotizacion' } = req.body;
    if (!datos_base64) return res.status(400).json({ error: 'datos_base64 requerido' });

    const schema = req.user?.schema || 'emp_vef';
    const ts     = Date.now();
    const key    = `${schema}/${tipo}/${ts}_${(nombre||'doc').replace(/[^a-zA-Z0-9._-]/g,'_')}.pdf`;
    const buf    = Buffer.from(datos_base64, 'base64');

    const savedKey = await global.s3Upload(buf, key);
    if (!savedKey) return res.status(500).json({ error: 'S3 no disponible. Verifica AWS_ACCESS_KEY_ID en .env' });

    const url = await global.s3SignedUrl(savedKey);
    res.json({ ok: true, key: savedKey, url, expires_in: 900 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/s3/url', auth, async (req, res) => {
  try {
    const { key } = req.body;
    if (!key) return res.status(400).json({ error: 'key requerida' });
    const url = await global.s3SignedUrl(key, 3600);
    if (!url) return res.status(404).json({ error: 'Archivo no encontrado o S3 no configurado' });
    res.json({ ok: true, url, expires_in: 3600 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/s3/eliminar', auth, async (req, res) => {
  try {
    const { key } = req.body;
    await global.s3Delete(key);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ================================================================
// MULTIEMPRESA — Login con selector de empresa
// ================================================================

// GET /api/auth/empresas — lista de empresas activas (para login)
app.get('/api/auth/empresas', async (req, res) => {
  try {
    const rows = await pool.query(
      'SELECT id,nombre,slug FROM public.empresas WHERE activa=true ORDER BY nombre');
    res.json(rows.rows.map(e => ({ id: e.id, nombre: e.nombre })));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/auth/mis-empresas?username=X — empresas donde existe ese usuario
app.post('/api/auth/mis-empresas', async (req, res) => {
  try {
    const { username } = req.body;
    if (!username) return res.json([]);
    const rows = await pool.query(
      `SELECT e.id, e.nombre, e.slug
       FROM public.usuarios u
       JOIN public.empresas e ON e.id = u.empresa_id
       WHERE (u.username=$1 OR u.email=$1) AND u.activo=true AND e.activa=true
       ORDER BY e.nombre`, [username]);
    res.json(rows.rows.map(e => ({ id: e.id, nombre: e.nombre })));
  } catch(e) { res.json([]); }
});

// POST /api/auth/cambiar-empresa — usuario master puede cambiar de empresa
app.post('/api/auth/cambiar-empresa', auth, async (req, res) => {
  try {
    if (req.user.rol !== 'admin' && req.user.rol !== 'master') {
      return res.status(403).json({ error: 'Solo el usuario master puede cambiar de empresa' });
    }
    const { empresa_id } = req.body;
    if (!empresa_id) return res.status(400).json({ error: 'empresa_id requerido' });

    const empRow = await pool.query(
      'SELECT id,nombre,slug,activa FROM public.empresas WHERE id=$1', [empresa_id]);
    if (!empRow.rows[0]) return res.status(404).json({ error: 'Empresa no encontrada' });
    if (!empRow.rows[0].activa) return res.status(400).json({ error: 'Empresa inactiva' });

    const empNombre = empRow.rows[0].nombre;
    const schema    = 'emp_' + empRow.rows[0].slug.replace(/[^a-z0-9]/g,'_');

    // Emitir nuevo token con el schema de la empresa destino
    const newToken = jwt.sign({
      id:             req.user.id,
      username:       req.user.username,
      nombre:         req.user.nombre,
      rol:            req.user.rol,
      empresa_id:     parseInt(empresa_id),
      schema,
      empresa_nombre: empNombre,
    }, JWT_SECRET, { expiresIn: '8h' });

    console.log('Cambio empresa:', req.user.username, '→', empNombre, '(schema:', schema + ')');
    res.json({ ok: true, token: newToken,
      user: { id: req.user.id, nombre: req.user.nombre, username: req.user.username,
              rol: req.user.rol, empresa_id: parseInt(empresa_id), empresa_nombre: empNombre, schema } });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ================================================================
// SAT DESCARGA MASIVA — Proxy al microservicio Python (sat_service/sat_api.py)
// El Python usa los módulos originales Login.py, Request.py, Verify.py, Download.py
// Iniciar el Python con: python3 sat_service/sat_api.py
// ================================================================

function satProxy(endpoint, body, timeoutMs = 120000) {
  const http = require('http');
  const data = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const reqOpts = {
      hostname: '127.0.0.1',
      port: 5050,
      path: endpoint,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data)
      },
      timeout: timeoutMs
    };
    const req = http.request(reqOpts, (res) => {
      let buf = '';
      res.on('data', c => buf += c);
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(buf); }
        catch(e) { parsed = { ok: false, error: 'Respuesta inválida del servicio SAT Python', raw: buf.slice(0,200) }; }
        // FIX: Si el microservicio Python retorna 4xx/5xx, incluir código en el objeto
        // para que el frontend pueda mostrar el error real en lugar de silenciarlo
        if (res.statusCode >= 400) {
          parsed.ok = false;
          parsed._httpStatus = res.statusCode;
          if (!parsed.error && !parsed.detail) {
            parsed.error = `SAT Python retornó HTTP ${res.statusCode}`;
          }
          // Log para diagnóstico en servidor
          console.error(`satProxy ${endpoint} → HTTP ${res.statusCode}:`, JSON.stringify(parsed).slice(0,300));
        }
        resolve(parsed);
      });
    });
    req.on('error', (e) => {
      if (e.code === 'ECONNREFUSED' || e.code === 'ECONNRESET') {
        console.warn('⚠️  SAT Python no responde, intentando reiniciar...');
        global._satPythonOk = false;
        startSatService(); // reiniciar automáticamente
        reject(new Error(
          'El servicio SAT se está iniciando. Espera 5 segundos e intenta de nuevo. ' +
          'Si persiste, abre una terminal y ejecuta: python sat_service/sat_api.py'
        ));
      } else {
        reject(e);
      }
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout — SAT no respondió')); });
    req.write(data);
    req.end();
  });
}

app.post('/api/sat/login', auth, async (req, res) => {
  try {
    const r = await satProxy('/login', req.body);
    res.json(r);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Verificar si token SAT sigue válido
app.post('/api/sat/verify-token', auth, async (req, res) => {
  try {
    const r = await satProxy('/verify_token', req.body);
    res.json(r);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/api/sat/solicitar', auth, async (req, res) => {
  try {
    const r = await satProxy('/solicitar', req.body);
    if (r.ok && r.solicitud?.IdSolicitud) {
      await QR(req, `INSERT INTO sat_solicitudes(id_solicitud,fecha_inicio,fecha_fin,tipo,estatus,created_by)
        VALUES($1,$2,$3,$4,'pendiente',$5) ON CONFLICT (id_solicitud) DO NOTHING`,
        [r.solicitud.IdSolicitud, req.body.fecha_inicio, req.body.fecha_fin,
         req.body.tipo||'CFDI', req.user.id]).catch(()=>{});
    }
    res.json(r);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/api/sat/verificar', auth, async (req, res) => {
  try {
    const r = await satProxy('/verificar', req.body);
    if (r.ok && r.listo && r.paquetes?.length) {
      await QR(req, `UPDATE sat_solicitudes SET estatus='listo', paquetes=$1 WHERE id_solicitud=$2`,
        [JSON.stringify(r.paquetes), req.body.id_solicitud]).catch(()=>{});
    }
    res.json(r);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Helpers SAT ────────────────────────────────────────────────
function satFecha(f){
  if(!f||f==='') return null;
  try{ const d=new Date(String(f).trim()); return isNaN(d.getTime())?null:d.toISOString(); }catch(e){ return null; }
}
function xmlAttr(xml,attr){
  const m=String(xml||'').match(new RegExp('\\b'+attr+'="([^"]*)"','i'));
  return m?m[1]:'';
}
// ── Fin helpers ──────────────────────────────────────────────────

app.post('/api/sat/descargar', auth, async (req, res) => {
  try {
    const r = await satProxy('/descargar', req.body);
    console.log('[SAT/descargar] ok='+r.ok+' cfdis='+(r.cfdis?.length||0)+' metadatos='+(r.metadatos?.length||0)+' schema='+req.user?.schema);

    // ── Asegurar tabla con TODAS las columnas ─────────────────────
    const ensureTable = async () => {
      await QR(req, `CREATE TABLE IF NOT EXISTS sat_cfdis (
        id SERIAL PRIMARY KEY, uuid VARCHAR(100) UNIQUE, fecha_cfdi TIMESTAMP,
        tipo_comprobante VARCHAR(5), subtotal NUMERIC(15,2) DEFAULT 0,
        total NUMERIC(15,2) DEFAULT 0, moneda VARCHAR(10) DEFAULT 'MXN',
        emisor_rfc VARCHAR(20), emisor_nombre VARCHAR(300),
        receptor_rfc VARCHAR(20), receptor_nombre VARCHAR(300), uso_cfdi VARCHAR(10),
        forma_pago VARCHAR(5), metodo_pago VARCHAR(5), lugar_expedicion VARCHAR(10),
        serie VARCHAR(50), folio VARCHAR(50), no_certificado VARCHAR(30),
        version VARCHAR(5), fecha_timbrado TIMESTAMP, rfc_prov_certif VARCHAR(20),
        xml_content TEXT, id_paquete VARCHAR(200),
        estatus_sat VARCHAR(20), monto_sat NUMERIC(15,2), rfc_pac VARCHAR(20),
        fecha_cancelacion TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())`).catch(()=>{});
      for (const col of [
        'forma_pago VARCHAR(5)','metodo_pago VARCHAR(5)','lugar_expedicion VARCHAR(10)',
        'serie VARCHAR(50)','folio VARCHAR(50)','no_certificado VARCHAR(30)',
        'version VARCHAR(5)','fecha_timbrado TIMESTAMP','rfc_prov_certif VARCHAR(20)',
        'estatus_sat VARCHAR(20)','monto_sat NUMERIC(15,2)','rfc_pac VARCHAR(20)',
        'fecha_cancelacion TIMESTAMP','xml_content TEXT'
      ]) { await QR(req, `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS ${col}`).catch(()=>{}); }
    };

    // ── GUARDAR CFDIs (XML completo) ──────────────────────────────
    if (r.ok && r.cfdis && r.cfdis.length > 0) {
      await ensureTable();
      let saved = 0;
      const errores = [];
      for (const cfdi of r.cfdis) {
        if (!cfdi.uuid) { console.log('[SAT] CFDI sin uuid, skip'); continue; }
        try {
          await QR(req, `INSERT INTO sat_cfdis(
              uuid,fecha_cfdi,tipo_comprobante,subtotal,total,moneda,
              emisor_rfc,emisor_nombre,receptor_rfc,receptor_nombre,uso_cfdi,
              forma_pago,metodo_pago,lugar_expedicion,serie,folio,
              no_certificado,version,fecha_timbrado,rfc_prov_certif,
              xml_content,id_paquete)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
            ON CONFLICT (uuid) DO UPDATE SET
              xml_content=EXCLUDED.xml_content,
              tipo_comprobante=COALESCE(EXCLUDED.tipo_comprobante,sat_cfdis.tipo_comprobante),
              fecha_cfdi=COALESCE(EXCLUDED.fecha_cfdi,sat_cfdis.fecha_cfdi),
              emisor_rfc=COALESCE(EXCLUDED.emisor_rfc,sat_cfdis.emisor_rfc),
              emisor_nombre=COALESCE(EXCLUDED.emisor_nombre,sat_cfdis.emisor_nombre),
              receptor_rfc=COALESCE(EXCLUDED.receptor_rfc,sat_cfdis.receptor_rfc),
              receptor_nombre=COALESCE(EXCLUDED.receptor_nombre,sat_cfdis.receptor_nombre),
              forma_pago=COALESCE(EXCLUDED.forma_pago,sat_cfdis.forma_pago),
              metodo_pago=COALESCE(EXCLUDED.metodo_pago,sat_cfdis.metodo_pago),
              uso_cfdi=COALESCE(EXCLUDED.uso_cfdi,sat_cfdis.uso_cfdi),
              version=EXCLUDED.version, fecha_timbrado=EXCLUDED.fecha_timbrado,
              rfc_prov_certif=EXCLUDED.rfc_prov_certif, updated_at=NOW()`,
            [cfdi.uuid,
             satFecha(cfdi.fecha),
             cfdi.tipo||null,
             parseFloat(cfdi.subtotal)||0,
             parseFloat(cfdi.total)||0,
             cfdi.moneda||'MXN',
             cfdi.emisor_rfc||null, cfdi.emisor_nombre||null,
             cfdi.receptor_rfc||null, cfdi.receptor_nombre||null, cfdi.uso_cfdi||null,
             cfdi.forma_pago||null, cfdi.metodo_pago||null, cfdi.lugar_expedicion||null,
             cfdi.serie||null, cfdi.folio||null, cfdi.no_certificado||null,
             cfdi.version||null, satFecha(cfdi.fecha_timbrado), cfdi.pac_rfc||cfdi.rfc_prov_certif||null,
             cfdi.xml||null, req.body.id_paquete||null]);
          saved++;
        } catch(eCfdi) {
          console.error('[SAT CFDI ERROR] uuid='+cfdi.uuid+' msg='+eCfdi.message);
          errores.push({ uuid: cfdi.uuid, error: eCfdi.message });
        }
      }
      console.log('[SAT] CFDIs guardados='+saved+'/'+r.cfdis.length+(errores.length?' errores='+errores.length:''));
      await QR(req, `UPDATE sat_solicitudes SET estatus='descargado' WHERE id_solicitud=$1`,
        [req.body.id_solicitud]).catch(()=>{});
      r.guardados = saved;
      if (errores.length) r.errores_bd = errores;
      r.cfdis = r.cfdis.map(function(c){ return Object.assign({},c,{xml:undefined}); });
    }

    // ── GUARDAR METADATOS ─────────────────────────────────────────
    if (r.ok && r.metadatos && r.metadatos.length > 0) {
      await ensureTable();
      let savedMeta = 0, skipped = 0;
      console.log('[SAT META] registros='+r.metadatos.length+' primer keys='+Object.keys(r.metadatos[0]).join(','));
      for (const mRaw of r.metadatos) {
        // Buscar UUID en todas las variantes posibles
        const uuid = String(
          mRaw.uuid||mRaw.Uuid||mRaw.UUID||mRaw.uuid_sat||
          (mRaw.raw&&typeof mRaw.raw==='object'?
            mRaw.raw.Uuid||mRaw.raw.UUID||mRaw.raw.uuid||
            Object.values(mRaw.raw).find(function(v){ return v&&/^[0-9a-f-]{36}$/i.test(String(v).trim()); })||''
          :'')||''
        ).trim();
        if(!uuid||uuid.length<10){ skipped++; continue; }
        try {
          await QR(req, `INSERT INTO sat_cfdis(
              uuid,fecha_cfdi,tipo_comprobante,subtotal,total,monto_sat,moneda,
              emisor_rfc,emisor_nombre,receptor_rfc,receptor_nombre,
              uso_cfdi,forma_pago,metodo_pago,lugar_expedicion,
              serie,folio,version,rfc_prov_certif,
              estatus_sat,rfc_pac,fecha_cancelacion,id_paquete)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
            ON CONFLICT (uuid) DO UPDATE SET
              tipo_comprobante  = COALESCE(EXCLUDED.tipo_comprobante,sat_cfdis.tipo_comprobante),
              fecha_cfdi        = COALESCE(EXCLUDED.fecha_cfdi,sat_cfdis.fecha_cfdi),
              subtotal          = COALESCE(NULLIF(EXCLUDED.subtotal,0),sat_cfdis.subtotal),
              total             = COALESCE(NULLIF(EXCLUDED.total,0),sat_cfdis.total),
              monto_sat         = COALESCE(NULLIF(EXCLUDED.monto_sat,0),sat_cfdis.monto_sat),
              emisor_rfc        = COALESCE(EXCLUDED.emisor_rfc,sat_cfdis.emisor_rfc),
              emisor_nombre     = COALESCE(EXCLUDED.emisor_nombre,sat_cfdis.emisor_nombre),
              receptor_rfc      = COALESCE(EXCLUDED.receptor_rfc,sat_cfdis.receptor_rfc),
              receptor_nombre   = COALESCE(EXCLUDED.receptor_nombre,sat_cfdis.receptor_nombre),
              uso_cfdi          = COALESCE(EXCLUDED.uso_cfdi,sat_cfdis.uso_cfdi),
              forma_pago        = COALESCE(EXCLUDED.forma_pago,sat_cfdis.forma_pago),
              metodo_pago       = COALESCE(EXCLUDED.metodo_pago,sat_cfdis.metodo_pago),
              estatus_sat       = EXCLUDED.estatus_sat,
              rfc_pac           = COALESCE(EXCLUDED.rfc_pac,sat_cfdis.rfc_pac),
              fecha_cancelacion = COALESCE(EXCLUDED.fecha_cancelacion,sat_cfdis.fecha_cancelacion),
              updated_at        = NOW()`,
            [uuid,
             satFecha(mRaw.fecha_emision||mRaw.fecha),
             mRaw.tipo||mRaw.efecto||null,
             parseFloat(mRaw.subtotal||mRaw.monto||0),
             parseFloat(mRaw.total||mRaw.monto||0),
             parseFloat(mRaw.monto||0),
             mRaw.moneda||'MXN',
             mRaw.rfc_emisor||null, mRaw.nombre_emisor||null,
             mRaw.rfc_receptor||null, mRaw.nombre_receptor||null,
             mRaw.uso_cfdi||null, mRaw.forma_pago||null, mRaw.metodo_pago||null,
             mRaw.lugar_expedicion||null,
             mRaw.serie||null, mRaw.folio||null,
             mRaw.version||null, mRaw.rfc_pac||null,
             mRaw.estatus||'Vigente', mRaw.rfc_pac||null,
             satFecha(mRaw.fecha_cancelacion),
             req.body.id_paquete||null]);
          savedMeta++;
        } catch(em) {
          console.error('[SAT META ERROR] uuid='+uuid+' err='+em.message);
        }
      }
      console.log('[SAT META] guardados='+savedMeta+' skipped='+skipped+' de '+r.metadatos.length);
      await QR(req, `UPDATE sat_solicitudes SET estatus='descargado' WHERE id_solicitud=$1`,
        [req.body.id_solicitud]).catch(()=>{});
      r.guardados_meta = savedMeta;
    }

    res.json(r);
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/api/sat/cfdis', auth, async (req, res) => {
  try {
    const { tipo, rfc, desde, hasta } = req.query;
    // Asegurar tabla y columnas nuevas
    await QR(req, `CREATE TABLE IF NOT EXISTS sat_cfdis (
      id SERIAL PRIMARY KEY,uuid VARCHAR(100) UNIQUE,fecha_cfdi TIMESTAMP,
      tipo_comprobante VARCHAR(5),subtotal NUMERIC(15,2) DEFAULT 0,total NUMERIC(15,2) DEFAULT 0,
      moneda VARCHAR(10) DEFAULT 'MXN',emisor_rfc VARCHAR(20),emisor_nombre VARCHAR(300),
      receptor_rfc VARCHAR(20),receptor_nombre VARCHAR(300),uso_cfdi VARCHAR(10),
      forma_pago VARCHAR(5),metodo_pago VARCHAR(5),lugar_expedicion VARCHAR(10),
      serie VARCHAR(50),folio VARCHAR(50),no_certificado VARCHAR(30),
      version VARCHAR(5),fecha_timbrado TIMESTAMP,rfc_prov_certif VARCHAR(20),
      xml_content TEXT,id_paquete VARCHAR(200),
      created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW())`)
    .catch(e => console.warn('sat_cfdis ensure:', e.message));
    for (const col of [
      'forma_pago VARCHAR(5)','metodo_pago VARCHAR(5)','lugar_expedicion VARCHAR(10)',
      'serie VARCHAR(50)','folio VARCHAR(50)','no_certificado VARCHAR(30)',
      'version VARCHAR(5)','fecha_timbrado TIMESTAMP','rfc_prov_certif VARCHAR(20)'
    ]) { await QR(req, `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS ${col}`).catch(()=>{}); }

    let where = 'WHERE 1=1'; const vals = []; let i = 1;
    if (tipo)  { where += ` AND tipo_comprobante=$${i++}`; vals.push(tipo); }
    if (rfc)   { where += ` AND (emisor_rfc=$${i++} OR receptor_rfc=$${i++})`; vals.push(rfc,rfc); }
    if (desde) { where += ` AND fecha_cfdi>=$${i++}`; vals.push(desde); }
    if (hasta) { where += ` AND fecha_cfdi<=$${i++}`; vals.push(hasta); }
    const rows = await QR(req, `SELECT id,uuid,fecha_cfdi,tipo_comprobante,
      COALESCE(subtotal,monto_sat,0)::numeric AS subtotal,
      COALESCE(total,monto_sat,0)::numeric AS total, moneda,
      emisor_rfc,emisor_nombre,receptor_rfc,receptor_nombre,uso_cfdi,
      forma_pago,metodo_pago,lugar_expedicion,serie,folio,no_certificado,
      version,fecha_timbrado,rfc_prov_certif,
      estatus_sat,monto_sat,rfc_pac,fecha_cancelacion,created_at
      FROM sat_cfdis ${where} ORDER BY fecha_cfdi DESC NULLS LAST LIMIT 500`, vals);
    res.json(Array.isArray(rows) ? rows : []);
  } catch(e) { console.error('GET /api/sat/cfdis:', e.message); res.status(500).json({ error: e.message }); }
});

app.get('/api/sat/cfdis/:uuid/xml', auth, async (req, res) => {
  try {
    const rows = await QR(req, 'SELECT xml_content,uuid FROM sat_cfdis WHERE uuid=$1', [req.params.uuid]);
    if (!rows.length || !rows[0].xml_content) return res.status(404).json({ error: 'CFDI no encontrado' });
    res.setHeader('Content-Type', 'application/xml; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${rows[0].uuid}.xml"`);
    res.send(rows[0].xml_content);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sat/solicitudes', auth, async (req, res) => {
  try {
    const rows = await QR(req, `SELECT * FROM sat_solicitudes ORDER BY created_at DESC LIMIT 50`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Re-parsear XMLs ya guardados en BD ───────────────────────────
// Extrae MetodoPago, FormaPago, LugarExpedicion, Serie, Folio, etc.
// desde xml_content y actualiza las columnas vacías.
app.post('/api/sat/reparsear-xmls', auth, async (req, res) => {
  try {
    // Agregar columnas si no existen
    for (const col of [
      'forma_pago VARCHAR(5)','metodo_pago VARCHAR(5)','lugar_expedicion VARCHAR(10)',
      'serie VARCHAR(50)','folio VARCHAR(50)','no_certificado VARCHAR(30)',
      'version VARCHAR(5)','fecha_timbrado TIMESTAMP','rfc_prov_certif VARCHAR(20)'
    ]) { await QR(req, `ALTER TABLE sat_cfdis ADD COLUMN IF NOT EXISTS ${col}`).catch(()=>{}); }

    // Traer todos los que tienen xml pero campos nuevos vacíos
    const rows = await QR(req,
      `SELECT id, uuid, xml_content FROM sat_cfdis
       WHERE xml_content IS NOT NULL AND xml_content != ''
         AND (metodo_pago IS NULL OR forma_pago IS NULL)
       LIMIT 2000`);

    if (!rows.length) return res.json({ ok: true, actualizados: 0, msg: 'No hay XMLs pendientes de re-parsear' });

    // Parser XML minimalista — extrae atributos del nodo raíz cfdi:Comprobante
    // y del nodo tfd:TimbreFiscalDigital sin dependencias externas
    function parseAttr(xml, attr) {
      const re = new RegExp(`\\b${attr}="([^"]*)"`, 'i');
      const m = xml.match(re);
      return m ? m[1] : null;
    }

    let actualizados = 0;
    const errores = [];

    for (const row of rows) {
      try {
        const xml = row.xml_content;
        const metodo_pago      = parseAttr(xml, 'MetodoPago');
        const forma_pago       = parseAttr(xml, 'FormaPago');
        const lugar_expedicion = parseAttr(xml, 'LugarExpedicion');
        const serie            = parseAttr(xml, 'Serie');
        const folio            = parseAttr(xml, 'Folio');
        const no_certificado   = parseAttr(xml, 'NoCertificado');
        const version          = parseAttr(xml, 'Version');
        // Timbre Fiscal Digital
        const fecha_timbrado_str = parseAttr(xml, 'FechaTimbrado');
        const rfc_prov_certif    = parseAttr(xml, 'RfcProvCertif');

        await QR(req,
          `UPDATE sat_cfdis SET
            metodo_pago      = COALESCE(metodo_pago,      $1),
            forma_pago       = COALESCE(forma_pago,       $2),
            lugar_expedicion = COALESCE(lugar_expedicion, $3),
            serie            = COALESCE(serie,            $4),
            folio            = COALESCE(folio,            $5),
            no_certificado   = COALESCE(no_certificado,   $6),
            version          = COALESCE(version,          $7),
            fecha_timbrado   = COALESCE(fecha_timbrado,   $8::timestamp),
            rfc_prov_certif  = COALESCE(rfc_prov_certif,  $9),
            updated_at       = NOW()
           WHERE id = $10`,
          [ metodo_pago, forma_pago, lugar_expedicion, serie, folio,
            no_certificado, version,
            fecha_timbrado_str || null,
            rfc_prov_certif, row.id ]);
        actualizados++;
      } catch(e) {
        errores.push({ uuid: row.uuid, error: e.message });
      }
    }
    res.json({ ok: true, total: rows.length, actualizados, errores: errores.slice(0,10) });
  } catch(e) {
    console.error('reparsear-xmls:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});



// ════════════════════════════════════════════════════════════════
// SOLICITUDES DE AUTORIZACIÓN — Envío de cotizaciones
// ════════════════════════════════════════════════════════════════


// ════════════════════════════════════════════════════════════════
// SOLICITUDES DE AUTORIZACIÓN — Envío de cotizaciones
// ════════════════════════════════════════════════════════════════

// Crear solicitud de autorización (cualquier usuario)
app.post('/api/autorizaciones', auth, async (req, res) => {
  try {
    const { tipo='envio_cotizacion', referencia_id, referencia_num,
            destinatario_email, destinatario_cc, asunto, mensaje } = req.body;
    await QR(req, `CREATE TABLE IF NOT EXISTS solicitudes_autorizacion (
      id SERIAL PRIMARY KEY, tipo VARCHAR(30) DEFAULT 'envio_cotizacion',
      referencia_id INTEGER, referencia_num TEXT,
      solicitante_id INTEGER, solicitante_nombre TEXT,
      destinatario_email TEXT, destinatario_cc TEXT, asunto TEXT, mensaje TEXT,
      estatus VARCHAR(20) DEFAULT 'pendiente', autorizado_por INTEGER,
      fecha_solicitud TIMESTAMP DEFAULT NOW(), fecha_resolucion TIMESTAMP,
      notas_autorizador TEXT)`).catch(()=>{});
    const rows = await QR(req, `INSERT INTO solicitudes_autorizacion
      (tipo,referencia_id,referencia_num,solicitante_id,solicitante_nombre,
       destinatario_email,destinatario_cc,asunto,mensaje)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
      [tipo, referencia_id||null, referencia_num||null,
       req.user.id, req.user.nombre||req.user.username,
       destinatario_email||null, destinatario_cc||null, asunto||null, mensaje||null]);
    res.json({ ok: true, id: rows[0]?.id });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Listar solicitudes (admin/gerencia)
app.get('/api/autorizaciones', auth, async (req, res) => {
  try {
    if (!['admin','gerencia'].includes(req.user.rol))
      return res.status(403).json({ error: 'Sin permisos' });
    await QR(req, `CREATE TABLE IF NOT EXISTS solicitudes_autorizacion (
      id SERIAL PRIMARY KEY, tipo VARCHAR(30) DEFAULT 'envio_cotizacion',
      referencia_id INTEGER, referencia_num TEXT, solicitante_id INTEGER,
      solicitante_nombre TEXT, destinatario_email TEXT, destinatario_cc TEXT,
      asunto TEXT, mensaje TEXT, estatus VARCHAR(20) DEFAULT 'pendiente',
      autorizado_por INTEGER, fecha_solicitud TIMESTAMP DEFAULT NOW(),
      fecha_resolucion TIMESTAMP, notas_autorizador TEXT)`).catch(()=>{});
    const estatus = req.query.estatus || 'pendiente';
    const rows = await QR(req, `SELECT * FROM solicitudes_autorizacion
      WHERE estatus=$1 ORDER BY fecha_solicitud DESC LIMIT 100`, [estatus]);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// Mis solicitudes (cualquier usuario ve las suyas)
app.get('/api/autorizaciones/mis-solicitudes', auth, async (req, res) => {
  try {
    await QR(req, `CREATE TABLE IF NOT EXISTS solicitudes_autorizacion (
      id SERIAL PRIMARY KEY, tipo VARCHAR(30), referencia_id INTEGER,
      referencia_num TEXT, solicitante_id INTEGER, solicitante_nombre TEXT,
      destinatario_email TEXT, destinatario_cc TEXT, asunto TEXT, mensaje TEXT,
      estatus VARCHAR(20) DEFAULT 'pendiente', autorizado_por INTEGER,
      fecha_solicitud TIMESTAMP DEFAULT NOW(), fecha_resolucion TIMESTAMP,
      notas_autorizador TEXT)`).catch(()=>{});
    const rows = await QR(req,
      `SELECT id,tipo,referencia_id,referencia_num,destinatario_email,
              asunto,estatus,notas_autorizador,fecha_solicitud,fecha_resolucion
       FROM solicitudes_autorizacion
       WHERE solicitante_id=$1
       ORDER BY fecha_solicitud DESC LIMIT 20`,
      [req.user.id]);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Contar pendientes para badge
app.get('/api/autorizaciones/pendientes-count', auth, async (req, res) => {
  try {
    if (!['admin','gerencia'].includes(req.user.rol)) return res.json({ count: 0 });
    await QR(req, `CREATE TABLE IF NOT EXISTS solicitudes_autorizacion (
      id SERIAL PRIMARY KEY, tipo VARCHAR(30), referencia_id INTEGER,
      referencia_num TEXT, solicitante_id INTEGER, solicitante_nombre TEXT,
      destinatario_email TEXT, destinatario_cc TEXT, asunto TEXT, mensaje TEXT,
      estatus VARCHAR(20) DEFAULT 'pendiente', autorizado_por INTEGER,
      fecha_solicitud TIMESTAMP DEFAULT NOW(), fecha_resolucion TIMESTAMP,
      notas_autorizador TEXT)`).catch(()=>{});
    const rows = await QR(req, `SELECT COUNT(*) as count FROM solicitudes_autorizacion WHERE estatus='pendiente'`);
    res.json({ count: parseInt(rows[0]?.count)||0 });
  } catch(e) { res.json({ count: 0 }); }
});


// Actualizar datos del correo antes de aprobar
app.post('/api/autorizaciones/:id/actualizar', auth, async (req, res) => {
  try {
    if (!['admin','gerencia'].includes(req.user.rol))
      return res.status(403).json({ error: 'Sin permisos' });
    const { destinatario_email, destinatario_cc, asunto, mensaje } = req.body;
    await QR(req, `UPDATE solicitudes_autorizacion SET
      destinatario_email=COALESCE($1,destinatario_email),
      destinatario_cc=$2,
      asunto=COALESCE($3,asunto),
      mensaje=COALESCE($4,mensaje)
      WHERE id=$5`,
      [destinatario_email||null, destinatario_cc||null,
       asunto||null, mensaje||null, req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Aprobar → envía el correo de cotización
app.post('/api/autorizaciones/:id/aprobar', auth, async (req, res) => {
  try {
    if (!['admin','gerencia'].includes(req.user.rol))
      return res.status(403).json({ error: 'Sin permisos' });
    const rows = await QR(req, `SELECT * FROM solicitudes_autorizacion WHERE id=$1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Solicitud no encontrada' });
    const sol = rows[0];
    if (sol.estatus !== 'pendiente') return res.status(400).json({ error: 'Ya fue procesada' });
    await QR(req, `UPDATE solicitudes_autorizacion SET estatus='aprobado',
      autorizado_por=$1, fecha_resolucion=NOW(), notas_autorizador=$2 WHERE id=$3`,
      [req.user.id, req.body.notas||null, req.params.id]);
    // Enviar el correo usando la ruta interna de cotizaciones
    if (sol.tipo === 'envio_cotizacion' && sol.referencia_id) {
      const http = require('http');
      const token = req.headers.authorization;
      const emailData = JSON.stringify({ to: sol.destinatario_email,
        cc: sol.destinatario_cc||undefined, asunto: sol.asunto, mensaje: sol.mensaje });
      await new Promise((resolve) => {
        const opts = { hostname:'127.0.0.1', port: parseInt(process.env.PORT)||3000,
          path:`/api/cotizaciones/${sol.referencia_id}/email`, method:'POST',
          headers:{'Content-Type':'application/json','Authorization':token,
          'Content-Length':Buffer.byteLength(emailData)} };
        const r2 = http.request(opts, (r)=>{ r.resume(); resolve(); });
        r2.on('error', ()=>resolve());
        r2.write(emailData); r2.end();
      });
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Rechazar solicitud
app.post('/api/autorizaciones/:id/rechazar', auth, async (req, res) => {
  try {
    if (!['admin','gerencia'].includes(req.user.rol))
      return res.status(403).json({ error: 'Sin permisos' });
    await QR(req, `UPDATE solicitudes_autorizacion SET estatus='rechazado',
      autorizado_por=$1, fecha_resolucion=NOW(), notas_autorizador=$2 WHERE id=$3`,
      [req.user.id, req.body.notas||null, req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Borrar un CFDI individual ────────────────────────────────────
app.delete('/api/sat/cfdis/:uuid', auth, async (req, res) => {
  try {
    await QR(req, `DELETE FROM sat_cfdis WHERE uuid=$1`, [req.params.uuid]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Borrar TODOS los CFDIs de la empresa ─────────────────────────
app.delete('/api/sat/cfdis', auth, async (req, res) => {
  try {
    const r = await QR(req, `DELETE FROM sat_cfdis`);
    // También limpiar paquetes de las solicitudes
    await QR(req, `UPDATE sat_solicitudes SET estatus='pendiente', paquetes=NULL`).catch(()=>{});
    res.json({ ok: true, eliminados: r?.rowCount ?? 0 });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Borrar una solicitud individual ──────────────────────────────
app.delete('/api/sat/solicitudes/:id', auth, async (req, res) => {
  try {
    await QR(req, `DELETE FROM sat_solicitudes WHERE id_solicitud=$1`, [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Borrar TODO el historial de solicitudes ───────────────────────
app.delete('/api/sat/solicitudes', auth, async (req, res) => {
  try {
    const r = await QR(req, `DELETE FROM sat_solicitudes`);
    res.json({ ok: true, eliminados: r?.rowCount ?? 0 });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Auto-iniciar microservicio SAT Python ──────────────────────
// ── SAT Python auto-start ────────────────────────────────────────
global._satPythonOk = false;

function checkSatHealth(cb, retries=0) {
  const http = require('http');
  const req = http.request(
    { hostname:'127.0.0.1', port:5050, path:'/health', method:'GET', timeout:3000 },
    (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => { global._satPythonOk = true; if(cb) cb(true); });
    }
  );
  req.on('error', () => {
    global._satPythonOk = false;
    if (retries < 5) {  // hasta 5 reintentos = ~10 segundos
      setTimeout(() => checkSatHealth(cb, retries+1), 2000);
    } else {
      if(cb) cb(false);
    }
  });
  req.on('timeout', () => {
    req.destroy();
    global._satPythonOk = false;
    if (retries < 5) {
      setTimeout(() => checkSatHealth(cb, retries+1), 2000);
    } else {
      if(cb) cb(false);
    }
  });
  req.end();
}

function startSatService() {
  const { spawn }  = require('child_process');
  const path       = require('path');
  const fs         = require('fs');
  const satScript  = path.join(__dirname, 'sat_service', 'sat_api.py');
  const isWin      = process.platform === 'win32';

  if (!fs.existsSync(satScript)) {
    console.warn('⚠️  SAT: No se encontró sat_service/sat_api.py');
    return;
  }
  if (global._satStarting) return;   // evitar arranques dobles
  global._satStarting = true;

  // En Windows usar shell:true + cmd /c para que funcione con cualquier PATH
  const launchCmd  = isWin ? 'cmd'   : null;
  const launchArgs = isWin
    ? ['/c', 'python', satScript]    // cmd /c python sat_api.py
    : null;

  const candidates = isWin
    ? [
        { cmd:'cmd', args:['/c','python',  satScript] },
        { cmd:'cmd', args:['/c','python3', satScript] },
        { cmd:'cmd', args:['/c','py',      satScript] },
      ]
    : [
        { cmd:'python3', args:[satScript] },
        { cmd:'python',  args:[satScript] },
      ];

  let attemptIdx = 0;

  function tryLaunch() {
    if (attemptIdx >= candidates.length) {
      console.warn('⚠️  SAT: No se encontró Python. Instala desde https://python.org');
      console.warn('   Luego ejecuta manualmente: python sat_service\\sat_api.py');
      global._satStarting = false;
      return;
    }
    const { cmd, args } = candidates[attemptIdx++];
    console.log('🐍 SAT: probando', cmd, args.join(' '));

    const opts = {
      stdio : ['ignore', 'pipe', 'pipe'],
      cwd   : __dirname,
      shell : isWin,          // shell:true en Windows = usa PATH del sistema
      windowsHide: false,     // visible para depuración
    };
    if (!isWin) opts.detached = true;

    let proc;
    try { proc = spawn(cmd, args, opts); }
    catch(e) { console.warn('  spawn error:', e.message); tryLaunch(); return; }

    let started = false;
    let errBuf  = '';

    proc.stdout.on('data', d => {
      const t = d.toString().trim();
      if (!t || t.includes('WARNING')) return;
      console.log('  SAT:', t);
      if (t.includes('5050') || t.includes('Running')) started = true;
    });

    proc.stderr.on('data', d => {
      const t = d.toString().trim();
      if (!t) return;
      // Werkzeug log normal (no es error)
      if (t.includes('Running on') || t.includes('werkzeug') || t.includes('WARNING')) {
        console.log('  SAT:', t.slice(0,100));
        started = true;
        return;
      }
      // Error real de Python (no encontrado)
      if (t.includes('not recognized') || t.includes('not found') || t.includes('No such file')) {
        tryLaunch(); return;
      }
      if (t.includes('Address already')) {
        console.log('  SAT: puerto 5050 ya en uso — OK');
        started = true;
        return;
      }
      errBuf += t + ' ';
      console.log('  SAT err:', t.slice(0,120));
    });

    proc.on('error', (e) => {
      // Error al ejecutar el comando — probar siguiente
      tryLaunch();
    });

    proc.on('exit', (code) => {
      global._satStarting = false;
      if (!started && code !== 0) {
        console.warn('  SAT terminó código', code, errBuf.slice(0,100));
        if (attemptIdx < candidates.length) tryLaunch();
        else global._satPythonOk = false;
      }
    });

    if (!isWin) proc.unref();

    // Verificar después de 4 segundos que responde
    setTimeout(() => {
      checkSatHealth((ok) => {
        global._satStarting = false;
        if (ok) {
          console.log('  ✅ SAT Python corriendo correctamente en puerto 5050');
          global._satPythonOk = true;
        } else {
          console.warn('  ⚠️  SAT no respondió. Revisa: python sat_service/sat_api.py');
          // Intentar siguiente candidato si este no funcionó
          if (!started && attemptIdx < candidates.length) tryLaunch();
        }
      });
    }, 4000);
  }

  console.log('🐍 Iniciando SAT Python (puerto 5050)...');
  tryLaunch();
}

// Verificar si ya está corriendo; si no, iniciar
checkSatHealth(function(running) {
  if (running) {
    console.log('✅ SAT Python ya estaba corriendo en puerto 5050');
  } else {
    startSatService();
  }
});
// ================================================================
// RFQ — Solicitudes de Cotización a Proveedores
// ================================================================
// Asegurar tabla rfq en autoSetup (se agrega como migración)
async function ensureRfqTable(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS rfq (
      id SERIAL PRIMARY KEY,
      numero_rfq VARCHAR(50),
      descripcion TEXT NOT NULL,
      proyecto_nombre VARCHAR(200),
      prioridad VARCHAR(20) DEFAULT 'media',
      fecha_limite DATE,
      presupuesto_max NUMERIC(15,2),
      moneda VARCHAR(10) DEFAULT 'MXN',
      condiciones_pago TEXT,
      lugar_entrega TEXT,
      criterios_eval TEXT,
      notas TEXT,
      terminos TEXT,
      estatus VARCHAR(30) DEFAULT 'borrador',
      proveedor_ids JSONB DEFAULT '[]',
      items JSONB DEFAULT '[]',
      created_by INTEGER,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`);
  } finally { client.release(); }
}

app.get('/api/rfq', auth, licencia, async (req, res) => {
  try {
    await ensureRfqTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT r.*, u.nombre creador_nombre
      FROM rfq r
      LEFT JOIN public.usuarios u ON u.id = r.created_by
      ORDER BY r.created_at DESC`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/rfq/:id', auth, licencia, async (req, res) => {
  try {
    await ensureRfqTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, 'SELECT * FROM rfq WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'RFQ no encontrada' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/rfq', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    await ensureRfqTable(schema);
    const {
      descripcion, proyecto_nombre, prioridad, fecha_limite,
      presupuesto_max, moneda, condiciones_pago, lugar_entrega,
      criterios_eval, notas, terminos, proveedor_ids, items, estatus
    } = req.body;
    if (!descripcion) return res.status(400).json({ error: 'Descripción requerida' });

    const yr  = new Date().getFullYear();
    const cnt = await QR(req, "SELECT COUNT(*) c FROM rfq WHERE fecha_limite::text LIKE $1 OR created_at::text LIKE $1", [`${yr}%`]);
    const num = `RFQ-${yr}-${String(parseInt(cnt[0]?.c || 0) + 1).padStart(3, '0')}`;

    const rows = await QR(req, `
      INSERT INTO rfq (numero_rfq,descripcion,proyecto_nombre,prioridad,fecha_limite,
        presupuesto_max,moneda,condiciones_pago,lugar_entrega,criterios_eval,notas,
        terminos,proveedor_ids,items,estatus,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING *`,
      [num, descripcion, proyecto_nombre||null, prioridad||'media', fecha_limite||null,
       parseFloat(presupuesto_max)||null, moneda||'MXN', condiciones_pago||null,
       lugar_entrega||null, criterios_eval||null, notas||null, terminos||null,
       JSON.stringify(proveedor_ids||[]), JSON.stringify(items||[]),
       estatus||'borrador', req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { console.error('RFQ POST:', e.message); res.status(500).json({ error: e.message }); }
});

app.put('/api/rfq/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureRfqTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const {
      descripcion, proyecto_nombre, prioridad, fecha_limite, presupuesto_max,
      moneda, condiciones_pago, lugar_entrega, criterios_eval, notas,
      terminos, proveedor_ids, items, estatus
    } = req.body;
    const rows = await QR(req, `
      UPDATE rfq SET
        descripcion=$1, proyecto_nombre=$2, prioridad=$3, fecha_limite=$4,
        presupuesto_max=$5, moneda=$6, condiciones_pago=$7, lugar_entrega=$8,
        criterios_eval=$9, notas=$10, terminos=$11,
        proveedor_ids=$12, items=$13, estatus=$14, updated_at=NOW()
      WHERE id=$15 RETURNING *`,
      [descripcion, proyecto_nombre||null, prioridad||'media', fecha_limite||null,
       parseFloat(presupuesto_max)||null, moneda||'MXN', condiciones_pago||null,
       lugar_entrega||null, criterios_eval||null, notas||null, terminos||null,
       JSON.stringify(proveedor_ids||[]), JSON.stringify(items||[]),
       estatus||'borrador', req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrada' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Cambiar solo el estatus (ej. borrador → enviada)
app.put('/api/rfq/:id/estatus', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureRfqTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { estatus } = req.body;
    if (!estatus) return res.status(400).json({ error: 'estatus requerido' });
    const rows = await QR(req,
      'UPDATE rfq SET estatus=$1, updated_at=NOW() WHERE id=$2 RETURNING *',
      [estatus, req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrada' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/rfq/:id', auth, empresaActiva, licencia, adminOnly, async (req, res) => {
  try {
    await QR(req, 'DELETE FROM rfq WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ================================================================
// PRODUCCIÓN / MRP
// ================================================================
async function ensureProduccionTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS produccion_ordenes (
      id SERIAL PRIMARY KEY,
      numero_op VARCHAR(50),
      producto_id INTEGER,
      producto_nombre TEXT,
      unidad VARCHAR(20) DEFAULT 'pza',
      cantidad_planificada NUMERIC(10,2) DEFAULT 0,
      cantidad_producida NUMERIC(10,2) DEFAULT 0,
      prioridad VARCHAR(20) DEFAULT 'normal',
      responsable TEXT,
      fecha_inicio DATE,
      fecha_fin_plan DATE,
      fecha_inicio_real DATE,
      fecha_fin_real DATE,
      bom JSONB DEFAULT '[]',
      notas TEXT,
      estatus VARCHAR(30) DEFAULT 'borrador',
      created_by INTEGER,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`);
  } finally { client.release(); }
}

app.post('/api/produccion/init', auth, async (req, res) => {
  try {
    await ensureProduccionTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false }); }
});

app.get('/api/produccion/ordenes', auth, licencia, async (req, res) => {
  try {
    await ensureProduccionTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT po.*, i.nombre producto_nombre, i.unidad
      FROM produccion_ordenes po
      LEFT JOIN inventario i ON i.id = po.producto_id
      ORDER BY po.created_at DESC`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/produccion/ordenes/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, `
      SELECT po.*, i.nombre producto_nombre, i.unidad
      FROM produccion_ordenes po LEFT JOIN inventario i ON i.id=po.producto_id
      WHERE po.id=$1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrada' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/produccion/ordenes', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureProduccionTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { producto_id, cantidad_planificada, prioridad, responsable,
            fecha_inicio, fecha_fin_plan, notas, estatus } = req.body;
    if (!producto_id) return res.status(400).json({ error: 'producto_id requerido' });
    const yr = new Date().getFullYear();
    const cnt = (await QR(req, "SELECT COUNT(*) c FROM produccion_ordenes"))[0]?.c || 0;
    const num = `OP-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    // Auto-populate nombre from inventario
    const prod = await QR(req, 'SELECT nombre, unidad FROM inventario WHERE id=$1', [producto_id]);
    const rows = await QR(req, `
      INSERT INTO produccion_ordenes (numero_op, producto_id, producto_nombre, unidad,
        cantidad_planificada, prioridad, responsable, fecha_inicio, fecha_fin_plan, notas, estatus, created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
      [num, producto_id, prod[0]?.nombre||null, prod[0]?.unidad||'pza',
       parseFloat(cantidad_planificada)||1, prioridad||'normal', responsable||null,
       fecha_inicio||null, fecha_fin_plan||null, notas||null, estatus||'planificada', req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/produccion/ordenes/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const sets = []; const vals = []; let i = 1;
    const add = (k,v) => { sets.push(`${k}=$${i++}`); vals.push(v); };
    const b = req.body;
    if(b.estatus !== undefined) add('estatus', b.estatus);
    if(b.cantidad_producida !== undefined) add('cantidad_producida', parseFloat(b.cantidad_producida)||0);
    if(b.fecha_inicio_real !== undefined) add('fecha_inicio_real', b.fecha_inicio_real);
    if(b.fecha_fin_real !== undefined) add('fecha_fin_real', b.fecha_fin_real);
    if(b.notas !== undefined) add('notas', b.notas);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req, `UPDATE produccion_ordenes SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const rows = await QR(req, 'SELECT * FROM produccion_ordenes WHERE id=$1', [req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/produccion/ordenes/:id/registrar', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const { cantidad } = req.body;
    const rows = await QR(req,
      'UPDATE produccion_ordenes SET cantidad_producida=COALESCE(cantidad_producida,0)+$1, updated_at=NOW() WHERE id=$2 RETURNING *',
      [parseFloat(cantidad)||0, req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/produccion/mrp', auth, licencia, async (req, res) => {
  try {
    const dias = parseInt(req.query.dias) || 30;
    const fechaLimite = new Date(); fechaLimite.setDate(fechaLimite.getDate() + dias);
    const ordenes = await QR(req, `
      SELECT po.*, i.nombre producto_nombre
      FROM produccion_ordenes po LEFT JOIN inventario i ON i.id=po.producto_id
      WHERE po.estatus IN ('planificada','en_proceso')
        AND (po.fecha_fin_plan IS NULL OR po.fecha_fin_plan <= $1)`,
      [fechaLimite.toISOString().slice(0,10)]);
    // Simplified MRP: check inventory vs requirements from BOM
    const faltantes = [];
    for (const op of ordenes) {
      let bom = [];
      try { bom = Array.isArray(op.bom) ? op.bom : JSON.parse(op.bom||'[]'); } catch {}
      for (const mat of bom) {
        const requerido = (parseFloat(mat.cantidad)||0) * (parseFloat(op.cantidad_planificada)||0);
        const inv = await QR(req,
          "SELECT COALESCE(cantidad_actual, stock_actual, 0) qty, nombre, unidad FROM inventario WHERE id=$1",
          [mat.producto_id]).catch(()=>[]);
        const disponible = parseFloat(inv[0]?.qty || 0);
        if (disponible < requerido) {
          faltantes.push({
            nombre: mat.nombre || inv[0]?.nombre || 'Material '+mat.producto_id,
            unidad: inv[0]?.unidad || mat.unidad || 'pza',
            requerido: requerido.toFixed(2),
            disponible: disponible.toFixed(2),
            faltante: (requerido - disponible).toFixed(2),
          });
        }
      }
    }
    res.json({ ordenes_analizadas: ordenes.length, faltantes });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// MANTENIMIENTO
// ================================================================
async function ensureMantenimientoTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS mant_activos (
      id SERIAL PRIMARY KEY, nombre TEXT NOT NULL, tipo VARCHAR(100),
      ubicacion TEXT, modelo TEXT, serie TEXT, marca TEXT,
      fecha_adquisicion DATE, proxima_revision DATE,
      activo BOOLEAN DEFAULT true, notas TEXT,
      created_at TIMESTAMP DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS mant_ordenes (
      id SERIAL PRIMARY KEY, numero_ot VARCHAR(50),
      activo_id INTEGER, tipo VARCHAR(30) DEFAULT 'correctivo',
      prioridad VARCHAR(20) DEFAULT 'media',
      descripcion TEXT, tecnico TEXT,
      fecha_plan DATE, fecha_inicio_real DATE, fecha_cierre DATE,
      tiempo_estimado_hrs NUMERIC(6,2), tiempo_real_hrs NUMERIC(6,2),
      resumen_trabajo TEXT,
      estatus VARCHAR(30) DEFAULT 'abierta',
      created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.post('/api/mantenimiento/init', auth, async (req, res) => {
  try { await ensureMantenimientoTables(req.user?.schema || global._defaultSchema || 'emp_vef'); res.json({ ok: true }); }
  catch(e) { res.json({ ok: false }); }
});

app.get('/api/mantenimiento/activos', auth, licencia, async (req, res) => {
  try {
    await ensureMantenimientoTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json(await QR(req, 'SELECT * FROM mant_activos ORDER BY nombre'));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/mantenimiento/activos', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureMantenimientoTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { nombre, tipo, ubicacion, modelo, serie, marca, notas } = req.body;
    if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
    const rows = await QR(req,
      'INSERT INTO mant_activos (nombre,tipo,ubicacion,modelo,serie,marca,notas) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *',
      [nombre, tipo||null, ubicacion||null, modelo||null, serie||null, marca||null, notas||null]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/mantenimiento/ordenes', auth, licencia, async (req, res) => {
  try {
    await ensureMantenimientoTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json(await QR(req, `
      SELECT mo.*, a.nombre activo_nombre
      FROM mant_ordenes mo LEFT JOIN mant_activos a ON a.id=mo.activo_id
      ORDER BY mo.created_at DESC`));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/mantenimiento/ordenes/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, `
      SELECT mo.*, a.nombre activo_nombre
      FROM mant_ordenes mo LEFT JOIN mant_activos a ON a.id=mo.activo_id
      WHERE mo.id=$1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrada' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/mantenimiento/ordenes', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureMantenimientoTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { activo_id, tipo, prioridad, descripcion, tecnico, fecha_plan, tiempo_estimado_hrs, estatus } = req.body;
    const cnt = (await QR(req, 'SELECT COUNT(*) c FROM mant_ordenes'))[0]?.c || 0;
    const num = `OT-${new Date().getFullYear()}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const rows = await QR(req,
      `INSERT INTO mant_ordenes (numero_ot,activo_id,tipo,prioridad,descripcion,tecnico,fecha_plan,tiempo_estimado_hrs,estatus,created_by)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [num, activo_id||null, tipo||'correctivo', prioridad||'media', descripcion||null,
       tecnico||null, fecha_plan||null, tiempo_estimado_hrs||null, estatus||'abierta', req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/mantenimiento/ordenes/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const b = req.body; const sets = []; const vals = []; let i = 1;
    const add = (k,v) => { sets.push(`${k}=$${i++}`); vals.push(v); };
    if(b.estatus           !== undefined) add('estatus',            b.estatus);
    if(b.fecha_inicio_real !== undefined) add('fecha_inicio_real',  b.fecha_inicio_real);
    if(b.fecha_cierre      !== undefined) add('fecha_cierre',       b.fecha_cierre);
    if(b.resumen_trabajo   !== undefined) add('resumen_trabajo',    b.resumen_trabajo);
    if(b.tiempo_real_hrs   !== undefined) add('tiempo_real_hrs',    parseFloat(b.tiempo_real_hrs)||null);
    vals.push(req.params.id);
    await QR(req, `UPDATE mant_ordenes SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const rows = await QR(req, 'SELECT * FROM mant_ordenes WHERE id=$1', [req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// SERVICIO TÉCNICO — Tickets
// ================================================================
async function ensureServicioTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS serv_tickets (
      id SERIAL PRIMARY KEY, titulo TEXT NOT NULL,
      cliente_id INTEGER, prioridad VARCHAR(20) DEFAULT 'medio',
      tecnico TEXT, sla_horas INTEGER,
      descripcion TEXT, resolucion TEXT,
      estatus VARCHAR(30) DEFAULT 'nuevo',
      notas JSONB DEFAULT '[]',
      created_by INTEGER, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.post('/api/servicio/init', auth, async (req, res) => {
  try { await ensureServicioTables(req.user?.schema || global._defaultSchema || 'emp_vef'); res.json({ ok: true }); }
  catch(e) { res.json({ ok: false }); }
});

app.get('/api/servicio/tickets', auth, licencia, async (req, res) => {
  try {
    await ensureServicioTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json(await QR(req, `
      SELECT st.*, cl.nombre cliente_nombre
      FROM serv_tickets st LEFT JOIN clientes cl ON cl.id=st.cliente_id
      ORDER BY CASE prioridad WHEN 'critico' THEN 1 WHEN 'alto' THEN 2 WHEN 'medio' THEN 3 ELSE 4 END,
               st.created_at DESC`));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/servicio/tickets/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, `
      SELECT st.*, cl.nombre cliente_nombre
      FROM serv_tickets st LEFT JOIN clientes cl ON cl.id=st.cliente_id
      WHERE st.id=$1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    const t = rows[0];
    let notas = []; try { notas = Array.isArray(t.notas) ? t.notas : JSON.parse(t.notas||'[]'); } catch {}
    res.json({ ...t, notas });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servicio/tickets', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureServicioTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { titulo, cliente_id, prioridad, tecnico, sla_horas, descripcion, estatus } = req.body;
    if (!titulo) return res.status(400).json({ error: 'título requerido' });
    const rows = await QR(req,
      `INSERT INTO serv_tickets (titulo,cliente_id,prioridad,tecnico,sla_horas,descripcion,estatus,created_by)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [titulo, cliente_id||null, prioridad||'medio', tecnico||null,
       sla_horas||null, descripcion||null, estatus||'nuevo', req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/servicio/tickets/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const b = req.body; const sets = []; const vals = []; let i = 1;
    const add = (k,v) => { sets.push(`${k}=$${i++}`); vals.push(v); };
    if(b.estatus    !== undefined) add('estatus',    b.estatus);
    if(b.tecnico    !== undefined) add('tecnico',    b.tecnico);
    if(b.resolucion !== undefined) add('resolucion', b.resolucion);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req, `UPDATE serv_tickets SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const rows = await QR(req, 'SELECT * FROM serv_tickets WHERE id=$1', [req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servicio/tickets/:id/nota', auth, async (req, res) => {
  try {
    const { nota } = req.body;
    if (!nota) return res.status(400).json({ error: 'nota requerida' });
    const rows = await QR(req, 'SELECT notas FROM serv_tickets WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    let notas = []; try { notas = Array.isArray(rows[0].notas) ? rows[0].notas : JSON.parse(rows[0].notas||'[]'); } catch {}
    notas.push({ nota, autor: req.user.nombre||req.user.username, fecha: new Date().toISOString() });
    await QR(req, 'UPDATE serv_tickets SET notas=$1, updated_at=NOW() WHERE id=$2',
      [JSON.stringify(notas), req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// CATÁLOGOS / ATRIBUTOS PERSONALIZADOS
// ================================================================
async function ensureCatalogosTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS cat_catalogos (
      id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
      tipo VARCHAR(30) DEFAULT 'lista',
      items JSONB DEFAULT '[]',
      created_at TIMESTAMP DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS cat_atributos (
      id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
      entidad VARCHAR(50) NOT NULL, tipo VARCHAR(30) DEFAULT 'texto',
      opciones TEXT, default_val TEXT, descripcion TEXT,
      requerido BOOLEAN DEFAULT false, orden INTEGER DEFAULT 0,
      activo BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.post('/api/catalogos/init', auth, async (req, res) => {
  try { await ensureCatalogosTables(req.user?.schema || global._defaultSchema || 'emp_vef'); res.json({ ok: true }); }
  catch(e) { res.json({ ok: false }); }
});

app.get('/api/catalogos', auth, licencia, async (req, res) => {
  try {
    await ensureCatalogosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT id, nombre, tipo,
        jsonb_array_length(COALESCE(items,'[]'::jsonb)) items_count
      FROM cat_catalogos ORDER BY nombre`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/catalogos/atributos', auth, licencia, async (req, res) => {
  try {
    await ensureCatalogosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json(await QR(req, 'SELECT * FROM cat_atributos WHERE activo=true ORDER BY entidad, orden, nombre'));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/catalogos/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, 'SELECT * FROM cat_catalogos WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    const cat = rows[0];
    let items = []; try { items = Array.isArray(cat.items) ? cat.items : JSON.parse(cat.items||'[]'); } catch {}
    res.json({ ...cat, items });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/catalogos', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureCatalogosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { nombre, tipo, items } = req.body;
    if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
    const itemsArr = (items||[]).map(v => ({ valor: typeof v === 'string' ? v : v.valor || v }));
    const rows = await QR(req,
      'INSERT INTO cat_catalogos (nombre, tipo, items) VALUES ($1,$2,$3) RETURNING *',
      [nombre, tipo||'lista', JSON.stringify(itemsArr)]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/catalogos/:id/items', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const { valor } = req.body;
    const rows = await QR(req, 'SELECT items FROM cat_catalogos WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    let items = []; try { items = Array.isArray(rows[0].items) ? rows[0].items : JSON.parse(rows[0].items||'[]'); } catch {}
    items.push({ valor });
    await QR(req, 'UPDATE cat_catalogos SET items=$1 WHERE id=$2', [JSON.stringify(items), req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/catalogos/:id/items/:valor', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const rows = await QR(req, 'SELECT items FROM cat_catalogos WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    let items = []; try { items = Array.isArray(rows[0].items) ? rows[0].items : JSON.parse(rows[0].items||'[]'); } catch {}
    items = items.filter(it => (it.valor||it) !== decodeURIComponent(req.params.valor));
    await QR(req, 'UPDATE cat_catalogos SET items=$1 WHERE id=$2', [JSON.stringify(items), req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/catalogos/atributos', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureCatalogosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { nombre, entidad, tipo, opciones, default_val, descripcion, requerido } = req.body;
    if (!nombre || !entidad) return res.status(400).json({ error: 'nombre y entidad requeridos' });
    const rows = await QR(req,
      `INSERT INTO cat_atributos (nombre,entidad,tipo,opciones,default_val,descripcion,requerido)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [nombre, entidad, tipo||'texto', opciones||null, default_val||null, descripcion||null, requerido||false]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/catalogos/atributos/:id', auth, empresaActiva, licencia, adminOnly, async (req, res) => {
  try {
    await QR(req, 'UPDATE cat_atributos SET activo=false WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});



// ================================================================
// RFQ DE PROYECTOS
// ================================================================
async function ensureRFQTable(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS proyectos_rfq (
      id SERIAL PRIMARY KEY,
      numero_rfq VARCHAR(50) UNIQUE,
      proyecto_id INTEGER,
      titulo TEXT NOT NULL,
      descripcion TEXT,
      partidas JSONB DEFAULT '[]',
      proveedores JSONB DEFAULT '[]',
      presupuesto_max NUMERIC(15,2),
      fecha_limite DATE,
      fecha_envio DATE,
      fecha_adjudicacion DATE,
      tiempo_entrega VARCHAR(50),
      condiciones_pago VARCHAR(50),
      lugar_entrega TEXT,
      criterio_evaluacion VARCHAR(50),
      notas_adicionales TEXT,
      proveedor_adjudicado TEXT,
      monto_adjudicado NUMERIC(15,2),
      notas_adjudicacion TEXT,
      estatus VARCHAR(30) DEFAULT 'borrador',
      created_by INTEGER,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.get('/api/proyectos/rfq', auth, licencia, async (req, res) => {
  try {
    await ensureRFQTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT r.*, p.nombre proyecto_nombre
      FROM proyectos_rfq r
      LEFT JOIN proyectos p ON p.id = r.proyecto_id
      ORDER BY r.created_at DESC`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/proyectos/rfq/:id', auth, licencia, async (req, res) => {
  try {
    await ensureRFQTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT r.*, p.nombre proyecto_nombre
      FROM proyectos_rfq r
      LEFT JOIN proyectos p ON p.id = r.proyecto_id
      WHERE r.id = $1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'RFQ no encontrado' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/proyectos/rfq', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureRFQTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const {
      titulo, proyecto_id, descripcion, partidas, proveedores,
      presupuesto_max, fecha_limite, fecha_envio, tiempo_entrega,
      condiciones_pago, lugar_entrega, criterio_evaluacion,
      notas_adicionales, estatus
    } = req.body;
    if (!titulo) return res.status(400).json({ error: 'título requerido' });
    // Generate RFQ number
    const yr  = new Date().getFullYear();
    const cnt = (await QR(req, 'SELECT COUNT(*) c FROM proyectos_rfq'))[0]?.c || 0;
    const num = `RFQ-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const rows = await QR(req, `
      INSERT INTO proyectos_rfq
        (numero_rfq, proyecto_id, titulo, descripcion, partidas, proveedores,
         presupuesto_max, fecha_limite, fecha_envio, tiempo_entrega,
         condiciones_pago, lugar_entrega, criterio_evaluacion,
         notas_adicionales, estatus, created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING *`,
      [num, proyecto_id||null, titulo,
       descripcion||null,
       typeof partidas==='string' ? partidas : JSON.stringify(partidas||[]),
       typeof proveedores==='string' ? proveedores : JSON.stringify(proveedores||[]),
       parseFloat(presupuesto_max)||null, fecha_limite||null,
       estatus==='enviado' ? (fecha_envio||new Date().toISOString().slice(0,10)) : null,
       tiempo_entrega||null, condiciones_pago||null,
       lugar_entrega||null, criterio_evaluacion||null,
       notas_adicionales||null, estatus||'borrador', req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/proyectos/rfq/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureRFQTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const b = req.body;
    const sets = []; const vals = []; let i = 1;
    const add = (k, v) => { sets.push(`${k}=$${i++}`); vals.push(v); };
    if (b.estatus             !== undefined) add('estatus',             b.estatus);
    if (b.proveedores         !== undefined) add('proveedores',         typeof b.proveedores==='string'?b.proveedores:JSON.stringify(b.proveedores));
    if (b.partidas            !== undefined) add('partidas',            typeof b.partidas==='string'?b.partidas:JSON.stringify(b.partidas));
    if (b.fecha_envio         !== undefined) add('fecha_envio',         b.fecha_envio);
    if (b.fecha_adjudicacion  !== undefined) add('fecha_adjudicacion',  b.fecha_adjudicacion);
    if (b.proveedor_adjudicado!== undefined) add('proveedor_adjudicado',b.proveedor_adjudicado);
    if (b.monto_adjudicado    !== undefined) add('monto_adjudicado',    parseFloat(b.monto_adjudicado)||null);
    if (b.notas_adjudicacion  !== undefined) add('notas_adjudicacion',  b.notas_adjudicacion);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req, `UPDATE proyectos_rfq SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const rows = await QR(req, `
      SELECT r.*, p.nombre proyecto_nombre FROM proyectos_rfq r
      LEFT JOIN proyectos p ON p.id=r.proyecto_id WHERE r.id=$1`, [req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/proyectos/rfq/:id', auth, empresaActiva, licencia, adminOnly, async (req, res) => {
  try {
    await QR(req, 'DELETE FROM proyectos_rfq WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Endpoint to get RFQ count per project (used in proyectos list)
app.get('/api/proyectos/:id/rfq-count', auth, licencia, async (req, res) => {
  try {
    await ensureRFQTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req,
      'SELECT COUNT(*) c FROM proyectos_rfq WHERE proyecto_id=$1', [req.params.id]);
    res.json({ count: parseInt(rows[0]?.c||0) });
  } catch(e) { res.json({ count: 0 }); }
});


// ================================================================
// RFP DE PROYECTOS — Request For Proposal
// ================================================================
async function ensureRFPTable(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS proyectos_rfp (
      id SERIAL PRIMARY KEY,
      numero_rfp VARCHAR(50) UNIQUE,
      proyecto_id INTEGER,
      titulo TEXT NOT NULL,
      descripcion TEXT, alcance TEXT,
      entregables JSONB DEFAULT '[]',
      hitos TEXT,
      criterios JSONB DEFAULT '[]',
      propuestas JSONB DEFAULT '[]',
      requisitos_propuesta JSONB DEFAULT '[]',
      presupuesto_max NUMERIC(15,2),
      fecha_limite DATE,
      fecha_inicio_estimada DATE,
      duracion VARCHAR(30),
      metodologia VARCHAR(30),
      modalidad VARCHAR(20),
      condiciones TEXT,
      proveedor_ganador TEXT,
      monto_adjudicado NUMERIC(15,2),
      justificacion_adjudicacion TEXT,
      fecha_adjudicacion DATE,
      estatus VARCHAR(30) DEFAULT 'borrador',
      created_by INTEGER,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.get('/api/proyectos/rfp', auth, licencia, async (req, res) => {
  try {
    await ensureRFPTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT r.*, p.nombre proyecto_nombre
      FROM proyectos_rfp r
      LEFT JOIN proyectos p ON p.id = r.proyecto_id
      ORDER BY r.created_at DESC`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/proyectos/rfp/:id', auth, licencia, async (req, res) => {
  try {
    await ensureRFPTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT r.*, p.nombre proyecto_nombre
      FROM proyectos_rfp r LEFT JOIN proyectos p ON p.id=r.proyecto_id
      WHERE r.id=$1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'RFP no encontrado' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/proyectos/rfp', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureRFPTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { titulo, proyecto_id, descripcion, alcance, entregables, hitos, criterios,
            propuestas, requisitos_propuesta, presupuesto_max, fecha_limite,
            fecha_inicio_estimada, duracion, metodologia, modalidad, condiciones, estatus } = req.body;
    if (!titulo) return res.status(400).json({ error: 'título requerido' });
    const yr  = new Date().getFullYear();
    const cnt = (await QR(req, 'SELECT COUNT(*) c FROM proyectos_rfp'))[0]?.c || 0;
    const num = `RFP-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const toJ = v => typeof v==='string' ? v : JSON.stringify(v||[]);
    const rows = await QR(req, `
      INSERT INTO proyectos_rfp
        (numero_rfp, proyecto_id, titulo, descripcion, alcance, entregables, hitos,
         criterios, propuestas, requisitos_propuesta, presupuesto_max, fecha_limite,
         fecha_inicio_estimada, duracion, metodologia, modalidad, condiciones, estatus, created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19) RETURNING *`,
      [num, proyecto_id||null, titulo, descripcion||null, alcance||null,
       toJ(entregables), hitos||null, toJ(criterios), toJ(propuestas), toJ(requisitos_propuesta),
       parseFloat(presupuesto_max)||null, fecha_limite||null, fecha_inicio_estimada||null,
       duracion||null, metodologia||null, modalidad||null, condiciones||null,
       estatus||'borrador', req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/proyectos/rfp/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureRFPTable(req.user?.schema || global._defaultSchema || 'emp_vef');
    const b = req.body;
    const toJ = v => v===undefined ? undefined : (typeof v==='string' ? v : JSON.stringify(v));
    const sets=[]; const vals=[]; let i=1;
    const add=(k,v)=>{if(v!==undefined){sets.push(`${k}=$${i++}`);vals.push(v);}};
    add('estatus',           b.estatus);
    add('propuestas',        toJ(b.propuestas));
    add('entregables',       toJ(b.entregables));
    add('criterios',         toJ(b.criterios));
    add('proveedor_ganador', b.proveedor_ganador);
    add('monto_adjudicado',  b.monto_adjudicado!==undefined?parseFloat(b.monto_adjudicado)||null:undefined);
    add('justificacion_adjudicacion', b.justificacion_adjudicacion);
    add('fecha_adjudicacion',b.fecha_adjudicacion);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req, `UPDATE proyectos_rfp SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const rows=await QR(req,`SELECT r.*,p.nombre proyecto_nombre FROM proyectos_rfp r LEFT JOIN proyectos p ON p.id=r.proyecto_id WHERE r.id=$1`,[req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/proyectos/rfp/:id', auth, empresaActiva, licencia, adminOnly, async (req,res)=>{
  try{await QR(req,'DELETE FROM proyectos_rfp WHERE id=$1',[req.params.id]);res.json({ok:true});}
  catch(e){res.status(500).json({error:e.message});}
});

// ================================================================
// ALMACÉN — Ubicaciones, Lotes, FIFO
// ================================================================
async function ensureAlmacenTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS alm_ubicaciones (
      id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
      tipo VARCHAR(50) DEFAULT 'General', descripcion TEXT,
      activo BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS alm_lotes (
      id SERIAL PRIMARY KEY,
      numero_lote VARCHAR(100), numero_serie VARCHAR(100),
      producto_id INTEGER NOT NULL,
      ubicacion_id INTEGER,
      cantidad_inicial NUMERIC(12,3) DEFAULT 0,
      cantidad_disponible NUMERIC(12,3) DEFAULT 0,
      costo_unitario NUMERIC(15,4),
      fecha_entrada DATE DEFAULT CURRENT_DATE,
      fecha_caducidad DATE,
      referencia_oc TEXT, notas TEXT,
      activo BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS alm_movimientos (
      id SERIAL PRIMARY KEY,
      lote_id INTEGER NOT NULL,
      producto_id INTEGER,
      ubicacion_id INTEGER,
      tipo VARCHAR(20) NOT NULL,
      cantidad NUMERIC(12,3) NOT NULL,
      costo_unitario NUMERIC(15,4),
      referencia TEXT, notas TEXT,
      created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.post('/api/almacen/init', auth, async (req, res) => {
  try {
    await ensureAlmacenTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, error: e.message }); }
});

app.get('/api/almacen/ubicaciones', auth, licencia, async (req, res) => {
  try {
    await ensureAlmacenTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT u.*, COUNT(l.id) lotes_count
      FROM alm_ubicaciones u
      LEFT JOIN alm_lotes l ON l.ubicacion_id=u.id AND l.cantidad_disponible>0
      WHERE u.activo=true GROUP BY u.id ORDER BY u.nombre`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/almacen/ubicaciones', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureAlmacenTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { nombre, tipo, descripcion } = req.body;
    if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
    const rows = await QR(req,
      'INSERT INTO alm_ubicaciones (nombre,tipo,descripcion) VALUES ($1,$2,$3) RETURNING *',
      [nombre, tipo||'General', descripcion||null]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/almacen/lotes', auth, licencia, async (req, res) => {
  try {
    await ensureAlmacenTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT l.*, i.nombre producto_nombre, i.unidad,
             u.nombre ubicacion_nombre
      FROM alm_lotes l
      LEFT JOIN inventario i ON i.id=l.producto_id
      LEFT JOIN alm_ubicaciones u ON u.id=l.ubicacion_id
      WHERE l.activo=true
      ORDER BY l.fecha_entrada ASC, l.id ASC`);  // ASC = FIFO order
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/almacen/lotes/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, `
      SELECT l.*, i.nombre producto_nombre, i.unidad, u.nombre ubicacion_nombre
      FROM alm_lotes l
      LEFT JOIN inventario i ON i.id=l.producto_id
      LEFT JOIN alm_ubicaciones u ON u.id=l.ubicacion_id
      WHERE l.id=$1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/almacen/lotes', auth, empresaActiva, licencia, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query('BEGIN');
    await ensureAlmacenTables(schema);

    const { producto_id, ubicacion_id, numero_lote, numero_serie,
            cantidad_inicial, costo_unitario, fecha_entrada,
            fecha_caducidad, referencia_oc, notas } = req.body;

    if (!producto_id) return res.status(400).json({ error: 'producto_id requerido' });
    const qty = parseFloat(cantidad_inicial) || 0;
    if (qty <= 0) return res.status(400).json({ error: 'Cantidad debe ser mayor a 0' });

    // Auto-generate lot number if not provided
    const cnt = (await client.query('SELECT COUNT(*) c FROM alm_lotes')).rows[0]?.c || 0;
    const loteNum = numero_lote || `LOT-${new Date().getFullYear()}-${String(parseInt(cnt)+1).padStart(4,'0')}`;

    // Insert lote
    const { rows: [lote] } = await client.query(`
      INSERT INTO alm_lotes (numero_lote,numero_serie,producto_id,ubicacion_id,
        cantidad_inicial,cantidad_disponible,costo_unitario,
        fecha_entrada,fecha_caducidad,referencia_oc,notas)
      VALUES ($1,$2,$3,$4,$5,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [loteNum, numero_serie||null, producto_id, ubicacion_id||null,
       qty, costo_unitario||null,
       fecha_entrada||new Date().toISOString().slice(0,10),
       fecha_caducidad||null, referencia_oc||null, notas||null]);

    // Register movement
    await client.query(`
      INSERT INTO alm_movimientos (lote_id,producto_id,ubicacion_id,tipo,cantidad,costo_unitario,referencia,created_by)
      VALUES ($1,$2,$3,'entrada',$4,$5,$6,$7)`,
      [lote.id, producto_id, ubicacion_id||null, qty, costo_unitario||null, referencia_oc||null, req.user.id]);

    // Update inventario stock
    await client.query(`
      UPDATE inventario SET
        cantidad_actual = COALESCE(cantidad_actual,0) + $1,
        stock_actual    = COALESCE(stock_actual,0)    + $1,
        fecha_ultima_entrada = $2
      WHERE id = $3`,
      [qty, fecha_entrada||new Date().toISOString().slice(0,10), producto_id]);

    await client.query('COMMIT');
    res.status(201).json(lote);
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally { client.release(); }
});

app.post('/api/almacen/lotes/:id/salida', auth, empresaActiva, licencia, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema = req.user?.schema || global._defaultSchema || 'emp_vef';
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query('BEGIN');

    const { cantidad, referencia, notas } = req.body;
    const qty = parseFloat(cantidad) || 0;
    if (qty <= 0) return res.status(400).json({ error: 'Cantidad inválida' });

    const loteR = await client.query(
      'SELECT * FROM alm_lotes WHERE id=$1', [req.params.id]);
    if (!loteR.rows.length) return res.status(404).json({ error: 'Lote no encontrado' });
    const lote = loteR.rows[0];

    if (parseFloat(lote.cantidad_disponible) < qty)
      return res.status(400).json({ error: `Stock insuficiente. Disponible: ${lote.cantidad_disponible}` });

    // Update lote
    await client.query(
      'UPDATE alm_lotes SET cantidad_disponible = cantidad_disponible - $1 WHERE id=$2',
      [qty, lote.id]);

    // Register movement
    await client.query(`
      INSERT INTO alm_movimientos (lote_id,producto_id,ubicacion_id,tipo,cantidad,costo_unitario,referencia,notas,created_by)
      VALUES ($1,$2,$3,'salida',$4,$5,$6,$7,$8)`,
      [lote.id, lote.producto_id, lote.ubicacion_id, qty,
       lote.costo_unitario, referencia||null, notas||null, req.user.id]);

    // Update inventario stock
    await client.query(`
      UPDATE inventario SET
        cantidad_actual = GREATEST(0, COALESCE(cantidad_actual,0) - $1),
        stock_actual    = GREATEST(0, COALESCE(stock_actual,0)    - $1)
      WHERE id = $2`, [qty, lote.producto_id]);

    await client.query('COMMIT');
    res.json({ ok: true, cantidad_retirada: qty,
      cantidad_disponible: parseFloat(lote.cantidad_disponible) - qty });
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally { client.release(); }
});

app.get('/api/almacen/movimientos', auth, licencia, async (req, res) => {
  try {
    await ensureAlmacenTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT m.*, l.numero_lote, i.nombre producto_nombre, u.nombre ubicacion_nombre
      FROM alm_movimientos m
      LEFT JOIN alm_lotes l ON l.id=m.lote_id
      LEFT JOIN inventario i ON i.id=m.producto_id
      LEFT JOIN alm_ubicaciones u ON u.id=m.ubicacion_id
      ORDER BY m.created_at DESC LIMIT 500`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/almacen/alertas', auth, licencia, async (req, res) => {
  try {
    await ensureAlmacenTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT l.*, i.nombre producto_nombre
      FROM alm_lotes l LEFT JOIN inventario i ON i.id=l.producto_id
      WHERE l.fecha_caducidad IS NOT NULL
        AND l.cantidad_disponible > 0
        AND l.fecha_caducidad <= CURRENT_DATE + INTERVAL '30 days'
      ORDER BY l.fecha_caducidad ASC`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// CONTABILIDAD — Tablas init + saldos + importar facturas
// ================================================================
async function ensureContabilidadTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS cat_cuentas (
      id SERIAL PRIMARY KEY, num_cta VARCHAR(30) NOT NULL UNIQUE,
      desc_cta VARCHAR(200) NOT NULL, cod_agrup VARCHAR(30),
      nivel INTEGER DEFAULT 1, naturaleza CHAR(1) DEFAULT 'D',
      tipo_cta CHAR(1) DEFAULT 'M', sub_cta_de VARCHAR(30),
      activo BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS polizas (
      id SERIAL PRIMARY KEY, fecha DATE NOT NULL DEFAULT CURRENT_DATE,
      tipo_pol CHAR(1) DEFAULT 'D', num_un_iden_pol VARCHAR(50),
      concepto TEXT, created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS polizas_detalle (
      id SERIAL PRIMARY KEY, poliza_id INTEGER NOT NULL,
      num_cta VARCHAR(30), concepto TEXT,
      debe NUMERIC(15,2) DEFAULT 0, haber NUMERIC(15,2) DEFAULT 0,
      num_cta_banco VARCHAR(30), banco_en_ext VARCHAR(3), dig_iden_ban CHAR(1),
      fec_cap DATE, num_refer VARCHAR(50), monto_total NUMERIC(15,2),
      tipo_moneda VARCHAR(5), tip_camb NUMERIC(10,4),
      num_factura_pago VARCHAR(50), folio_fiscal0 VARCHAR(50),
      rfc_emp VARCHAR(20), monto_tot_gravado NUMERIC(15,2))`);
  } finally { client.release(); }
}

app.post('/api/contabilidad/init', auth, async (req, res) => {
  try {
    await ensureContabilidadTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json({ ok: true });
  } catch(e) { res.json({ ok: false, error: e.message }); }
});

// Saldos acumulados por cuenta para el período
app.get('/api/contabilidad/saldos', auth, licencia, async (req, res) => {
  try {
    await ensureContabilidadTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { mes, anio } = req.query;
    const rows = await QR(req, `
      SELECT d.num_cta,
        COALESCE(c.desc_cta,'') desc_cta,
        COALESCE(c.naturaleza,'D') naturaleza,
        SUM(d.debe)  AS debe,
        SUM(d.haber) AS haber,
        CASE COALESCE(c.naturaleza,'D')
          WHEN 'D' THEN SUM(d.debe)  - SUM(d.haber)
          ELSE          SUM(d.haber) - SUM(d.debe)
        END AS saldo
      FROM polizas_detalle d
      JOIN polizas p ON p.id=d.poliza_id
      LEFT JOIN cat_cuentas c ON c.num_cta=d.num_cta
      WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM p.fecha)=$1::int)
        AND ($2::int IS NULL OR EXTRACT(YEAR  FROM p.fecha)=$2::int)
      GROUP BY d.num_cta, c.desc_cta, c.naturaleza
      ORDER BY d.num_cta`,
      [mes||null, anio||null]);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Import facturas as accounting entries automatically
app.post('/api/contabilidad/importar-facturas', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureContabilidadTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { mes, anio } = req.body;
    const facturas = await QR(req, `
      SELECT f.*, cl.nombre cliente_nombre
      FROM facturas f LEFT JOIN clientes cl ON cl.id=f.cliente_id
      WHERE ($1::int IS NULL OR EXTRACT(MONTH FROM f.fecha_emision)=$1::int)
        AND ($2::int IS NULL OR EXTRACT(YEAR  FROM f.fecha_emision)=$2::int)
        AND f.total > 0`,
      [mes||null, anio||null]);

    let importadas = 0;
    for (const f of facturas) {
      const subtotal = parseFloat(f.subtotal || f.total || 0);
      const iva      = parseFloat(f.iva || 0);
      const total    = subtotal + iva;
      if (!total) continue;

      // Check if already imported
      const exists = await QR(req,
        `SELECT id FROM polizas WHERE concepto LIKE $1 LIMIT 1`,
        [`%FAC${f.numero_factura}%`]);
      if (exists.length) continue;

      const client2 = await pool.connect();
      try {
        const schema2 = req.user?.schema || global._defaultSchema || 'emp_vef';
        await client2.query(`SET search_path TO "${schema2}", public`);
        await client2.query('BEGIN');
        const { rows: [pol] } = await client2.query(`
          INSERT INTO polizas (fecha,tipo_pol,concepto,created_by)
          VALUES ($1,'I',$2,$3) RETURNING id`,
          [f.fecha_emision, `Ingreso Factura ${f.numero_factura} - ${f.cliente_nombre||'Cliente'}`, req.user.id]);
        // Debe: Clientes (105)
        await client2.query(`INSERT INTO polizas_detalle (poliza_id,num_cta,concepto,debe,haber) VALUES ($1,'105',$2,$3,0)`,
          [pol.id, f.numero_factura||'Factura', total]);
        // Haber: Ventas (401)
        await client2.query(`INSERT INTO polizas_detalle (poliza_id,num_cta,concepto,debe,haber) VALUES ($1,'401',$2,0,$3)`,
          [pol.id, `Venta ${f.cliente_nombre||''}`, subtotal]);
        // Haber: IVA por pagar (213) si hay IVA
        if (iva > 0) {
          await client2.query(`INSERT INTO polizas_detalle (poliza_id,num_cta,concepto,debe,haber) VALUES ($1,'213','IVA Facturado',0,$2)`,
            [pol.id, iva]);
        }
        await client2.query('COMMIT');
        importadas++;
      } catch { await client2.query('ROLLBACK'); }
      finally { client2.release(); }
    }
    res.json({ ok: true, importadas });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ================================================================
// ACTIVOS FIJOS
// ================================================================
async function ensureActivosFijosTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS activos_fijos (
      id SERIAL PRIMARY KEY,
      codigo VARCHAR(50), nombre TEXT NOT NULL, categoria VARCHAR(100),
      marca VARCHAR(100), modelo VARCHAR(100), numero_serie VARCHAR(100),
      valor_original NUMERIC(15,2) DEFAULT 0,
      valor_salvamento NUMERIC(15,2) DEFAULT 0,
      valor_actual NUMERIC(15,2) DEFAULT 0,
      depreciacion_acumulada NUMERIC(15,2) DEFAULT 0,
      metodo_depr VARCHAR(30) DEFAULT 'Línea Recta',
      vida_util_anos INTEGER DEFAULT 10,
      tasa_depr NUMERIC(8,4),
      fecha_adquisicion DATE,
      ubicacion TEXT, proveedor_factura TEXT,
      estatus VARCHAR(20) DEFAULT 'activo',
      motivo_baja TEXT, valor_recuperacion NUMERIC(15,2),
      notas_baja TEXT,
      created_by INTEGER, created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS depreciaciones_af (
      id SERIAL PRIMARY KEY,
      activo_id INTEGER NOT NULL,
      periodo VARCHAR(20),
      monto_depr NUMERIC(15,2) DEFAULT 0,
      valor_libros_inicio NUMERIC(15,2),
      valor_libros_fin NUMERIC(15,2),
      metodo VARCHAR(30),
      created_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.post('/api/activos-fijos/init', auth, async (req, res) => {
  try { await ensureActivosFijosTables(req.user?.schema || global._defaultSchema || 'emp_vef'); res.json({ ok: true }); }
  catch(e) { res.json({ ok: false }); }
});

app.get('/api/activos-fijos', auth, licencia, async (req, res) => {
  try {
    await ensureActivosFijosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json(await QR(req, 'SELECT * FROM activos_fijos ORDER BY created_at DESC'));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/activos-fijos/depreciaciones', auth, licencia, async (req, res) => {
  try {
    await ensureActivosFijosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const rows = await QR(req, `
      SELECT d.*, a.nombre activo_nombre
      FROM depreciaciones_af d LEFT JOIN activos_fijos a ON a.id=d.activo_id
      ORDER BY d.created_at DESC LIMIT 100`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/activos-fijos/depreciar', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureActivosFijosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { mes, anio } = req.body;
    const periodo = `${anio}-${String(mes).padStart(2,'0')}`;
    const activos = await QR(req,
      "SELECT * FROM activos_fijos WHERE estatus='activo' AND vida_util_anos>0 AND valor_actual>0");
    let procesados = 0; let totalDepr = 0;
    for (const a of activos) {
      const vOrig = parseFloat(a.valor_original || 0);
      const vSalv = parseFloat(a.valor_salvamento || 0);
      const vida  = parseInt(a.vida_util_anos || 10);
      const vAct  = parseFloat(a.valor_actual || 0);
      let deprMes = 0;
      if (a.metodo_depr === 'Línea Recta') {
        deprMes = (vOrig - vSalv) / vida / 12;
      } else if (a.metodo_depr === 'Saldo Decreciente') {
        deprMes = vAct * 0.20 / 12;
      }
      deprMes = Math.min(deprMes, Math.max(0, vAct - vSalv));
      if (deprMes <= 0) continue;
      const vFin = Math.max(vSalv, vAct - deprMes);
      // Register depreciation
      await QR(req, `INSERT INTO depreciaciones_af (activo_id,periodo,monto_depr,valor_libros_inicio,valor_libros_fin,metodo) VALUES ($1,$2,$3,$4,$5,$6)`,
        [a.id, periodo, deprMes, vAct, vFin, a.metodo_depr]);
      // Update activo
      await QR(req, `UPDATE activos_fijos SET valor_actual=$1, depreciacion_acumulada=COALESCE(depreciacion_acumulada,0)+$2, updated_at=NOW() WHERE id=$3`,
        [vFin, deprMes, a.id]);
      // Register accounting entry if polizas exists
      await QR(req, `INSERT INTO polizas (fecha,tipo_pol,concepto,created_by) VALUES ($1,'D',$2,$3)`,
        [`${anio}-${String(mes).padStart(2,'0')}-01`,
         `Depreciación ${a.nombre} ${periodo}`, req.user.id]).catch(() => {});
      procesados++;
      totalDepr += deprMes;
    }
    res.json({ ok: true, procesados, total: totalDepr.toFixed(2) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// CONTROL DE CALIDAD
// ================================================================
async function ensureCalidadTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS cal_inspecciones (
      id SERIAL PRIMARY KEY, tipo VARCHAR(50) NOT NULL,
      producto_id INTEGER, referencia TEXT, inspector TEXT,
      fecha DATE DEFAULT CURRENT_DATE,
      cantidad_inspeccionada INTEGER DEFAULT 0,
      cantidad_aprobada INTEGER DEFAULT 0,
      cantidad_rechazada INTEGER DEFAULT 0,
      resultado VARCHAR(30) DEFAULT 'pendiente',
      observaciones TEXT, acciones_correctivas TEXT,
      created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS cal_no_conformidades (
      id SERIAL PRIMARY KEY, descripcion TEXT NOT NULL,
      area VARCHAR(100), severidad VARCHAR(20) DEFAULT 'menor',
      accion_correctiva TEXT, responsable TEXT,
      fecha_deteccion DATE DEFAULT CURRENT_DATE,
      fecha_limite DATE, fecha_cierre DATE,
      estatus VARCHAR(20) DEFAULT 'abierta',
      created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.post('/api/calidad/init', auth, async (req, res) => {
  try { await ensureCalidadTables(req.user?.schema || global._defaultSchema || 'emp_vef'); res.json({ ok: true }); }
  catch(e) { res.json({ ok: false }); }
});

app.get('/api/calidad/inspecciones', auth, licencia, async (req, res) => {
  try {
    await ensureCalidadTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json(await QR(req, 'SELECT ci.*, i.nombre producto_nombre FROM cal_inspecciones ci LEFT JOIN inventario i ON i.id=ci.producto_id ORDER BY ci.fecha DESC'));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/calidad/inspecciones/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, 'SELECT * FROM cal_inspecciones WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/calidad/inspecciones', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureCalidadTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { tipo, producto_id, referencia, inspector, fecha, cantidad_inspeccionada,
            cantidad_aprobada, cantidad_rechazada, resultado, observaciones, acciones_correctivas } = req.body;
    const rows = await QR(req, `
      INSERT INTO cal_inspecciones (tipo,producto_id,referencia,inspector,fecha,
        cantidad_inspeccionada,cantidad_aprobada,cantidad_rechazada,resultado,observaciones,acciones_correctivas,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
      [tipo, producto_id||null, referencia||null, inspector||null,
       fecha||new Date().toISOString().slice(0,10),
       parseInt(cantidad_inspeccionada)||0, parseInt(cantidad_aprobada)||0,
       parseInt(cantidad_rechazada)||0, resultado||'pendiente',
       observaciones||null, acciones_correctivas||null, req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/calidad/no-conformidades', auth, licencia, async (req, res) => {
  try {
    await ensureCalidadTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json(await QR(req, 'SELECT * FROM cal_no_conformidades ORDER BY CASE severidad WHEN \'critica\' THEN 1 WHEN \'mayor\' THEN 2 ELSE 3 END, created_at DESC'));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/calidad/no-conformidades/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, 'SELECT * FROM cal_no_conformidades WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/calidad/no-conformidades', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureCalidadTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { descripcion, area, severidad, accion_correctiva, responsable, fecha_limite } = req.body;
    if (!descripcion) return res.status(400).json({ error: 'descripcion requerida' });
    const rows = await QR(req, `
      INSERT INTO cal_no_conformidades (descripcion,area,severidad,accion_correctiva,responsable,fecha_limite,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [descripcion, area||null, severidad||'menor', accion_correctiva||null,
       responsable||null, fecha_limite||null, req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/calidad/no-conformidades/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const b = req.body;
    const sets = []; const vals = []; let i = 1;
    const add = (k,v) => { sets.push(`${k}=$${i++}`); vals.push(v); };
    if (b.estatus      !== undefined) add('estatus',      b.estatus);
    if (b.fecha_cierre !== undefined) add('fecha_cierre', b.fecha_cierre);
    if (b.accion_correctiva !== undefined) add('accion_correctiva', b.accion_correctiva);
    vals.push(req.params.id);
    await QR(req, `UPDATE cal_no_conformidades SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const rows = await QR(req, 'SELECT * FROM cal_no_conformidades WHERE id=$1', [req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// CONTRATOS / SLA
// ================================================================
async function ensureContratosTables(schema) {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);
    await client.query(`CREATE TABLE IF NOT EXISTS contratos_servicio (
      id SERIAL PRIMARY KEY,
      numero_contrato VARCHAR(50),
      cliente_id INTEGER, tipo VARCHAR(80),
      descripcion TEXT,
      fecha_inicio DATE, fecha_fin DATE,
      valor_mensual NUMERIC(15,2),
      sla_horas_respuesta INTEGER DEFAULT 24,
      sla_horas_resolucion INTEGER DEFAULT 48,
      visitas_mes INTEGER DEFAULT 0,
      horas_soporte INTEGER DEFAULT 0,
      estatus VARCHAR(20) DEFAULT 'activo',
      created_by INTEGER, created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW())`);
  } finally { client.release(); }
}

app.post('/api/contratos/init', auth, async (req, res) => {
  try { await ensureContratosTables(req.user?.schema || global._defaultSchema || 'emp_vef'); res.json({ ok: true }); }
  catch(e) { res.json({ ok: false }); }
});

app.get('/api/contratos', auth, licencia, async (req, res) => {
  try {
    await ensureContratosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    res.json(await QR(req, `
      SELECT cs.*, cl.nombre cliente_nombre
      FROM contratos_servicio cs LEFT JOIN clientes cl ON cl.id=cs.cliente_id
      ORDER BY cs.fecha_fin ASC NULLS LAST`));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/contratos/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, `
      SELECT cs.*, cl.nombre cliente_nombre
      FROM contratos_servicio cs LEFT JOIN clientes cl ON cl.id=cs.cliente_id
      WHERE cs.id=$1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/contratos', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureContratosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { cliente_id, tipo, descripcion, fecha_inicio, fecha_fin, valor_mensual,
            sla_horas_respuesta, sla_horas_resolucion, visitas_mes, horas_soporte, estatus } = req.body;
    if (!cliente_id) return res.status(400).json({ error: 'cliente_id requerido' });
    const cnt = (await QR(req, 'SELECT COUNT(*) c FROM contratos_servicio'))[0]?.c || 0;
    const num = `CT-${new Date().getFullYear()}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const rows = await QR(req, `
      INSERT INTO contratos_servicio
        (numero_contrato,cliente_id,tipo,descripcion,fecha_inicio,fecha_fin,valor_mensual,
         sla_horas_respuesta,sla_horas_resolucion,visitas_mes,horas_soporte,estatus,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *`,
      [num, parseInt(cliente_id), tipo||'Servicio', descripcion||null,
       fecha_inicio||null, fecha_fin||null, parseFloat(valor_mensual)||null,
       parseInt(sla_horas_respuesta)||24, parseInt(sla_horas_resolucion)||48,
       parseInt(visitas_mes)||0, parseInt(horas_soporte)||0,
       estatus||'activo', req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/contratos/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const b = req.body;
    const sets = []; const vals = []; let i = 1;
    const add = (k,v) => { sets.push(`${k}=$${i++}`); vals.push(v); };
    if (b.estatus    !== undefined) add('estatus',    b.estatus);
    if (b.fecha_fin  !== undefined) add('fecha_fin',  b.fecha_fin);
    if (b.valor_mensual !== undefined) add('valor_mensual', parseFloat(b.valor_mensual)||null);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req, `UPDATE contratos_servicio SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const rows = await QR(req, 'SELECT * FROM contratos_servicio WHERE id=$1', [req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// STRIPE — Rutas de pago (registro con pago)
// Requiere: npm install stripe
// .env: STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, STRIPE_PRICE_ID
// ================================================================
try {
  const stripe = process.env.STRIPE_SECRET_KEY
    ? require('stripe')(process.env.STRIPE_SECRET_KEY)
    : null;
  if (stripe) {
    require('./stripe_routes')(app, pool, stripe);
    console.log('💳 Stripe rutas de pago cargadas');
  } else {
    console.log('💳 Stripe no configurado (STRIPE_SECRET_KEY no definida — solo registro gratuito disponible)');
  }
} catch(e) {
  console.warn('⚠️  stripe_routes no disponible:', e.message);
}


// ================================================================
// SEED DE DATOS INICIALES — FAC-2026-001 (VEF Automatización)
// Se ejecuta al iniciar si no existen datos
// ================================================================
async function seedDatosIniciales() {
  const schema = global._defaultSchema || 'emp_vef';
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO "${schema}", public`);

    // ── 1. Empresa config ──────────────────────────────────────
    const ec = await client.query('SELECT id FROM empresa_config LIMIT 1');
    if (ec.rows.length) {
      await client.query(`UPDATE empresa_config SET
        nombre='VEF Automatización',
        rfc='GOBE840604JLA',
        regimen_fiscal='Régimen Simplificado de Confianza',
        telefono='+52 (722) 115-7792',
        email='soporte.ventas@vef-automatizacion.com',
        direccion='Privada Rio Panuco, Manzana 5 Lote 10',
        ciudad='Toluca', estado='Estado de México',
        cp='50227', pais='México',
        moneda_default='MXN', iva_default=16.00,
        updated_at=NOW()
        WHERE id=$1`, [ec.rows[0].id]);
    } else {
      await client.query(`INSERT INTO empresa_config
        (nombre,rfc,regimen_fiscal,telefono,email,direccion,ciudad,estado,cp,pais,moneda_default,iva_default)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
        ['VEF Automatización','GOBE840604JLA','Régimen Simplificado de Confianza',
         '+52 (722) 115-7792','soporte.ventas@vef-automatizacion.com',
         'Privada Rio Panuco, Manzana 5 Lote 10','Toluca','Estado de México',
         '50227','México','MXN',16.00]);
    }
    console.log('  ✅ empresa_config configurada');

    // ── 2. Cliente HMO ────────────────────────────────────────
    let cliId;
    const cliRes = await client.query(`SELECT id FROM clientes WHERE rfc='HAC190729242' LIMIT 1`);
    if (cliRes.rows.length) {
      cliId = cliRes.rows[0].id;
    } else {
      // Asegurar columnas opcionales existen
      const colsExist = {
        uso_cfdi:       false, cp: false, ciudad: false,
        tipo_persona:   false, regimen_fiscal: false,
      };
      for (const col of Object.keys(colsExist)) {
        try {
          await client.query(`ALTER TABLE clientes ADD COLUMN IF NOT EXISTS ${col} TEXT`);
          colsExist[col] = true;
        } catch {}
      }
      const ins = await client.query(`INSERT INTO clientes
        (nombre,rfc,regimen_fiscal,tipo_persona,email,telefono,direccion,cp,ciudad,uso_cfdi,activo)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,true) RETURNING id`,
        ['HMO AUTOMATIZACION Y COMERCIALIZACION INDUSTRIAL',
         'HAC190729242','Régimen Simplificado de Confianza','moral',
         'hmo.venta1@gmail.com','7223087247',
         'Melero y Piña 511-interior 102, San Sebastian',
         '50150','Toluca de Lerdo, Méx.','G03']);
      cliId = ins.rows[0].id;
      console.log(`  ✅ Cliente HMO creado ID=${cliId}`);
    }

    // ── 3. Proyecto ──────────────────────────────────────────
    let proyId;
    const proyRes = await client.query(`SELECT id FROM proyectos WHERE nombre='Servicio de Programación HMO' LIMIT 1`);
    if (proyRes.rows.length) {
      proyId = proyRes.rows[0].id;
    } else {
      const ins = await client.query(`INSERT INTO proyectos (nombre,cliente_id,estatus,responsable)
        VALUES($1,$2,'activo',$3) RETURNING id`,
        ['Servicio de Programación HMO', cliId, 'VEF Automatización']);
      proyId = ins.rows[0].id;
      console.log(`  ✅ Proyecto creado ID=${proyId}`);
    }

    // ── 4. Cotización ────────────────────────────────────────
    let cotId;
    const cotRes = await client.query(`SELECT id FROM cotizaciones WHERE numero_cotizacion='COT-2026-001' LIMIT 1`);
    if (cotRes.rows.length) {
      cotId = cotRes.rows[0].id;
    } else {
      const ins = await client.query(`INSERT INTO cotizaciones
        (numero_cotizacion,proyecto_id,fecha_emision,validez_hasta,
         alcance_tecnico,moneda,subtotal,iva,total,estatus)
        VALUES('COT-2026-001',$1,'2026-04-10','2026-04-10',
               'Servicio de programación industrial','MXN',
               35750.00,5720.00,37208.59,'aprobada') RETURNING id`, [proyId]);
      cotId = ins.rows[0].id;

      // Items cotización
      try {
        const coluIco = await client.query(`SELECT column_name FROM information_schema.columns
          WHERE table_schema=$1 AND table_name='items_cotizacion'`,[schema]);
        const iCols = coluIco.rows.map(r=>r.column_name);
        const extraCols = iCols.includes('clave_prod_serv') ?
          ',clave_prod_serv,clave_unidad,objeto_imp' : '';
        const extraVals = iCols.includes('clave_prod_serv') ?
          ",'81111600','H87','02'" : '';
        await client.query(`INSERT INTO items_cotizacion
          (cotizacion_id,descripcion,cantidad,precio_unitario,total${extraCols})
          VALUES($1,'Servicio de programacion',1,35750.00,35750.00${extraVals})`, [cotId]);
      } catch(e) { console.log('  ⚠ items_cotizacion:', e.message); }
      console.log(`  ✅ Cotización COT-2026-001 creada ID=${cotId}`);
    }

    // ── 5. Factura FAC-2026-001 ──────────────────────────────
    const facRes = await client.query(`SELECT id FROM facturas WHERE numero_factura='FAC-2026-001' LIMIT 1`);
    if (facRes.rows.length) {
      console.log(`  ✅ Factura FAC-2026-001 ya existe ID=${facRes.rows[0].id}`);
    } else {
      // Asegurar columnas retenciones
      for (const col of ['retencion_isr','retencion_iva','estatus_pago','cliente_id',
                          'subtotal','iva','moneda','fecha_vencimiento']) {
        try { await client.query(`ALTER TABLE facturas ADD COLUMN IF NOT EXISTS ${col} TEXT`); } catch {}
      }
      const fCols = await client.query(`SELECT column_name FROM information_schema.columns
        WHERE table_schema=$1 AND table_name='facturas'`,[schema]);
      const fc = fCols.rows.map(r=>r.column_name);

      const cols = ['numero_factura','cotizacion_id'];
      const vals = ['FAC-2026-001', cotId];
      const mp=(col,val)=>{if(fc.includes(col)){cols.push(col);vals.push(val);}};
      mp('cliente_id',    cliId);
      mp('moneda',        'MXN');
      mp('subtotal',      35750.00);
      mp('iva',           5720.00);
      mp('retencion_isr', 446.88);
      mp('retencion_iva', 3814.53);
      mp('total',         37208.59);
      mp('monto',         37208.59);
      mp('fecha_emision', '2026-04-10');
      mp('fecha_vencimiento','2026-04-10');
      mp('estatus',       'pendiente');
      mp('estatus_pago',  'pendiente');
      mp('notas',         'PPD — Pago en Parcialidades o Diferido | Forma de Pago: 03 — Transferencia electrónica');
      const ph = vals.map((_,i)=>`$${i+1}`).join(',');
      const ins = await client.query(
        `INSERT INTO facturas (${cols.join(',')}) VALUES (${ph}) RETURNING id`, vals);
      console.log(`  ✅ Factura FAC-2026-001 creada ID=${ins.rows[0].id}`);
      console.log(`     Subtotal: $35,750 | IVA: $5,720 | ISR: -$446.88 | RetIVA: -$3,814.53 | Total: $37,208.59`);
    }

  } catch(e) {
    console.error('  ❌ seedDatosIniciales:', e.message);
  } finally {
    client.release();
  }
}

app.listen(PORT, async ()=>{
  console.log(`\n${'═'.repeat(50)}`);
  console.log(`  VEF ERP — Puerto ${PORT}`);
  console.log(`  DB: ${process.env.DB_HOST}`);
  console.log('═'.repeat(50)+'\n');
  await autoSetup();
  await seedDatosIniciales();
  console.log(`\n🚀 http://localhost:${PORT}`);
  console.log(`🔐 Licencias: activas (trial 30 días por defecto)`);
  console.log(`🔑 Solo admin puede gestionar usuarios y empresas\n`);
});
module.exports=app
app.get('/api/activos-fijos/:id', auth, licencia, async (req, res) => {
  try {
    const rows = await QR(req, 'SELECT * FROM activos_fijos WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'No encontrado' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/activos-fijos', auth, empresaActiva, licencia, async (req, res) => {
  try {
    await ensureActivosFijosTables(req.user?.schema || global._defaultSchema || 'emp_vef');
    const { nombre, codigo, categoria, marca, modelo, numero_serie,
            valor_original, valor_salvamento, metodo_depr, vida_util_anos,
            fecha_adquisicion, ubicacion, proveedor_factura } = req.body;
    if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
    const cnt = (await QR(req, 'SELECT COUNT(*) c FROM activos_fijos'))[0]?.c || 0;
    const cod = codigo || `AF-${new Date().getFullYear()}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const vOrig = parseFloat(valor_original) || 0;
    const vSalv = parseFloat(valor_salvamento) || 0;
    const vida  = parseInt(vida_util_anos) || 10;
    const tasa  = vida > 0 ? Math.round((vOrig - vSalv) / vida / vOrig * 10000) / 100 : 0;
    const rows = await QR(req, `
      INSERT INTO activos_fijos
        (codigo,nombre,categoria,marca,modelo,numero_serie,valor_original,valor_salvamento,
         valor_actual,metodo_depr,vida_util_anos,tasa_depr,fecha_adquisicion,ubicacion,proveedor_factura,created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$7,$9,$10,$11,$12,$13,$14,$15) RETURNING *`,
      [cod, nombre, categoria||null, marca||null, modelo||null, numero_serie||null,
       vOrig, vSalv, metodo_depr||'Línea Recta', vida, tasa,
       fecha_adquisicion||null, ubicacion||null, proveedor_factura||null, req.user.id]);
    res.status(201).json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/activos-fijos/:id', auth, empresaActiva, licencia, async (req, res) => {
  try {
    const b = req.body;
    const sets = []; const vals = []; let i = 1;
    const add = (k,v) => { sets.push(`${k}=$${i++}`); vals.push(v); };
    if (b.estatus            !== undefined) add('estatus',            b.estatus);
    if (b.motivo_baja        !== undefined) add('motivo_baja',        b.motivo_baja);
    if (b.valor_recuperacion !== undefined) add('valor_recuperacion', parseFloat(b.valor_recuperacion)||0);
    if (b.notas_baja         !== undefined) add('notas_baja',         b.notas_baja);
    if (b.valor_actual       !== undefined) add('valor_actual',       parseFloat(b.valor_actual)||0);
    if (b.depreciacion_acumulada !== undefined) add('depreciacion_acumulada', parseFloat(b.depreciacion_acumulada)||0);
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id);
    await QR(req, `UPDATE activos_fijos SET ${sets.join(',')} WHERE id=$${i}`, vals);
    const rows = await QR(req, 'SELECT * FROM activos_fijos WHERE id=$1', [req.params.id]);
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

;