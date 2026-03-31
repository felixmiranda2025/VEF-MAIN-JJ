"""
SAT Descarga Masiva — Microservicio async con aiohttp
Puerto: 5050
Instalar: pip install aiohttp requests lxml pyOpenSSL chilkat2
"""
import sys, os, asyncio, base64, zipfile, traceback, json
from io import BytesIO
from datetime import datetime

# ── Rutas absolutas ──────────────────────────────────────────────
_THIS_DIR   = os.path.dirname(os.path.abspath(__file__))
_PARENT_DIR = os.path.dirname(_THIS_DIR)

# ── Registrar sat_service como paquete (imports relativos) ───────
import types, importlib.util

def _register_package():
    if 'sat_service' in sys.modules:
        return
    pkg = types.ModuleType('sat_service')
    pkg.__file__    = os.path.join(_THIS_DIR, '__init__.py')
    pkg.__path__    = [_THIS_DIR]
    pkg.__package__ = 'sat_service'
    sys.modules['sat_service'] = pkg

    for mod_name in ['Utils', 'Login', 'Request', 'Verify', 'Download']:
        full_name = f'sat_service.{mod_name}'
        filepath  = os.path.join(_THIS_DIR, f'{mod_name}.py')
        if not os.path.exists(filepath):
            print(f'⚠️  No se encontró: {filepath}')
            continue
        spec = importlib.util.spec_from_file_location(full_name, filepath,
                    submodule_search_locations=[])
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = 'sat_service'
        sys.modules[full_name] = mod
        setattr(pkg, mod_name, mod)
        try:
            spec.loader.exec_module(mod)
        except Exception as e:
            print(f'❌ Error cargando {mod_name}: {e}')
            traceback.print_exc()
            sys.exit(1)
    print('✅ Módulos SAT cargados: Utils, Login, Request, Verify, Download')

# ── Verificar dependencias ───────────────────────────────────────
_missing = []
for _pkg, _imp in [('aiohttp','aiohttp'),('requests','requests'),
                   ('lxml','lxml'),('pyOpenSSL','OpenSSL'),('chilkat2','chilkat2')]:
    try: __import__(_imp)
    except ImportError: _missing.append(_pkg)

if _missing:
    print(f'\n❌ Dependencias faltantes: {", ".join(_missing)}')
    print(f'   Ejecuta: pip install {" ".join(_missing)}')
    sys.exit(1)

_register_package()

Utils    = sys.modules['sat_service.Utils']
Login    = sys.modules['sat_service.Login']
SatReq   = sys.modules['sat_service.Request']
Verify   = sys.modules['sat_service.Verify']
Download = sys.modules['sat_service.Download']

import aiohttp
from aiohttp import web

# ── Helpers ──────────────────────────────────────────────────────
def get_fiel(data: dict):
    cer_b64  = data.get('cer', '')
    key_b64  = data.get('key', '')
    password = data.get('password', '')
    if ',' in cer_b64: cer_b64 = cer_b64.split(',')[1]
    if ',' in key_b64: key_b64 = key_b64.split(',')[1]
    if not cer_b64 or not key_b64:
        raise ValueError('Se requieren cer y key en base64')
    cer_bytes  = base64.b64decode(cer_b64)
    key_buffer = BytesIO(base64.b64decode(key_b64))
    key_pem    = Utils.pkey_buffer_to_pem(key_buffer, password)
    return cer_bytes, key_pem

def parse_cfdi(xml_str, filename=''):
    try:
        from lxml import etree
        NS4='http://www.sat.gob.mx/cfd/4'; NS3='http://www.sat.gob.mx/cfd/3'
        TFD='http://www.sat.gob.mx/TimbreFiscalDigital'
        raw = xml_str.encode('utf-8') if isinstance(xml_str, str) else xml_str
        root = etree.fromstring(raw)
        def g(el, *attrs):
            if el is None: return ''
            for a in attrs:
                v = el.get(a)
                if v: return v
            return ''
        em  = root.find(f'.//{{{NS4}}}Emisor')   or root.find(f'.//{{{NS3}}}Emisor')   or root.find('.//Emisor')
        rec = root.find(f'.//{{{NS4}}}Receptor')  or root.find(f'.//{{{NS3}}}Receptor')  or root.find('.//Receptor')
        tf  = root.find(f'.//{{{TFD}}}TimbreFiscalDigital') or root.find('.//TimbreFiscalDigital')
        imp = root.find(f'.//{{{NS4}}}Impuestos') or root.find(f'.//{{{NS3}}}Impuestos') or root.find('.//Impuestos')
        iva = isr = '0'
        if imp is not None:
            tr = imp.find(f'.//{{{NS4}}}Traslado')  or imp.find('.//Traslado')
            rt = imp.find(f'.//{{{NS4}}}Retencion') or imp.find('.//Retencion')
            if tr: iva = g(tr,'Importe','importe') or '0'
            if rt: isr = g(rt,'Importe','importe') or '0'
        return {
            'archivo': filename,
            'uuid':    g(tf,'UUID','Uuid') if tf is not None else '',
            'fecha':   g(root,'Fecha','fecha'),
            'tipo':    g(root,'TipoDeComprobante','tipoDeComprobante'),
            'subtotal':g(root,'SubTotal','subTotal'),
            'iva': iva, 'isr_ret': isr,
            'total':   g(root,'Total','total'),
            'moneda':  g(root,'Moneda','moneda') or 'MXN',
            'emisor_rfc':      g(em,'Rfc','rfc')     if em  is not None else '',
            'emisor_nombre':   g(em,'Nombre','nombre') if em is not None else '',
            'receptor_rfc':    g(rec,'Rfc','rfc')     if rec is not None else '',
            'receptor_nombre': g(rec,'Nombre','nombre') if rec is not None else '',
            'uso_cfdi':        g(rec,'UsoCFDI','usoCFDI') if rec is not None else '',
            'xml': xml_str if isinstance(xml_str,str) else xml_str.decode('utf-8','replace'),
        }
    except Exception as e:
        return {'archivo':filename,'uuid':'','error':str(e),
                'xml': xml_str if isinstance(xml_str,str) else ''}

def json_resp(data, status=200):
    return web.Response(
        text=json.dumps(data, ensure_ascii=False),
        content_type='application/json',
        status=status
    )

# ── Endpoints async ──────────────────────────────────────────────
async def health(request):
    return json_resp({'ok': True, 'servicio': 'SAT Descarga Masiva async', 'puerto': 5050})

async def login(request):
    try:
        d = await request.json()
        loop = asyncio.get_event_loop()
        # Login SAT es síncrono (SOAP) — ejecutar en thread pool
        cer, kpem = get_fiel(d)
        tok = await loop.run_in_executor(None,
            lambda: Login.TokenRequest().soapRequest(certificate=cer, keyPEM=kpem))
        if tok:
            rfc = Utils.rfc_from_certificate(cer)
            return json_resp({'ok': True, 'token': tok, 'rfc': rfc})
        return json_resp({'ok': False, 'error': 'No se obtuvo token. Verifica FIEL y contraseña.'}, 400)
    except Exception as e:
        traceback.print_exc()
        return json_resp({'ok': False, 'error': str(e)}, 500)

async def solicitar(request):
    try:
        d = await request.json()
        cer, kpem = get_fiel(d)
        dt_i = datetime.strptime(d['fecha_inicio'],'%Y-%m-%d').replace(hour=0,  minute=0,  second=0)
        dt_f = datetime.strptime(d['fecha_fin'],   '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(None, lambda:
            SatReq.RequestDownloadRequest().soapRequest(
                certificate=cer, keyPEM=kpem, token=d['token'],
                start_date=dt_i, end_date=dt_f,
                tipo_solicitud=d.get('tipo','CFDI')))
        return json_resp({'ok': True, 'solicitud': dict(r)})
    except Exception as e:
        traceback.print_exc()
        return json_resp({'ok': False, 'error': str(e)}, 500)

async def verificar(request):
    try:
        d = await request.json()
        cer, kpem = get_fiel(d)
        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(None, lambda:
            Verify.VerifyRequest().soapRequest(
                certificate=cer, keyPEM=kpem,
                token=d['token'], id_solicitud=d['id_solicitud']))
        return json_resp({'ok': True, 'listo': r.ready,
                          'paquetes': r.paquetes or [], 'error_info': r.error})
    except Exception as e:
        traceback.print_exc()
        return json_resp({'ok': False, 'error': str(e)}, 500)

async def descargar(request):
    try:
        d = await request.json()
        cer, kpem = get_fiel(d)
        loop = asyncio.get_event_loop()
        zip_path = await loop.run_in_executor(None, lambda:
            Download.DownloadRequest().soapRequest(
                certificate=cer, keyPEM=kpem,
                token=d['token'], id_paquete=d['id_paquete'],
                path='/tmp/sat_pkg_'))
        if not zip_path:
            return json_resp({'ok':False,'error':'No se pudo descargar. Token expirado o paquete inválido.'},400)
        cfdis = []
        with zipfile.ZipFile(zip_path,'r') as zf:
            for name in zf.namelist():
                if name.lower().endswith('.xml'):
                    raw = zf.read(name)
                    cfdis.append(parse_cfdi(raw.decode('utf-8','replace'), name))
        try: os.remove(zip_path)
        except: pass
        return json_resp({'ok':True,'paquete':d['id_paquete'],'total':len(cfdis),'cfdis':cfdis})
    except Exception as e:
        traceback.print_exc()
        return json_resp({'ok':False,'error':str(e)},500)

# ── App aiohttp ──────────────────────────────────────────────────
app = web.Application()
app.router.add_get( '/health',    health)
app.router.add_post('/login',     login)
app.router.add_post('/solicitar', solicitar)
app.router.add_post('/verificar', verificar)
app.router.add_post('/descargar', descargar)

if __name__ == '__main__':
    print('🏛  SAT async (aiohttp) iniciando en puerto 5050...')
    web.run_app(app, host='0.0.0.0', port=5050, print=lambda *a: None)
