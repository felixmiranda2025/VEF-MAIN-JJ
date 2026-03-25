"""
SAT Descarga Masiva — Microservicio Flask
Puerto: 5050
Dependencias: pip install flask requests lxml pyOpenSSL chilkat2
"""
import sys, os, base64, zipfile, traceback
from io import BytesIO
from datetime import datetime

# ── Resolver rutas absolutas ─────────────────────────────────────
_THIS_DIR   = os.path.dirname(os.path.abspath(__file__))   # .../sat_service/
_PARENT_DIR = os.path.dirname(_THIS_DIR)                    # .../vef-erp/

# El truco: registrar sat_service como paquete ANTES de cualquier import
# Esto permite que "from . import Utils" funcione en Login.py, etc.
import types

def _register_package():
    """Registra sat_service como paquete Python para que los imports relativos funcionen."""
    if 'sat_service' in sys.modules:
        return  # ya registrado

    # Crear el paquete
    pkg = types.ModuleType('sat_service')
    pkg.__file__    = os.path.join(_THIS_DIR, '__init__.py')
    pkg.__path__    = [_THIS_DIR]
    pkg.__package__ = 'sat_service'
    pkg.__spec__    = None
    sys.modules['sat_service'] = pkg

    # Cargar cada submódulo y registrarlo como sat_service.X
    import importlib.util

    for mod_name in ['Utils', 'Login', 'Request', 'Verify', 'Download']:
        full_name = f'sat_service.{mod_name}'
        filepath  = os.path.join(_THIS_DIR, f'{mod_name}.py')

        if not os.path.exists(filepath):
            print(f'⚠️  Archivo no encontrado: {filepath}')
            continue

        spec = importlib.util.spec_from_file_location(
            full_name, filepath,
            submodule_search_locations=[]
        )
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = 'sat_service'
        mod.__spec__    = spec

        # Registrar ANTES de ejecutar para que imports circulares funcionen
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
missing = []
for pkg_name, import_name in [
    ('flask','flask'), ('requests','requests'),
    ('lxml','lxml'), ('pyOpenSSL','OpenSSL'), ('chilkat2','chilkat2')
]:
    try:
        __import__(import_name)
    except ImportError:
        missing.append(pkg_name)

if missing:
    print(f'\n❌ Faltan dependencias: {", ".join(missing)}')
    print(f'   Ejecuta: pip install {" ".join(missing)}\n')
    sys.exit(1)

# ── Cargar módulos SAT ───────────────────────────────────────────
_register_package()

# Acceder a los módulos registrados
sat_service = sys.modules['sat_service']
Utils    = sys.modules['sat_service.Utils']
Login    = sys.modules['sat_service.Login']
SatReq   = sys.modules['sat_service.Request']
Verify   = sys.modules['sat_service.Verify']
Download = sys.modules['sat_service.Download']

from flask import Flask, request, jsonify
app = Flask(__name__)

# ── Helpers ──────────────────────────────────────────────────────
def get_fiel(data):
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
        NS4 = 'http://www.sat.gob.mx/cfd/4'
        NS3 = 'http://www.sat.gob.mx/cfd/3'
        TFD = 'http://www.sat.gob.mx/TimbreFiscalDigital'
        root = etree.fromstring(xml_str.encode('utf-8') if isinstance(xml_str, str) else xml_str)

        def g(el, *attrs):
            if el is None: return ''
            for a in attrs:
                v = el.get(a)
                if v: return v
            return ''

        em = (root.find(f'.//{{{NS4}}}Emisor')   or root.find(f'.//{{{NS3}}}Emisor')
              or root.find('.//Emisor'))
        re_ = (root.find(f'.//{{{NS4}}}Receptor') or root.find(f'.//{{{NS3}}}Receptor')
               or root.find('.//Receptor'))
        tf  = (root.find(f'.//{{{TFD}}}TimbreFiscalDigital')
               or root.find('.//TimbreFiscalDigital'))

        imp = (root.find(f'.//{{{NS4}}}Impuestos') or root.find(f'.//{{{NS3}}}Impuestos')
               or root.find('.//Impuestos'))
        iva = isr = '0'
        if imp is not None:
            tr = imp.find(f'.//{{{NS4}}}Traslado') or imp.find('.//Traslado')
            rt = imp.find(f'.//{{{NS4}}}Retencion') or imp.find('.//Retencion')
            if tr: iva = g(tr, 'Importe', 'importe') or '0'
            if rt: isr = g(rt, 'Importe', 'importe') or '0'

        return {
            'archivo':         filename,
            'uuid':            g(tf,  'UUID', 'Uuid') if tf is not None else '',
            'fecha':           g(root, 'Fecha', 'fecha'),
            'tipo':            g(root, 'TipoDeComprobante', 'tipoDeComprobante'),
            'subtotal':        g(root, 'SubTotal', 'subTotal'),
            'iva':             iva,
            'isr_ret':         isr,
            'total':           g(root, 'Total', 'total'),
            'moneda':          g(root, 'Moneda', 'moneda') or 'MXN',
            'tipo_cambio':     g(root, 'TipoCambio') or '1',
            'emisor_rfc':      g(em,  'Rfc', 'rfc')     if em  is not None else '',
            'emisor_nombre':   g(em,  'Nombre', 'nombre') if em is not None else '',
            'receptor_rfc':    g(re_, 'Rfc', 'rfc')      if re_ is not None else '',
            'receptor_nombre': g(re_, 'Nombre', 'nombre') if re_ is not None else '',
            'uso_cfdi':        g(re_, 'UsoCFDI', 'usoCFDI') if re_ is not None else '',
            'xml': xml_str if isinstance(xml_str, str) else xml_str.decode('utf-8', 'replace'),
        }
    except Exception as e:
        return {'archivo': filename, 'uuid': '', 'error': str(e),
                'xml': xml_str if isinstance(xml_str, str) else ''}

# ── Endpoints ─────────────────────────────────────────────────────
@app.route('/health')
def health():
    return jsonify({'ok': True, 'servicio': 'SAT Descarga Masiva', 'puerto': 5050})

@app.route('/login', methods=['POST'])
def login():
    try:
        d = request.get_json(force=True) or {}
        cer, kpem = get_fiel(d)
        tok = Login.TokenRequest().soapRequest(certificate=cer, keyPEM=kpem)
        if tok:
            rfc = Utils.rfc_from_certificate(cer)
            return jsonify({'ok': True, 'token': tok, 'rfc': rfc})
        return jsonify({'ok': False, 'error': 'No se obtuvo token. Verifica FIEL y contraseña.'}), 400
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/solicitar', methods=['POST'])
def solicitar():
    try:
        d = request.get_json(force=True) or {}
        cer, kpem = get_fiel(d)
        dt_i = datetime.strptime(d['fecha_inicio'], '%Y-%m-%d').replace(hour=0,  minute=0,  second=0)
        dt_f = datetime.strptime(d['fecha_fin'],    '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        r = SatReq.RequestDownloadRequest().soapRequest(
            certificate=cer, keyPEM=kpem, token=d['token'],
            start_date=dt_i, end_date=dt_f, tipo_solicitud=d.get('tipo', 'CFDI'))
        return jsonify({'ok': True, 'solicitud': dict(r)})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/verificar', methods=['POST'])
def verificar():
    try:
        d = request.get_json(force=True) or {}
        cer, kpem = get_fiel(d)
        r = Verify.VerifyRequest().soapRequest(
            certificate=cer, keyPEM=kpem,
            token=d['token'], id_solicitud=d['id_solicitud'])
        return jsonify({'ok': True, 'listo': r.ready,
                        'paquetes': r.paquetes or [], 'error_info': r.error})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/descargar', methods=['POST'])
def descargar():
    try:
        d = request.get_json(force=True) or {}
        cer, kpem = get_fiel(d)
        zip_path = Download.DownloadRequest().soapRequest(
            certificate=cer, keyPEM=kpem,
            token=d['token'], id_paquete=d['id_paquete'], path='/tmp/sat_pkg_')
        if not zip_path:
            return jsonify({'ok': False,
                           'error': 'No se pudo descargar. Token expirado o paquete inválido.'}), 400
        cfdis = []
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for name in zf.namelist():
                    if name.lower().endswith('.xml'):
                        raw = zf.read(name)
                        cfdis.append(parse_cfdi(raw.decode('utf-8', 'replace'), name))
        finally:
            try: os.remove(zip_path)
            except: pass
        return jsonify({'ok': True, 'paquete': d['id_paquete'],
                        'total': len(cfdis), 'cfdis': cfdis})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'ok': False, 'error': str(e)}), 500

if __name__ == '__main__':
    print('🏛  SAT Microservicio iniciando en puerto 5050...')
    app.run(host='0.0.0.0', port=5050, debug=False)
