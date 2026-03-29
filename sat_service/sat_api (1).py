"""
SAT Descarga Masiva + Timbrado + Cancelación + Validación
Microservicio satcfdi v2.0 — Puerto 5050

Instalar:
    pip install satcfdi aiohttp lxml

Para timbrar en PRODUCCIÓN se necesita un PAC:
    - Finkok:   https://finkok.com
    - SW Sapien: https://sw.com.mx
    - Diverza:  https://diverza.com
Configura las credenciales del PAC en el .env del proyecto.
"""
import sys, os, asyncio, io, base64, zipfile, json, logging, tempfile
from datetime import datetime
from decimal import Decimal

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('sat_api')

_missing = []
for _pkg, _imp in [('aiohttp','aiohttp'),('satcfdi','satcfdi'),('lxml','lxml')]:
    try: __import__(_imp)
    except ImportError: _missing.append(_pkg)
if _missing:
    logger.error(f'Falta instalar: pip install {" ".join(_missing)}')
    sys.exit(1)

from satcfdi.models import Signer
from satcfdi.pacs.sat import SAT, TipoDescargaMasivaTerceros, EstadoSolicitud, EstadoComprobante, Environment
from satcfdi.pacs import CancelReason
from satcfdi.cfdi import CFDI
from satcfdi.create.cfd import cfdi40
import aiohttp
from aiohttp import web

# ── PAC configurado desde variables de entorno ───────────────────
def get_pac(signer: Signer, environment=Environment.TEST):
    """
    Retorna el PAC configurado. En pruebas usa SAT directo.
    En producción usa Finkok, SWSapien, etc. según el .env
    """
    pac_name = os.environ.get('PAC_NOMBRE', '').lower()

    if environment == Environment.TEST or not pac_name:
        # Modo prueba: SAT directo (solo valida, no timbra realmente)
        return SAT(signer=signer, environment=environment)

    if pac_name == 'finkok':
        from satcfdi.pacs.finkok import Finkok
        return Finkok(
            username=os.environ.get('PAC_USUARIO', ''),
            password=os.environ.get('PAC_PASSWORD', ''),
            environment=environment
        )
    elif pac_name == 'swsapien':
        from satcfdi.pacs.swsapien import SWSapien
        return SWSapien(
            token=os.environ.get('PAC_TOKEN', ''),
            environment=environment
        )
    elif pac_name == 'diverza':
        from satcfdi.pacs.diverza import Diverza
        return Diverza(
            username=os.environ.get('PAC_USUARIO', ''),
            password=os.environ.get('PAC_PASSWORD', ''),
            environment=environment
        )
    else:
        return SAT(signer=signer, environment=environment)


# ── Helpers ──────────────────────────────────────────────────────
def get_signer(data: dict) -> Signer:
    cer_b64  = data.get('cer', '')
    key_b64  = data.get('key', '')
    password = data.get('password', '')
    if ',' in cer_b64: cer_b64 = cer_b64.split(',')[1]
    if ',' in key_b64: key_b64 = key_b64.split(',')[1]
    if not cer_b64 or not key_b64:
        raise ValueError('Se requieren cer y key en base64')
    return Signer.load(
        certificate=base64.b64decode(cer_b64),
        key=base64.b64decode(key_b64),
        password=password
    )


def parse_cfdi_xml(xml_str, filename=''):
    try:
        from lxml import etree
        NS4='http://www.sat.gob.mx/cfd/4'; NS3='http://www.sat.gob.mx/cfd/3'
        TFD='http://www.sat.gob.mx/TimbreFiscalDigital'
        raw  = xml_str.encode('utf-8') if isinstance(xml_str, str) else xml_str
        root = etree.fromstring(raw)
        def g(el,*attrs):
            if el is None: return ''
            for a in attrs:
                v = el.get(a)
                if v: return v
            return ''
        em  = root.find(f'.//{{{NS4}}}Emisor')   or root.find(f'.//{{{NS3}}}Emisor')
        rec = root.find(f'.//{{{NS4}}}Receptor')  or root.find(f'.//{{{NS3}}}Receptor')
        tf  = root.find(f'.//{{{TFD}}}TimbreFiscalDigital')
        imp = root.find(f'.//{{{NS4}}}Impuestos') or root.find(f'.//{{{NS3}}}Impuestos')
        iva = isr = '0'
        if imp is not None:
            tr = imp.find(f'.//{{{NS4}}}Traslado')  or imp.find('.//Traslado')
            rt = imp.find(f'.//{{{NS4}}}Retencion') or imp.find('.//Retencion')
            if tr is not None: iva = g(tr,'Importe','importe') or '0'
            if rt is not None: isr = g(rt,'Importe','importe') or '0'
        return {
            'archivo': filename, 'uuid': g(tf,'UUID','Uuid') if tf is not None else '',
            'fecha': g(root,'Fecha','fecha'), 'tipo': g(root,'TipoDeComprobante','tipoDeComprobante'),
            'subtotal': g(root,'SubTotal','subTotal'), 'iva': iva, 'isr_ret': isr,
            'total': g(root,'Total','total'), 'moneda': g(root,'Moneda','moneda') or 'MXN',
            'emisor_rfc': g(em,'Rfc','rfc') if em is not None else '',
            'emisor_nombre': g(em,'Nombre','nombre') if em is not None else '',
            'receptor_rfc': g(rec,'Rfc','rfc') if rec is not None else '',
            'receptor_nombre': g(rec,'Nombre','nombre') if rec is not None else '',
            'uso_cfdi': g(rec,'UsoCFDI','usoCFDI') if rec is not None else '',
            'xml': xml_str if isinstance(xml_str, str) else xml_str.decode('utf-8','replace'),
        }
    except Exception as e:
        logger.exception('Error parseando CFDI')
        return {'archivo': filename, 'uuid': '', 'error': str(e),
                'xml': xml_str if isinstance(xml_str, str) else ''}


def json_resp(data, status=200):
    return web.Response(text=json.dumps(data, ensure_ascii=False, default=str),
        content_type='application/json', status=status)

def estado_num(e): return e.value if hasattr(e,'value') else int(e or 0)
def estado_str(e): return {1:'Aceptada',2:'En proceso',3:'Terminada',
    4:'Error',5:'Rechazada',6:'Vencida'}.get(estado_num(e), str(e))


# ════════════════════════════════════════════════════════════════
# DESCARGA MASIVA
# ════════════════════════════════════════════════════════════════
async def health(request):
    pac_cfg = os.environ.get('PAC_NOMBRE', 'SAT directo (pruebas)')
    return json_resp({'ok': True, 'servicio': 'SAT satcfdi v2.0',
        'puerto': 5050, 'pac': pac_cfg})

async def login(request):
    try:
        d = await request.json()
        loop = asyncio.get_event_loop()
        signer = get_signer(d)
        sat = SAT(signer=signer)
        token = await loop.run_in_executor(None, lambda: sat._get_token_comprobante())
        logger.info(f'Login OK: {signer.rfc}')
        return json_resp({'ok': True, 'token': token, 'rfc': signer.rfc})
    except Exception as e:
        logger.exception('Error /login')
        return json_resp({'ok': False, 'error': str(e)}, 500)

async def verify_token(request):
    try:
        d = await request.json()
        loop = asyncio.get_event_loop()
        signer = get_signer(d)
        sat = SAT(signer=signer)
        token = await loop.run_in_executor(None, lambda: sat._get_token_comprobante())
        return json_resp({'ok': True, 'token_valido': bool(token), 'rfc': signer.rfc})
    except Exception as e:
        return json_resp({'ok': True, 'token_valido': False, 'error': str(e)})

async def solicitar(request):
    try:
        d = await request.json()
        loop = asyncio.get_event_loop()
        signer = get_signer(d)
        sat = SAT(signer=signer)
        fi = datetime.strptime(d['fecha_inicio'],'%Y-%m-%d').replace(hour=0,  minute=0,  second=0)
        ff = datetime.strptime(d['fecha_fin'],   '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        tipo = d.get('tipo','CFDI')
        # SAT v1.5:
        # - tipo_solicitud como string directo (enum envía "Metadata" en minúsculas)
        # - EstadoComprobante=Vigente SOLO para CFDI XML, no para METADATA
        tipo_sol_str = 'CFDI' if tipo == 'CFDI' else 'METADATA'
        estado_comp  = EstadoComprobante.VIGENTE if tipo == 'CFDI' else None

        def _solicitar():
            # satcfdi 4.9.x envía METADATA='Metadata' pero SAT v1.5 requiere 'METADATA'
            # Parcheamos el valor del enum temporalmente
            from satcfdi.pacs.sat import TipoDescargaMasivaTerceros as _T
            _orig = _T.METADATA._value_
            try:
                _T.METADATA._value_ = 'METADATA'
                return sat.recover_comprobante_received_request(
                    fecha_inicial      = fi,
                    fecha_final        = ff,
                    rfc_receptor       = signer.rfc,
                    tipo_solicitud     = _T.METADATA if tipo != 'CFDI' else _T.CFDI,
                    estado_comprobante = estado_comp,
                )
            finally:
                _T.METADATA._value_ = _orig

        r = await loop.run_in_executor(None, _solicitar)
        id_sol = r.get('IdSolicitud','')
        cod = r.get('CodEstatus','')
        msg = r.get('Mensaje','')
        logger.info(f'Solicitud: fi={fi} ff={ff} tipo={tipo} rfc={signer.rfc}')
        logger.info(f'Respuesta completa: {r}')
        logger.info(f'Solicitud: {id_sol} | {cod} | {msg}')
        return json_resp({'ok': cod=='5000',
            'solicitud': {'IdSolicitud': id_sol, 'CodEstatus': cod, 'Mensaje': msg}})
    except Exception as e:
        import traceback as _tb
        _err_detail = _tb.format_exc()
        logger.error('Error /solicitar completo:\n' + _err_detail)
        return json_resp({'ok': False, 'error': str(e), 'detalle': _err_detail}, 500)

async def solicitar_emitidos(request):
    """Solicita descarga masiva de CFDIs EMITIDOS por el RFC de la FIEL."""
    try:
        d    = await request.json()
        loop = asyncio.get_event_loop()
        signer = get_signer(d)
        sat    = SAT(signer=signer)
        fi   = datetime.strptime(d['fecha_inicio'],'%Y-%m-%d').replace(hour=0,  minute=0,  second=0)
        ff   = datetime.strptime(d['fecha_fin'],   '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        tipo = d.get('tipo','CFDI')
        tipo_sol_str = 'CFDI' if tipo == 'CFDI' else 'METADATA'
        # Para emitidos EstadoComprobante puede ser Todos
        estado_comp = None  # sin filtro de estado para emitidos

        def _solicitar_emi():
            from satcfdi.pacs.sat import TipoDescargaMasivaTerceros as _T
            _orig = _T.METADATA._value_
            try:
                _T.METADATA._value_ = 'METADATA'
                return sat.recover_comprobante_emitted_request(
                    fecha_inicial      = fi,
                    fecha_final        = ff,
                    rfc_emisor         = signer.rfc,
                    tipo_solicitud     = _T.METADATA if tipo != 'CFDI' else _T.CFDI,
                    estado_comprobante = estado_comp,
                )
            finally:
                _T.METADATA._value_ = _orig

        r = await loop.run_in_executor(None, _solicitar_emi)
        id_sol = r.get('IdSolicitud','')
        cod    = r.get('CodEstatus','')
        msg    = r.get('Mensaje','')
        logger.info(f'Solicitud EMITIDOS: {id_sol} | {cod} | {msg}')
        return json_resp({'ok': cod=='5000',
            'solicitud': {'IdSolicitud': id_sol, 'CodEstatus': cod, 'Mensaje': msg}})
    except Exception as e:
        import traceback as _tb
        logger.error('Error /solicitar-emitidos:\n' + _tb.format_exc())
        return json_resp({'ok': False, 'error': str(e), 'detalle': _tb.format_exc()}, 500)


async def verificar(request):
    try:
        d = await request.json()
        loop = asyncio.get_event_loop()
        signer = get_signer(d)
        sat = SAT(signer=signer)
        r = await loop.run_in_executor(None, lambda:
            sat.recover_comprobante_status(d['id_solicitud']))
        estado = r.get('EstadoSolicitud')
        paquetes = r.get('IdsPaquetes',[]) or []
        listo = estado_num(estado) == EstadoSolicitud.TERMINADA.value
        logger.info(f'Verificar {d["id_solicitud"]}: {estado_str(estado)} paq={len(paquetes)}')
        return json_resp({'ok': True, 'listo': listo,
            'estado': estado_str(estado), 'estado_num': estado_num(estado),
            'paquetes': paquetes, 'num_cfdis': r.get('NumeroCFDIs',0),
            'cod_estatus': r.get('CodEstatus',''), 'mensaje': r.get('Mensaje',''),
            'error_info': None})
    except Exception as e:
        logger.exception('Error /verificar')
        return json_resp({'ok': False, 'error': str(e)}, 500)

def parse_metadata_zip(paq_bytes: bytes) -> list:
    """
    Parsea un ZIP de METADATA del SAT.
    El SAT envía un .txt delimitado por | con encoding Windows-1252.
    """
    registros = []
    try:
        with zipfile.ZipFile(io.BytesIO(paq_bytes), 'r') as zf:
            for name in zf.namelist():
                raw_bytes = zf.read(name)
                # SAT usa Windows-1252 (latin-1) para los metadatos
                for enc in ('windows-1252', 'latin-1', 'utf-8-sig', 'utf-8'):
                    try:
                        raw = raw_bytes.decode(enc)
                        break
                    except Exception:
                        continue
                else:
                    raw = raw_bytes.decode('utf-8', 'replace')

                lineas = raw.splitlines()
                logger.info(f'Metadata archivo={name} lineas={len(lineas)} enc={enc}')
                if len(lineas) < 2:
                    logger.warning(f'Archivo vacío o sin datos: {name}')
                    continue

                # Detectar separador: SAT usa ~ (tilde) en v1.5
                primera = lineas[0]
                sep = '~' if '~' in primera else '|'
                headers = [h.strip() for h in primera.split(sep)]
                logger.info(f'Headers ({sep}): {headers}')

                for linea in lineas[1:]:
                    if not linea.strip():
                        continue
                    valores = [v.strip() for v in linea.split(sep)]
                    while len(valores) < len(headers):
                        valores.append('')
                    reg = dict(zip(headers, valores))

                    # Mapear campos SAT v1.5
                    efecto  = reg.get('EfectoComprobante', '')
                    # Estatus: 1=Vigente, 2=Cancelado (SAT v1.5 usa números)
                    est_raw = reg.get('Estatus', '')
                    estatus = {'1':'Vigente','2':'Cancelado'}.get(est_raw, est_raw)

                    registros.append({
                        'uuid':               reg.get('Uuid',               reg.get('UUID', '')),
                        'rfc_emisor':         reg.get('RfcEmisor',          ''),
                        'nombre_emisor':      reg.get('NombreEmisor',       ''),
                        'rfc_receptor':       reg.get('RfcReceptor',        ''),
                        'nombre_receptor':    reg.get('NombreReceptor',     ''),
                        'rfc_pac':            reg.get('PacCertifico',       reg.get('RfcPac', '')),
                        'fecha_emision':      reg.get('FechaEmision',       ''),
                        'fecha_certificacion':reg.get('FechaCertificacionSat', ''),
                        'monto':              reg.get('Monto',              '0'),
                        'efecto':             efecto,
                        'tipo':               efecto,  # I=Ingreso E=Egreso T=Traslado N=Nomina P=Pago
                        'estatus':            estatus,
                        'fecha_cancelacion':  reg.get('FechaCancelacion',   ''),
                        'raw':                reg,
                    })
    except Exception as e:
        logger.exception('Error parseando metadata ZIP')
    return registros


async def descargar(request):
    """Descarga un paquete ZIP — soporta CFDI (XML) y METADATA."""
    try:
        d      = await request.json()
        loop   = asyncio.get_event_loop()
        signer = get_signer(d)
        sat    = SAT(signer=signer)
        id_paq = d['id_paquete']
        tipo   = d.get('tipo', 'CFDI')   # 'CFDI' o 'METADATA'

        r, paq_b64 = await loop.run_in_executor(None, lambda:
            sat.recover_comprobante_download(id_paquete=id_paq))

        if not paq_b64:
            return json_resp({'ok': False, 'error': 'No se pudo descargar el paquete.'}, 400)

        paq_bytes = base64.b64decode(paq_b64)

        # ── METADATA ──────────────────────────────────────────────
        if tipo == 'METADATA':
            registros = parse_metadata_zip(paq_bytes)
            logger.info(f'Metadata {id_paq}: {len(registros)} registros')
            return json_resp({'ok': True, 'paquete': id_paq, 'tipo': 'METADATA',
                              'total': len(registros), 'metadatos': registros})

        # ── CFDI XML ──────────────────────────────────────────────
        cfdis = []
        with zipfile.ZipFile(io.BytesIO(paq_bytes), 'r') as zf:
            for name in zf.namelist():
                if name.lower().endswith('.xml'):
                    cfdis.append(parse_cfdi_xml(zf.read(name).decode('utf-8','replace'), name))

        logger.info(f'Paquete {id_paq}: {len(cfdis)} CFDIs')
        return json_resp({'ok': True, 'paquete': id_paq, 'tipo': 'CFDI',
                          'total': len(cfdis), 'cfdis': cfdis})
    except Exception as e:
        logger.exception('Error /descargar')
        return json_resp({'ok': False, 'error': str(e)}, 500)

async def validar_cfdi(request):
    """
    Valida si un CFDI está vigente consultando el servicio público del SAT.
    No requiere autenticación FIEL.
    """
    try:
        d = await request.json()
        rfc_emisor   = d.get('rfc_emisor','')
        rfc_receptor = d.get('rfc_receptor','')
        total        = d.get('total','')
        uuid         = d.get('uuid','')

        if not all([rfc_emisor, rfc_receptor, total, uuid]):
            return json_resp({'ok': False, 'error': 'Se requieren: rfc_emisor, rfc_receptor, total, uuid'}, 400)

        loop = asyncio.get_event_loop()

        # Consulta directa al webservice público del SAT
        import requests as req_lib
        url = 'https://consultaqr.facturaelectronica.sat.gob.mx/ConsultaCFDIService.svc'
        expresion = f'?re={rfc_emisor}&rr={rfc_receptor}&tt={total}&id={uuid}'
        soap_body = (
            '<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:tem="http://tempuri.org/">'
            '<Body><tem:Consulta><tem:expresionImpresa>'
            f'<![CDATA[{expresion}]]>'
            '</tem:expresionImpresa></tem:Consulta></Body></Envelope>'
        )
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction':   'http://tempuri.org/IConsultaCFDIService/Consulta',
        }

        def _consultar():
            resp = req_lib.post(url, data=soap_body.encode('utf-8'),
                                headers=headers, timeout=15)
            return resp.text

        xml_resp = await loop.run_in_executor(None, _consultar)

        from lxml import etree
        root = etree.fromstring(xml_resp.encode('utf-8'))

        def get_text(tag):
            el = root.find(f'.//*{{*}}{tag}')
            return el.text if el is not None else ''

        estado        = get_text('Estado')
        cancelable    = get_text('EsCancelable')
        estado_cancel = get_text('EstatusCancelacion')
        codigo        = get_text('CodigoEstatus')

        vigente = estado == 'Vigente'
        logger.info(f'Validación UUID {uuid}: {estado} | {codigo}')

        return json_resp({
            'ok':              True,
            'uuid':            uuid,
            'estado':          estado,          # Vigente | Cancelado | No encontrado
            'vigente':         vigente,
            'es_cancelable':   cancelable,
            'estado_cancel':   estado_cancel,
            'codigo_estatus':  codigo,
        })
    except Exception as e:
        logger.exception('Error /validar-cfdi')
        return json_resp({'ok': False, 'error': str(e)}, 500)


# ════════════════════════════════════════════════════════════════
# TIMBRAR CFDI 4.0
# Endpoint: POST /timbrar
# Requiere CSD (Certificado de Sello Digital) y PAC configurado
# En pruebas: usa el ambiente de pruebas del PAC o SAT
# ════════════════════════════════════════════════════════════════
async def timbrar(request):
    """
    Genera un CFDI 4.0 de Ingreso firmado con el CSD o FIEL.
    - Sin PAC configurado: devuelve el XML pre-timbrado (sin UUID) para revisión.
    - Con PAC configurado: timbra completamente y devuelve UUID.
    """
    try:
        d    = await request.json()
        loop = asyncio.get_event_loop()

        # CSD del emisor (Certificado de Sello Digital, diferente a la FIEL)
        signer = get_signer(d)

        # Ambiente: pruebas por defecto
        env_str = d.get('ambiente', os.environ.get('PAC_AMBIENTE', 'pruebas'))
        environment = Environment.PRODUCTION if env_str == 'produccion' else Environment.TEST
        pac = get_pac(signer, environment)

        factura = d.get('factura', {})

        # Construir CFDI 4.0
        emisor = cfdi40.Emisor(
            rfc            = signer.rfc,
            nombre         = factura.get('emisor_nombre', signer.legal_name if hasattr(signer,'legal_name') else ''),
            regimen_fiscal = factura.get('regimen_fiscal', '601'),
        )

        receptor = cfdi40.Receptor(
            rfc                       = factura.get('rfc_receptor',''),
            nombre                    = factura.get('nombre_receptor',''),
            domicilio_fiscal_receptor = factura.get('cp_receptor',''),
            regimen_fiscal_receptor   = factura.get('regimen_receptor','616'),
            uso_cfdi                  = factura.get('uso_cfdi','G03'),
        )

        # Conceptos
        conceptos_raw = factura.get('conceptos', [])
        conceptos = []
        for c in conceptos_raw:
            valor_unitario = Decimal(str(c.get('precio_unitario', 0)))
            cantidad       = Decimal(str(c.get('cantidad', 1)))
            tasa_iva       = Decimal(str(c.get('tasa_iva', '0.16')))

            impuestos = None
            if tasa_iva > 0:
                base_imp = valor_unitario * cantidad
                impuestos = cfdi40.Impuestos(
                    traslados=cfdi40.Traslado(
                        impuesto   = '002',        # IVA
                        tipo_factor= 'Tasa',
                        tasa_o_cuota = tasa_iva,
                        importe    = (base_imp * tasa_iva).quantize(Decimal('0.01')),
                        base       = base_imp,
                    )
                )

            conceptos.append(cfdi40.Concepto(
                clave_prod_serv = c.get('clave_prod_serv', '84111506'),
                cantidad        = cantidad,
                clave_unidad    = c.get('clave_unidad', 'E48'),
                descripcion     = c.get('descripcion', ''),
                valor_unitario  = valor_unitario,
                objeto_imp      = '02' if tasa_iva > 0 else '01',
                impuestos       = impuestos,
            ))

        # Crear comprobante
        comprobante = cfdi40.Comprobante(
            emisor            = emisor,
            receptor          = receptor,
            lugar_expedicion  = factura.get('cp_expedicion', ''),
            conceptos         = conceptos,
            moneda            = factura.get('moneda', 'MXN'),
            tipo_de_comprobante = factura.get('tipo', 'I'),
            forma_pago        = factura.get('forma_pago', '99'),
            metodo_pago       = factura.get('metodo_pago', 'PPD'),
            serie             = factura.get('serie', None),
            folio             = factura.get('folio', None),
        )

        # Firmar con CSD/FIEL
        comprobante.sign(signer)

        # Intentar timbrar con PAC — si no hay PAC, devolver XML pre-firmado
        timbrado = False
        uuid_cfdi = ''
        try:
            def _timbrar():
                return pac.stamp(comprobante)
            doc = await loop.run_in_executor(None, _timbrar)
            xml_bytes = doc.xml_bytes() if hasattr(doc,'xml_bytes') else bytes(doc)
            xml_str   = xml_bytes.decode('utf-8')
            timbrado  = True
            cfdi_data = parse_cfdi_xml(xml_str)
            uuid_cfdi = cfdi_data.get('uuid','')
            logger.info(f'CFDI timbrado con PAC: {uuid_cfdi}')
        except NotImplementedError:
            # Sin PAC — devolver XML pre-firmado para revisión de estructura
            xml_bytes = comprobante.xml_bytes()
            xml_str   = xml_bytes.decode('utf-8')
            cfdi_data = parse_cfdi_xml(xml_str)
            logger.info(f'XML generado sin timbrar (sin PAC): {signer.rfc}')

        return json_resp({
            'ok':      True,
            'timbrado': timbrado,
            'uuid':    uuid_cfdi,
            'xml':     xml_str,
            'cfdi':    cfdi_data,
            'nota':    None if timbrado else
                       'XML generado y firmado con CSD. Para timbrar configura un PAC en el .env '
                       '(PAC_NOMBRE=finkok, PAC_USUARIO, PAC_PASSWORD)',
        })
    except Exception as e:
        logger.exception('Error /timbrar')
        return json_resp({'ok': False, 'error': str(e)}, 500)


# ════════════════════════════════════════════════════════════════
# CANCELAR CFDI
# Endpoint: POST /cancelar
# Body: { cer, key, password, uuid, rfc_receptor, total, motivo, uuid_sustitucion? }
# ════════════════════════════════════════════════════════════════
async def cancelar(request):
    """
    Cancela un CFDI emitido ante el SAT.
    Requiere la FIEL del emisor y el XML original timbrado.
    """
    try:
        d    = await request.json()
        loop = asyncio.get_event_loop()

        signer = get_signer(d)
        env_str = d.get('ambiente', os.environ.get('PAC_AMBIENTE', 'pruebas'))
        environment = Environment.PRODUCTION if env_str == 'produccion' else Environment.TEST
        pac = get_pac(signer, environment)

        # XML del CFDI a cancelar (debe ser el timbrado completo)
        xml_str = d.get('xml', '')
        if not xml_str:
            return json_resp({'ok': False, 'error': 'Se requiere el XML del CFDI a cancelar'}, 400)

        cfdi_obj = CFDI.from_string(xml_str)

        # Motivo de cancelación
        motivo_str = d.get('motivo', '02')
        motivo_map = {
            '01': CancelReason.COMPROBANTE_EMITIDO_CON_ERRORES_CON_RELACION,
            '02': CancelReason.COMPROBANTE_EMITIDO_CON_ERRORES_SIN_RELACION,
            '03': CancelReason.NO_SE_LLEVO_A_CABO_LA_OPERACION,
            '04': CancelReason.OPERACION_NORMATIVA_RELACIONADA_EN_LA_FACTURA_GLOBAL,
        }
        motivo = motivo_map.get(motivo_str, CancelReason.COMPROBANTE_EMITIDO_CON_ERRORES_SIN_RELACION)
        uuid_sust = d.get('uuid_sustitucion', None)

        def _cancelar():
            return pac.cancel(cfdi_obj, reason=motivo, substitution_id=uuid_sust, signer=signer)

        resultado = await loop.run_in_executor(None, _cancelar)
        uuid = cfdi_obj.get('Complemento',{}).get('TimbreFiscalDigital',{}).get('UUID','')
        logger.info(f'CFDI cancelado: {uuid}')

        return json_resp({
            'ok':        True,
            'uuid':      uuid,
            'resultado': str(resultado),
        })
    except NotImplementedError:
        return json_resp({'ok': False,
            'error': 'Cancelación no disponible. Configura un PAC en el .env'}, 501)
    except Exception as e:
        logger.exception('Error /cancelar')
        return json_resp({'ok': False, 'error': str(e)}, 500)


# ════════════════════════════════════════════════════════════════
# VINCULAR CFDI DESCARGADO A ORDEN DE COMPRA
# Endpoint: POST /vincular-cfdi
# Body: { xml_cfdi } — extrae datos y los devuelve para guardar en BD
# ════════════════════════════════════════════════════════════════
async def vincular_cfdi(request):
    """
    Parsea un XML de CFDI descargado y devuelve sus campos
    para vincularlos a una Orden de Compra en la BD del ERP.
    El server.js toma estos datos y los guarda en ordenes_proveedor.
    """
    try:
        d = await request.json()
        xml_str = d.get('xml', '')
        if not xml_str:
            return json_resp({'ok': False, 'error': 'Se requiere el XML del CFDI'}, 400)

        cfdi_data = parse_cfdi_xml(xml_str)
        cfdi_data['ok'] = True
        return json_resp(cfdi_data)
    except Exception as e:
        logger.exception('Error /vincular-cfdi')
        return json_resp({'ok': False, 'error': str(e)}, 500)


# ════════════════════════════════════════════════════════════════
# PAC INFO — estado de configuración
# ════════════════════════════════════════════════════════════════
async def pac_info(request):
    pac_nombre  = os.environ.get('PAC_NOMBRE', '')
    pac_usuario = os.environ.get('PAC_USUARIO', '')
    pac_ambiente= os.environ.get('PAC_AMBIENTE', 'pruebas')
    configurado = bool(pac_nombre and pac_usuario)
    return json_resp({
        'ok':         True,
        'pac':        pac_nombre or 'No configurado',
        'ambiente':   pac_ambiente,
        'configurado':configurado,
        'nota': 'Para timbrar en producción configura PAC_NOMBRE, PAC_USUARIO y PAC_PASSWORD en el .env'
               if not configurado else f'PAC {pac_nombre} configurado para {pac_ambiente}',
    })


# ════════════════════════════════════════════════════════════════
# GENERAR XML CFDI 4.0 SIN TIMBRAR (para revisión de estructura)
# Endpoint: POST /generar-xml
# Genera el XML completo firmado con CSD/FIEL, listo para PAC
# ════════════════════════════════════════════════════════════════
async def generar_xml(request):
    """
    Genera un CFDI 4.0 firmado sin enviarlo al PAC.
    Útil para revisar estructura antes de contratar un PAC.
    Acepta CSD o FIEL como certificado firmante.
    """
    try:
        d    = await request.json()
        loop = asyncio.get_event_loop()

        signer   = get_signer(d)
        factura  = d.get('factura', {})

        emisor = cfdi40.Emisor(
            rfc            = signer.rfc,
            nombre         = factura.get('emisor_nombre') or signer.rfc,
            regimen_fiscal = factura.get('regimen_fiscal', '601'),
        )

        receptor = cfdi40.Receptor(
            rfc                       = factura.get('rfc_receptor', 'XAXX010101000'),
            nombre                    = factura.get('nombre_receptor', 'PUBLICO EN GENERAL'),
            domicilio_fiscal_receptor = factura.get('cp_receptor', ''),
            regimen_fiscal_receptor   = factura.get('regimen_receptor', '616'),
            uso_cfdi                  = factura.get('uso_cfdi', 'G03'),
        )

        # Construir conceptos con IVA 16%
        conceptos = []
        for c in factura.get('conceptos', []):
            valor_unitario = Decimal(str(c.get('precio_unitario', 0)))
            cantidad       = Decimal(str(c.get('cantidad', 1)))
            tasa_iva       = Decimal(str(c.get('tasa_iva', '0.16')))
            base_imp       = (valor_unitario * cantidad).quantize(Decimal('0.01'))

            impuestos = None
            if tasa_iva > 0:
                impuestos = cfdi40.Impuestos(
                    traslados=cfdi40.Traslado(
                        impuesto     = '002',
                        tipo_factor  = 'Tasa',
                        tasa_o_cuota = tasa_iva,
                        importe      = (base_imp * tasa_iva).quantize(Decimal('0.01')),
                        base         = base_imp,
                    )
                )

            conceptos.append(cfdi40.Concepto(
                clave_prod_serv = c.get('clave_prod_serv', '84111506'),
                cantidad        = cantidad,
                clave_unidad    = c.get('clave_unidad', 'E48'),
                descripcion     = c.get('descripcion', 'Servicio'),
                valor_unitario  = valor_unitario,
                objeto_imp      = '02' if tasa_iva > 0 else '01',
                impuestos       = impuestos,
            ))

        comprobante = cfdi40.Comprobante(
            emisor              = emisor,
            receptor            = receptor,
            lugar_expedicion    = factura.get('cp_expedicion', ''),
            conceptos           = conceptos,
            moneda              = factura.get('moneda', 'MXN'),
            tipo_de_comprobante = factura.get('tipo', 'I'),
            forma_pago          = factura.get('forma_pago', '99'),
            metodo_pago         = factura.get('metodo_pago', 'PPD'),
            serie               = factura.get('serie') or None,
            folio               = factura.get('folio') or None,
        )

        # Firmar con CSD o FIEL
        def _firmar():
            comprobante.sign(signer)
            return comprobante.xml_bytes().decode('utf-8')

        xml_str = await loop.run_in_executor(None, _firmar)

        # Calcular totales para mostrar resumen
        subtotal = sum(
            Decimal(str(c.get('precio_unitario',0))) * Decimal(str(c.get('cantidad',1)))
            for c in factura.get('conceptos',[])
        )
        iva_total = (subtotal * Decimal('0.16')).quantize(Decimal('0.01'))
        total     = (subtotal + iva_total).quantize(Decimal('0.01'))

        logger.info(f'XML generado: RFC={signer.rfc} total={total}')

        return json_resp({
            'ok':       True,
            'xml':      xml_str,
            'resumen': {
                'rfc_emisor':    signer.rfc,
                'rfc_receptor':  factura.get('rfc_receptor',''),
                'subtotal':      str(subtotal),
                'iva':           str(iva_total),
                'total':         str(total),
                'moneda':        factura.get('moneda','MXN'),
                'num_conceptos': len(conceptos),
                'serie_folio':   f"{factura.get('serie','')}-{factura.get('folio','')}",
            },
            'nota': 'XML generado y firmado. Falta timbrar con un PAC para ser válido ante el SAT.',
        })
    except Exception as e:
        logger.exception('Error /generar-xml')
        return json_resp({'ok': False, 'error': str(e)}, 500)


async def descargar_uuid(request):
    """
    Descarga el XML de un CFDI específico por UUID.
    Flujo automático: solicitar -> verificar (polling) -> descargar.
    Puede tardar de segundos a minutos según el SAT.
    """
    try:
        d      = await request.json()
        loop   = asyncio.get_event_loop()
        signer = get_signer(d)
        sat    = SAT(signer=signer)
        uuid   = d.get('uuid','').strip()

        if not uuid:
            return json_resp({'ok':False,'error':'UUID requerido'},400)

        logger.info(f'Descarga por UUID: {uuid}')

        def _solicitar():
            # Solo acepta el UUID/folio como parámetro
            return sat.recover_comprobante_uuid_request(folio=uuid)

        r_sol = await loop.run_in_executor(None, _solicitar)
        id_sol = r_sol.get('IdSolicitud','')
        cod    = r_sol.get('CodEstatus','')
        logger.info(f'UUID solicitud: {id_sol} | {cod}')

        if cod not in ('5000','5004'):  # 5004 = ya existe solicitud
            return json_resp({'ok':False,
                'error':f'SAT rechazó la solicitud: {cod} - {r_sol.get("Mensaje","")}'},400)

        # Polling hasta que esté lista (máximo 3 minutos, cada 10s)
        import time
        id_paquete = None
        for intento in range(18):  # 18 * 10s = 3 min
            await asyncio.sleep(10)
            def _verificar(sol=id_sol):
                return sat.recover_comprobante_status(sol)
            r_ver = await loop.run_in_executor(None, _verificar)
            estado = r_ver.get('EstadoSolicitud')
            estado_n = estado.value if hasattr(estado,'value') else int(estado or 0)
            paquetes = r_ver.get('IdsPaquetes',[]) or []
            logger.info(f'UUID poll {intento+1}: estado={estado_n} paq={len(paquetes)}')

            if estado_n == 3 and paquetes:  # Terminada
                id_paquete = paquetes[0]
                break
            elif estado_n in (4,5,6):  # Error/Rechazada/Vencida
                return json_resp({'ok':False,
                    'error':f'SAT error en verificación: estado={estado_n}'},400)

        if not id_paquete:
            return json_resp({'ok':False,
                'error':'El SAT tardó demasiado. Intenta de nuevo en unos minutos.',
                'id_solicitud': id_sol},202)

        # Descargar el paquete
        r_dl, paq_b64 = await loop.run_in_executor(None,
            lambda: sat.recover_comprobante_download(id_paquete=id_paquete))

        if not paq_b64:
            return json_resp({'ok':False,'error':'No se pudo descargar el paquete'},400)

        paq_bytes = base64.b64decode(paq_b64)

        # Extraer XML del ZIP
        xml_str = None
        with zipfile.ZipFile(io.BytesIO(paq_bytes),'r') as zf:
            for name in zf.namelist():
                if name.lower().endswith('.xml'):
                    xml_str = zf.read(name).decode('utf-8','replace')
                    break

        if not xml_str:
            return json_resp({'ok':False,'error':'ZIP vacío — sin XML'},400)

        logger.info(f'XML descargado para UUID {uuid}: {len(xml_str)} chars')
        cfdi_data = parse_cfdi_xml(xml_str)

        return json_resp({'ok':True,'uuid':uuid,'xml':xml_str,'cfdi':cfdi_data})

    except Exception as e:
        import traceback as _tb
        logger.error('Error /descargar-uuid:\n'+_tb.format_exc())
        return json_resp({'ok':False,'error':str(e),'detalle':_tb.format_exc()},500)


# ── App aiohttp ──────────────────────────────────────────────────
app = web.Application()

# Descarga masiva
app.router.add_get( '/health',       health)
app.router.add_post('/login',        login)
app.router.add_post('/verify_token', verify_token)
app.router.add_post('/solicitar',          solicitar)
app.router.add_post('/solicitar-emitidos', solicitar_emitidos)
app.router.add_post('/verificar',    verificar)
app.router.add_post('/descargar',         descargar)
app.router.add_post('/descargar-uuid',    descargar_uuid)

# Facturación
app.router.add_post('/validar-cfdi', validar_cfdi)
app.router.add_post('/timbrar',      timbrar)
app.router.add_post('/cancelar',     cancelar)
app.router.add_post('/vincular-cfdi',vincular_cfdi)
app.router.add_post('/generar-xml',   generar_xml)
app.router.add_get( '/pac-info',     pac_info)

if __name__ == '__main__':
    logger.info('SAT (satcfdi) iniciando en puerto 5050...')
    web.run_app(app, host='0.0.0.0', port=5050, print=logger.info)
