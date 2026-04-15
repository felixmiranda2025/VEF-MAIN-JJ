"""
SAT Descarga Masiva + Timbrado + Cancelación + Validación
Microservicio satcfdi v2.4 — Puerto 5050
[FIX: RfcReceptores nodo hijo v1.5, firma antes de RfcReceptores, logging completo]

Instalar:
    pip install satcfdi aiohttp lxml

Para timbrar en PRODUCCIÓN se necesita un PAC:
    - Finkok:   https://finkok.com
    - SW Sapien: https://sw.com.mx
    - Diverza:  https://diverza.com
Configura las credenciales del PAC en el .env del proyecto.
"""
import sys, os, asyncio, io, base64, zipfile, json, logging, tempfile

# Asegurar que sat_service/ esté en el path para imports directos
_SAT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SAT_DIR not in sys.path:
    sys.path.insert(0, _SAT_DIR)
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
    """Parsea CFDI 4.0/3.3 extrayendo todos los campos incluyendo conceptos, forma/método pago, claves."""
    try:
        from lxml import etree
        NS4 = 'http://www.sat.gob.mx/cfd/4'
        NS3 = 'http://www.sat.gob.mx/cfd/3'
        TFD = 'http://www.sat.gob.mx/TimbreFiscalDigital'
        raw  = xml_str.encode('utf-8') if isinstance(xml_str, str) else xml_str
        if raw.startswith(b'\xef\xbb\xbf'): raw = raw[3:]
        root = etree.fromstring(raw)
        def g(el, *attrs):
            if el is None: return ''
            for a in attrs:
                v = el.get(a)
                if v: return v
            return ''
        em  = root.find(f'.//{{{NS4}}}Emisor')   or root.find(f'.//{{{NS3}}}Emisor')
        rec = root.find(f'.//{{{NS4}}}Receptor')  or root.find(f'.//{{{NS3}}}Receptor')
        tf  = root.find(f'.//{{{TFD}}}TimbreFiscalDigital') or root.find('.//TimbreFiscalDigital')
        imp = root.find(f'.//{{{NS4}}}Impuestos') or root.find(f'.//{{{NS3}}}Impuestos')

        iva = isr = '0'
        if imp is not None:
            tr = imp.find(f'.//{{{NS4}}}Traslado')  or imp.find('.//Traslado')
            rt = imp.find(f'.//{{{NS4}}}Retencion') or imp.find('.//Retencion')
            if tr is not None: iva = g(tr,'Importe','importe') or '0'
            if rt is not None: isr = g(rt,'Importe','importe') or '0'

        # Extraer todos los conceptos con sus claves
        conceptos = []
        for ns in [NS4, NS3, '']:
            tag = f'{{{ns}}}Concepto' if ns else 'Concepto'
            items = root.findall(f'.//{tag}')
            if items:
                for c in items:
                    c_iva = '0'
                    c_imp = c.find(f'.//{{{NS4}}}Traslado') or c.find('.//Traslado')
                    if c_imp is not None: c_iva = g(c_imp,'Importe','importe') or '0'
                    conceptos.append({
                        'descripcion':     g(c,'Descripcion','descripcion'),
                        'cantidad':        g(c,'Cantidad','cantidad'),
                        'unidad':          g(c,'Unidad','unidad'),
                        'clave_unidad':    g(c,'ClaveUnidad','claveUnidad'),
                        'clave_prod_serv': g(c,'ClaveProdServ','claveProdServ'),
                        'valor_unitario':  g(c,'ValorUnitario','valorUnitario'),
                        'importe':         g(c,'Importe','importe'),
                        'descuento':       g(c,'Descuento','descuento') or '0',
                        'objeto_imp':      g(c,'ObjetoImp','objetoImp'),
                        'iva_concepto':    c_iva,
                    })
                break

        xml_out = xml_str if isinstance(xml_str, str) else xml_str.decode('utf-8','replace')
        return {
            'archivo':          filename,
            'uuid':             g(tf,'UUID','Uuid') if tf is not None else '',
            'fecha':            g(root,'Fecha','fecha'),
            'fecha_timbrado':   g(tf,'FechaTimbrado') if tf is not None else '',
            'tipo':             g(root,'TipoDeComprobante','tipoDeComprobante'),
            'serie':            g(root,'Serie','serie'),
            'folio':            g(root,'Folio','folio'),
            'subtotal':         g(root,'SubTotal','subTotal'),
            'descuento':        g(root,'Descuento','descuento') or '0',
            'iva':              iva,
            'isr_ret':          isr,
            'total':            g(root,'Total','total'),
            'moneda':           g(root,'Moneda','moneda') or 'MXN',
            'tipo_cambio':      g(root,'TipoCambio','tipoCambio') or '1',
            'forma_pago':       g(root,'FormaPago','formaPago'),
            'metodo_pago':      g(root,'MetodoPago','metodoPago'),
            'uso_cfdi':         g(rec,'UsoCFDI','usoCFDI') if rec is not None else '',
            'lugar_expedicion': g(root,'LugarExpedicion','lugarExpedicion'),
            'exportacion':      g(root,'Exportacion','exportacion'),
            'emisor_rfc':       g(em,'Rfc','rfc')           if em  is not None else '',
            'emisor_nombre':    g(em,'Nombre','nombre')     if em  is not None else '',
            'emisor_regimen':   g(em,'RegimenFiscal')       if em  is not None else '',
            'receptor_rfc':     g(rec,'Rfc','rfc')          if rec is not None else '',
            'receptor_nombre':  g(rec,'Nombre','nombre')    if rec is not None else '',
            'receptor_regimen': g(rec,'RegimenFiscalReceptor') if rec is not None else '',
            'receptor_cp':      g(rec,'DomicilioFiscalReceptor') if rec is not None else '',
            'pac_rfc':          g(tf,'RfcProvCertif')       if tf is not None else '',
            'no_certificado':   g(root,'NoCertificado','noCertificado'),
            'no_cert_sat':      g(tf,'NoCertificadoSAT')   if tf is not None else '',
            'conceptos':        conceptos,
            'num_conceptos':    len(conceptos),
            'xml':              xml_out,
        }
    except Exception as e:
        logger.exception('Error al parsear CFDI')
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
# ── Helpers SOAP inline para descarga masiva ─────────────────────
# No dependen de Request.py ni Utils.py (evita import relativo)

def _get_sat_token(cer_bytes: bytes, key_pem: str) -> str:
    """
    Obtiene token SAT via SOAP.
    Digest y firma calculados sobre nodos lxml canonicalizados (C14N exclusivo),
    igual que lo hace satcfdi internamente.
    """
    import hashlib, requests as _req
    from lxml import etree as _et
    from datetime import datetime as _dt, timedelta as _td
    from cryptography.hazmat.primitives import hashes as _h, serialization as _ser
    from cryptography.hazmat.primitives.asymmetric import padding as _pad
    from cryptography.hazmat.backends import default_backend as _db
    from OpenSSL import crypto as _cry

    # ── Firmar bytes con SHA1/RSA ────────────────────────────────
    def _sign_bytes(data_bytes):
        try:
            pk = _cry.load_privatekey(_cry.FILETYPE_PEM, key_pem)
            return base64.b64encode(_cry.sign(pk, data_bytes, 'sha1')).decode('ascii')
        except AttributeError:
            pass
        pk = _ser.load_pem_private_key(
            key_pem.encode('utf-8') if isinstance(key_pem, str) else key_pem,
            password=None, backend=_db())
        return base64.b64encode(
            pk.sign(data_bytes, _pad.PKCS1v15(), _h.SHA1())
        ).decode('ascii')

    # ── C14N exclusivo de un nodo lxml ───────────────────────────
    def _c14n(el):
        return _et.tostring(el, method='c14n', exclusive=True)

    def _digest_c14n(el):
        return base64.b64encode(hashlib.sha1(_c14n(el)).digest()).decode('ascii')

    # ── Timestamps UTC ───────────────────────────────────────────
    # Usar datetime.utcnow() — sin timezone para evitar desfase
    now     = _dt.utcnow()
    created = now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond // 1000:03d}Z'
    expires = (now + _td(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S.') + f'{(now.microsecond // 1000):03d}Z'

    b64cert = base64.b64encode(cer_bytes).decode('ascii')

    # ── Cargar template exacto de satcfdi ────────────────────────
    NS_S   = 'http://schemas.xmlsoap.org/soap/envelope/'
    NS_O   = 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'
    NS_U   = 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd'
    NS_DS  = 'http://www.w3.org/2000/09/xmldsig#'
    NS_SAT = 'http://DescargaMasivaTerceros.gob.mx'

    TEMPLATE = f"""<s:Envelope xmlns:s="{NS_S}" xmlns:o="{NS_O}" xmlns:u="{NS_U}">
<s:Header>
  <o:Security s:mustUnderstand="1">
    <u:Timestamp u:Id="_0">
      <u:Created>{created}</u:Created>
      <u:Expires>{expires}</u:Expires>
    </u:Timestamp>
    <o:BinarySecurityToken u:Id="BinarySecurityToken"
      ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3"
      EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary"
      >{b64cert}</o:BinarySecurityToken>
    <Signature xmlns="{NS_DS}">
      <SignedInfo>
        <CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
        <SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>
        <Reference URI="#_0">
          <Transforms><Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/></Transforms>
          <DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>
          <DigestValue>DIGEST_PLACEHOLDER</DigestValue>
        </Reference>
      </SignedInfo>
      <SignatureValue>SIG_PLACEHOLDER</SignatureValue>
      <KeyInfo>
        <o:SecurityTokenReference>
          <o:Reference ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3" URI="#BinarySecurityToken"/>
        </o:SecurityTokenReference>
      </KeyInfo>
    </Signature>
  </o:Security>
</s:Header>
<s:Body><Autentica xmlns="{NS_SAT}"/></s:Body>
</s:Envelope>"""

    root = _et.fromstring(TEMPLATE.encode('utf-8'))

    # Namespaces para XPath
    ns = {'s': NS_S, 'o': NS_O, 'u': NS_U, 'ds': NS_DS}

    timestamp_el  = root.find('.//u:Timestamp',  ns)
    signed_info_el= root.find('.//ds:SignedInfo', ns)
    digest_el     = root.find('.//ds:DigestValue',ns)
    sig_val_el    = root.find('.//ds:SignatureValue', ns)

    # 1. Digest del Timestamp (C14N exclusivo del nodo parseado)
    digest_el.text = _digest_c14n(timestamp_el)

    # 2. Firma del SignedInfo (C14N exclusivo)
    sig_val_el.text = _sign_bytes(_c14n(signed_info_el))

    # ── Enviar ───────────────────────────────────────────────────
    xml_bytes = _et.tostring(root, xml_declaration=True, encoding='UTF-8')
    url      = 'https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/Autenticacion/Autenticacion.svc'
    soap_act = 'http://DescargaMasivaTerceros.gob.mx/IAutenticacion/Autentica'
    hdrs = {'Content-type': 'text/xml;charset="utf-8"', 'Accept': 'text/xml',
            'cache-control': 'no-cache', 'SOAPAction': soap_act}
    resp = _req.post(url, data=xml_bytes, headers=hdrs, timeout=15)
    logger.info(f'Auth SAT status={resp.status_code} resp_len={len(resp.text)}')

    # Parsear respuesta
    try:
        resp_root = _et.fromstring(resp.text.encode('utf-8'))
        # Buscar AutenticaResult con cualquier namespace
        for el in resp_root.iter():
            if el.tag.endswith('}AutenticaResult') or el.tag == 'AutenticaResult':
                if el.text:
                    logger.info(f'Token SAT obtenido OK (len={len(el.text)})')
                    return el.text
    except Exception as ep:
        logger.warning(f'Error parseando respuesta auth: {ep}')

    raise Exception(f'No se obtuvo token SAT (status={resp.status_code}). Resp: {resp.text[:400]}')


def _get_key_pem(key_bytes: bytes, password: str) -> str:
    """Convierte .key SAT (PKCS8 encriptado) a PEM."""
    try:
        import chilkat2
        k = chilkat2.PrivateKey()
        ok = k.LoadPkcs8Encrypted(memoryview(key_bytes), password)
        if ok:
            return k.GetPkcs8Pem()
    except Exception:
        pass
    from cryptography.hazmat.primitives.serialization import (
        load_der_private_key, Encoding, PrivateFormat, NoEncryption)
    from cryptography.hazmat.backends import default_backend
    pw = password.encode('utf-8') if isinstance(password, str) and password else None
    pk = load_der_private_key(key_bytes, password=pw, backend=default_backend())
    return pk.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')


def _soap_solicitar(cer_bytes: bytes, key_pem: str, token: str,
                    fi, ff, tipo: str, rfc: str, es_emitidos: bool = False) -> dict:
    """Llama directamente al SOAP del SAT — sin depender de satcfdi."""
    import hashlib, requests as _req
    from lxml import etree as _et
    from OpenSSL import crypto as _cry
    from cryptography.hazmat.primitives import hashes as _h, serialization as _ser
    from cryptography.hazmat.primitives.asymmetric import padding as _pad
    from cryptography.hazmat.backends import default_backend as _db

    def _b64sha1(s):
        return base64.b64encode(hashlib.sha1(s.encode('utf-8')).digest()).decode('ascii')

    def _sign(data, pem):
        data_b = data.encode('utf-8')
        try:
            pk = _cry.load_privatekey(_cry.FILETYPE_PEM, pem)
            return base64.b64encode(_cry.sign(pk, data_b, 'sha1')).decode('ascii')
        except AttributeError:
            pass
        pk = _ser.load_pem_private_key(
            pem.encode('utf-8') if isinstance(pem, str) else pem,
            password=None, backend=_db())
        return base64.b64encode(pk.sign(data_b, _pad.PKCS1v15(), _h.SHA1())).decode('ascii')

    def _cert_info(cer):
        c = _cry.load_certificate(_cry.FILETYPE_ASN1, cer)
        return (base64.b64encode(cer).decode('ascii'),
                str(c.get_serial_number()),
                ','.join(b'='.join(t).decode('utf-8') for t in c.get_issuer().get_components()))

    b64cert, serial, issuer = _cert_info(cer_bytes)
    fi_s = fi.strftime('%Y-%m-%dT%H:%M:%S')
    ff_s = ff.strftime('%Y-%m-%dT%H:%M:%S')
    tipo_str = 'CFDI' if tipo == 'CFDI' else 'METADATA'
    estado   = 'Vigente' if tipo == 'CFDI' and not es_emitidos else 'Todos'

    if es_emitidos:
        method   = 'SolicitaDescargaEmitidos'
        soap_act = 'http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaEmitidos'
        sol_node = ('<des:solicitud EstadoComprobante="{e}" FechaFinal="{ff}" '
                    'FechaInicial="{fi}" RfcEmisor="{rfc}" '
                    'RfcSolicitante="{rfc}" TipoSolicitud="{t}"></des:solicitud>'
                    ).format(e=estado, ff=ff_s, fi=fi_s, rfc=rfc, t=tipo_str)
    else:
        method   = 'SolicitaDescargaRecibidos'
        soap_act = 'http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaRecibidos'
        # v1.5: RfcReceptor ahora va como nodo hijo, no como atributo
        sol_node = ('<des:solicitud EstadoComprobante="{e}" FechaFinal="{ff}" '
                    'FechaInicial="{fi}" '
                    'RfcSolicitante="{rfc}" TipoSolicitud="{t}">'
                    '<des:RfcReceptores><des:RfcReceptor>{rfc}</des:RfcReceptor></des:RfcReceptores>'
                    '</des:solicitud>'
                    ).format(e=estado, ff=ff_s, fi=fi_s, rfc=rfc, t=tipo_str)

    body_digest = ('<des:{m} xmlns:des="http://DescargaMasivaTerceros.sat.gob.mx">'
                   '{n}</des:{m}>').format(m=method, n=sol_node)
    dv = _b64sha1(body_digest)
    data_sign = ('<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">'
                 '<CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>'
                 '<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
                 '<Reference URI=""><Transforms><Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/></Transforms>'
                 '<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>'
                 '<DigestValue>{dv}</DigestValue></Reference></SignedInfo>').format(dv=dv)
    sig = _sign(data_sign, key_pem)
    sig_block = ('<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">'
                 '<SignedInfo><CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>'
                 '<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
                 '<Reference URI=""><Transforms><Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/></Transforms>'
                 '<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>'
                 '<DigestValue>{dv}</DigestValue></Reference></SignedInfo>'
                 '<SignatureValue>{sig}</SignatureValue>'
                 '<KeyInfo><X509Data><X509IssuerSerial>'
                 '<X509IssuerName>{issuer}</X509IssuerName>'
                 '<X509SerialNumber>{serial}</X509SerialNumber>'
                 '</X509IssuerSerial><X509Certificate>{cert}</X509Certificate>'
                 '</X509Data></KeyInfo></Signature>'
                 ).format(dv=dv, sig=sig, issuer=issuer, serial=serial, cert=b64cert)

    sol_signed = sol_node.replace('</des:solicitud>', sig_block + '</des:solicitud>', 1)
    envelope = ('<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
                'xmlns:des="http://DescargaMasivaTerceros.sat.gob.mx" '
                'xmlns:xd="http://www.w3.org/2000/09/xmldsig#">'
                '<s:Header/><s:Body><des:{m}>{sol}</des:{m}></s:Body></s:Envelope>'
                ).format(m=method, sol=sol_signed)

    url = 'https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/SolicitaDescargaService.svc'
    hdrs = {'Content-type':'text/xml;charset="utf-8"','Accept':'text/xml',
            'cache-control':'no-cache','SOAPAction':soap_act,
            'Authorization':f'WRAP access_token="{token}"'}
    resp = _req.post(url, data=envelope.encode('utf-8'), headers=hdrs, timeout=30)

    # Parse — strip namespaces
    from io import StringIO as _SIO
    xslt_src = b"""<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="no"/>
<xsl:template match="/|comment()|processing-instruction()"><xsl:copy><xsl:apply-templates/></xsl:copy></xsl:template>
<xsl:template match="*"><xsl:element name="{local-name()}"><xsl:apply-templates select="@*|node()"/></xsl:element></xsl:template>
<xsl:template match="@*"><xsl:attribute name="{local-name()}"><xsl:value-of select="."/></xsl:attribute></xsl:template>
</xsl:stylesheet>"""
    xdoc = _et.parse(io.BytesIO(xslt_src))
    tree = _et.XSLT(xdoc)(_et.parse(_SIO(resp.text)))

    result = None
    for path in [
        'Body/SolicitaDescargaRecibidosResponse/SolicitaDescargaRecibidosResult',
        'Body/SolicitaDescargaEmitidosResponse/SolicitaDescargaEmitidosResult',
        'Body/SolicitaDescargaResponse/SolicitaDescargaResult',
    ]:
        result = tree.find(path)
        if result is not None: break
    if result is None:
        for el in tree.iter():
            if el.get('CodEstatus') or el.get('IdSolicitud'):
                result = el; break
    if result is not None:
        return dict(result.attrib)
    fault = tree.find('Body/Fault')
    if fault is not None:
        raise Exception(f"SAT Fault: {fault.find('faultcode').text} — {fault.find('faultstring').text}")
    raise Exception(f"Respuesta inesperada SAT: {resp.text[:300]}")


def _soap_descargar(cer_bytes: bytes, key_pem: str, token: str, id_paquete: str) -> str:
    """Descarga un paquete ZIP del SAT via SOAP directo. Devuelve base64 del ZIP."""
    import hashlib, requests as _req
    from lxml import etree as _et
    from io import StringIO as _SIO
    from OpenSSL import crypto as _cry
    from cryptography.hazmat.primitives import hashes as _h, serialization as _ser
    from cryptography.hazmat.primitives.asymmetric import padding as _pad
    from cryptography.hazmat.backends import default_backend as _db

    def _b64sha1(s):
        return base64.b64encode(hashlib.sha1(s.encode('utf-8')).digest()).decode('ascii')
    def _sign(data, pem):
        data_b = data.encode('utf-8')
        try:
            pk = _cry.load_privatekey(_cry.FILETYPE_PEM, pem)
            return base64.b64encode(_cry.sign(pk, data_b, 'sha1')).decode('ascii')
        except AttributeError:
            pass
        pk = _ser.load_pem_private_key(
            pem.encode('utf-8') if isinstance(pem, str) else pem,
            password=None, backend=_db())
        return base64.b64encode(pk.sign(data_b, _pad.PKCS1v15(), _h.SHA1())).decode('ascii')
    def _cert_info(cer):
        c = _cry.load_certificate(_cry.FILETYPE_ASN1, cer)
        return (base64.b64encode(cer).decode('ascii'),
                str(c.get_serial_number()),
                ','.join(b'='.join(t).decode('utf-8') for t in c.get_issuer().get_components()))

    b64cert, serial, issuer = _cert_info(cer_bytes)
    rfc_bytes = _cry.load_certificate(_cry.FILETYPE_ASN1, cer_bytes).get_subject().get_components()
    rfc = dict((k.decode(), v.decode()) for k,v in rfc_bytes).get('x500UniqueIdentifier','')

    data = ('<des:PeticionDescargaMasivaTercerosEntrada '
            'xmlns:des="http://DescargaMasivaTerceros.sat.gob.mx">'
            '<des:peticionDescarga IdPaquete="{p}" RfcSolicitante="{rfc}">'
            '</des:peticionDescarga>'
            '</des:PeticionDescargaMasivaTercerosEntrada>'
            ).format(p=id_paquete, rfc=rfc)
    dv = _b64sha1(data)
    data_sign = ('<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">'
                 '<CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>'
                 '<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
                 '<Reference URI=""><Transforms><Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/></Transforms>'
                 '<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>'
                 '<DigestValue>{dv}</DigestValue></Reference></SignedInfo>').format(dv=dv)
    sig = _sign(data_sign, key_pem)
    envelope = ('<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
                'xmlns:des="http://DescargaMasivaTerceros.sat.gob.mx" '
                'xmlns:xd="http://www.w3.org/2000/09/xmldsig#">'
                '<s:Header/><s:Body>'
                '<des:PeticionDescargaMasivaTercerosEntrada>'
                '<des:peticionDescarga IdPaquete="{p}" RfcSolicitante="{rfc}">'
                '<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">'
                '<SignedInfo><CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>'
                '<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
                '<Reference URI=""><Transforms><Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/></Transforms>'
                '<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>'
                '<DigestValue>{dv}</DigestValue></Reference></SignedInfo>'
                '<SignatureValue>{sig}</SignatureValue>'
                '<KeyInfo><X509Data><X509IssuerSerial>'
                '<X509IssuerName>{issuer}</X509IssuerName>'
                '<X509SerialNumber>{serial}</X509SerialNumber>'
                '</X509IssuerSerial><X509Certificate>{cert}</X509Certificate>'
                '</X509Data></KeyInfo></Signature>'
                '</des:peticionDescarga>'
                '</des:PeticionDescargaMasivaTercerosEntrada>'
                '</s:Body></s:Envelope>'
                ).format(p=id_paquete, rfc=rfc, dv=dv, sig=sig,
                         issuer=issuer, serial=serial, cert=b64cert)

    url = 'https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/DescargaMasivaTercerosService.svc'
    soap_act = 'http://DescargaMasivaTerceros.sat.gob.mx/IDescargaMasivaTercerosService/Descargar'
    hdrs = {'Content-type':'text/xml;charset="utf-8"','Accept':'text/xml',
            'cache-control':'no-cache','SOAPAction':soap_act,
            'Authorization':f'WRAP access_token="{token}"'}
    resp = _req.post(url, data=envelope.encode('utf-8'), headers=hdrs, timeout=120)

    xslt_src = b"""<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="no"/>
<xsl:template match="/|comment()|processing-instruction()"><xsl:copy><xsl:apply-templates/></xsl:copy></xsl:template>
<xsl:template match="*"><xsl:element name="{local-name()}"><xsl:apply-templates select="@*|node()"/></xsl:element></xsl:template>
<xsl:template match="@*"><xsl:attribute name="{local-name()}"><xsl:value-of select="."/></xsl:attribute></xsl:template>
</xsl:stylesheet>"""
    xdoc = _et.parse(io.BytesIO(xslt_src))
    tree = _et.XSLT(xdoc)(_et.parse(_SIO(resp.text)))

    paq_el = tree.find('Body/RespuestaDescargaMasivaTercerosSalida/Paquete')
    if paq_el is not None and paq_el.text:
        return paq_el.text
    fault = tree.find('Body/Fault')
    if fault is not None:
        raise Exception(f"SAT Fault descarga: {fault.find('faultstring').text}")
    raise Exception(f"Sin paquete en respuesta SAT: {resp.text[:300]}")


async def health(request):
    pac_cfg = os.environ.get('PAC_NOMBRE', 'SAT directo (pruebas)')
    return json_resp({'ok': True, 'servicio': 'SAT satcfdi v2.0',
        'puerto': 5050, 'pac': pac_cfg})

async def login(request):
    try:
        d = await request.json()
        loop = asyncio.get_event_loop()
        cer_b64 = d.get('cer',''); key_b64 = d.get('key','')
        if ',' in cer_b64: cer_b64 = cer_b64.split(',')[1]
        if ',' in key_b64: key_b64 = key_b64.split(',')[1]
        cer_bytes = base64.b64decode(cer_b64)
        key_bytes = base64.b64decode(key_b64)
        password  = d.get('password','')
        signer = get_signer(d)

        def _do_login():
            key_pem = _get_key_pem(key_bytes, password)
            token   = _get_sat_token(cer_bytes, key_pem)
            return token

        token = await loop.run_in_executor(None, _do_login)
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

def _solicitar_via_internals(sat, signer, fi, ff, tipo: str, es_emitidos: bool) -> dict:
    """
    Construye la solicitud SAT con lxml + C14N correcto.
    No depende de templates ni de versión específica de satcfdi.
    Usa signature_c14n_sha1 de satcfdi si está disponible, o lo implementa inline.
    """
    import hashlib, requests as _req
    from lxml import etree as _et

    NS_S   = 'http://schemas.xmlsoap.org/soap/envelope/'
    NS_DES = 'http://DescargaMasivaTerceros.sat.gob.mx'
    NS_DS  = 'http://www.w3.org/2000/09/xmldsig#'

    tipo_str  = tipo if tipo in ('CFDI','METADATA') else 'CFDI'
    fi_s      = fi.strftime('%Y-%m-%dT%H:%M:%S')
    ff_s      = ff.strftime('%Y-%m-%dT%H:%M:%S')
    method    = 'SolicitaDescargaEmitidos' if es_emitidos else 'SolicitaDescargaRecibidos'
    soap_act  = (
        'http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaEmitidos'
        if es_emitidos else
        'http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaRecibidos'
    )

    # ── Construir árbol XML con lxml ──────────────────────────────
    NSMAP_ENV = {'s': NS_S, 'des': NS_DES}
    envelope  = _et.Element(f'{{{NS_S}}}Envelope', nsmap=NSMAP_ENV)
    header    = _et.SubElement(envelope, f'{{{NS_S}}}Header')
    body      = _et.SubElement(envelope, f'{{{NS_S}}}Body')
    method_el = _et.SubElement(body, f'{{{NS_DES}}}{method}')
    sol       = _et.SubElement(method_el, f'{{{NS_DES}}}solicitud')

    # Atributos en orden alfabético (requerido por SAT)
    # IMPORTANTE v1.5: EstadoComprobante debe ser 'Vigente' siempre para tipo CFDI
    # Los valores 'Todos' y 'Cancelados' causan error 301 desde la versión 1.5
    if tipo_str == 'CFDI':
        sol.set('EstadoComprobante', 'Vigente')
    sol.set('FechaFinal',     ff_s)
    sol.set('FechaInicial',   fi_s)
    # v1.5: RfcEmisor remains an attribute for EMITIDOS
    # v1.5: RfcReceptor moves to child node <des:RfcReceptores> for RECIBIDOS
    if es_emitidos:
        sol.set('RfcEmisor', signer.rfc)
    sol.set('RfcSolicitante', signer.rfc)
    sol.set('TipoSolicitud',  tipo_str)

    # ── Firmar PRIMERO, luego agregar RfcReceptores ─────────────────
    # IMPORTANTE: La firma debe computarse ANTES de agregar RfcReceptores
    # porque signature_c14n_sha1 computa el digest sobre method_el
    # y el SAT verifica la firma sobre el contenido sin RfcReceptores
    sig_appended = False
    try:
        from satcfdi.create.w3.signature import signature_c14n_sha1
        sig_xml = signature_c14n_sha1(signer=signer, element=method_el).to_xml()
        sol.append(sig_xml)
        sig_appended = True
        logger.info('Firma con signature_c14n_sha1 de satcfdi OK')
    except ImportError:
        pass

    # v1.5: RfcReceptores como nodo hijo - se agrega DESPUÉS de la firma
    if not es_emitidos:
        rfc_receptores = _et.SubElement(sol, f'{{{NS_DES}}}RfcReceptores')
        rfc_receptor   = _et.SubElement(rfc_receptores, f'{{{NS_DES}}}RfcReceptor')
        rfc_receptor.text = signer.rfc
        logger.info(f'v1.5: RfcReceptores agregado como nodo hijo: {signer.rfc}')

    if not sig_appended:
        # Implementación inline: C14N no-exclusivo del method_el (padre de sol)
        from OpenSSL import crypto as _cry
        from cryptography.hazmat.primitives import hashes as _h, serialization as _ser
        from cryptography.hazmat.primitives.asymmetric import padding as _pad
        from cryptography.hazmat.backends import default_backend as _db

        def _sign_bytes(data_bytes, pem):
            try:
                pk = _cry.load_privatekey(_cry.FILETYPE_PEM, pem)
                return base64.b64encode(_cry.sign(pk, data_bytes, 'sha1')).decode('ascii')
            except AttributeError:
                pk = _ser.load_pem_private_key(
                    pem.encode('utf-8') if isinstance(pem, str) else pem,
                    password=None, backend=_db())
                return base64.b64encode(
                    pk.sign(data_bytes, _pad.PKCS1v15(), _h.SHA1())).decode('ascii')

        # Digest del nodo padre (method_el = SolicitaDescargaRecibidos/Emitidos)
        # usando C14N no-exclusivo (exclusive=False) como lo hace satcfdi
        parent_c14n = _et.tostring(method_el, method='c14n', exclusive=False)
        dv = base64.b64encode(hashlib.sha1(parent_c14n).digest()).decode('ascii')

        # Construir SignedInfo y firmarlo con C14N no-exclusivo
        sig_el     = _et.SubElement(sol, f'{{{NS_DS}}}Signature')
        si_el      = _et.SubElement(sig_el, f'{{{NS_DS}}}SignedInfo')
        cm_el      = _et.SubElement(si_el, f'{{{NS_DS}}}CanonicalizationMethod')
        cm_el.set('Algorithm', 'http://www.w3.org/TR/2001/REC-xml-c14n-20010315')
        sm_el      = _et.SubElement(si_el, f'{{{NS_DS}}}SignatureMethod')
        sm_el.set('Algorithm', 'http://www.w3.org/2000/09/xmldsig#rsa-sha1')
        ref_el     = _et.SubElement(si_el, f'{{{NS_DS}}}Reference')
        ref_el.set('URI', '')
        tr_el      = _et.SubElement(ref_el, f'{{{NS_DS}}}Transforms')
        t_el       = _et.SubElement(tr_el,  f'{{{NS_DS}}}Transform')
        t_el.set('Algorithm', 'http://www.w3.org/2000/09/xmldsig#enveloped-signature')
        dm_el      = _et.SubElement(ref_el, f'{{{NS_DS}}}DigestMethod')
        dm_el.set('Algorithm', 'http://www.w3.org/2000/09/xmldsig#sha1')
        dv_el      = _et.SubElement(ref_el, f'{{{NS_DS}}}DigestValue')
        dv_el.text = dv

        # Firma del SignedInfo con C14N no-exclusivo
        si_c14n = _et.tostring(si_el, method='c14n', exclusive=False)

        # Obtener PEM del signer
        pem = signer._sign.__self__.private_bytes(
            _ser.Encoding.PEM, _ser.PrivateFormat.PKCS8, _ser.NoEncryption()
        ).decode('utf-8')
        sig_v_el      = _et.SubElement(sig_el, f'{{{NS_DS}}}SignatureValue')
        sig_v_el.text = _sign_bytes(si_c14n, pem)

        # KeyInfo
        ki_el  = _et.SubElement(sig_el, f'{{{NS_DS}}}KeyInfo')
        x5_el  = _et.SubElement(ki_el,  f'{{{NS_DS}}}X509Data')
        is_el  = _et.SubElement(x5_el,  f'{{{NS_DS}}}X509IssuerSerial')
        in_el  = _et.SubElement(is_el,  f'{{{NS_DS}}}X509IssuerName')
        sn_el  = _et.SubElement(is_el,  f'{{{NS_DS}}}X509SerialNumber')
        ce_el  = _et.SubElement(x5_el,  f'{{{NS_DS}}}X509Certificate')
        c = _cry.load_certificate(_cry.FILETYPE_ASN1, signer.certificate_bytes)
        in_el.text = ','.join(b'='.join(t).decode('utf-8') for t in c.get_issuer().get_components())
        sn_el.text = str(c.get_serial_number())
        ce_el.text = base64.b64encode(signer.certificate_bytes).decode('ascii')
        logger.info('Firma inline C14N OK')

    # v1.5: RfcReceptores DESPUÉS de la firma (inline path)
    if not es_emitidos and not any(
        e.tag.endswith('}RfcReceptores') for e in sol
    ):
        rfc_receptores = _et.SubElement(sol, f'{{{NS_DES}}}RfcReceptores')
        rfc_receptor   = _et.SubElement(rfc_receptores, f'{{{NS_DES}}}RfcReceptor')
        rfc_receptor.text = signer.rfc
        logger.info(f'v1.5 inline: RfcReceptores agregado: {signer.rfc}')

    # ── Enviar ────────────────────────────────────────────────────
    payload = _et.tostring(envelope, xml_declaration=True, encoding='UTF-8')
    token   = sat._get_token_comprobante()
    hdrs    = {
        'Content-type': 'text/xml;charset="utf-8"',
        'Accept':        'text/xml',
        'Cache-Control': 'no-cache',
        'SOAPAction':    soap_act,
        'Authorization': f'WRAP access_token="{token}"',
    }
    url  = 'https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/SolicitaDescargaService.svc'
    resp = _req.post(url, data=payload, headers=hdrs, timeout=30)
    # Log completo para diagnóstico
    logger.info(f'_solicitar_via_internals status={resp.status_code}')
    # Log XML enviado en chunks para ver completo en PM2
    xml_str = payload.decode('utf-8') if isinstance(payload, bytes) else str(payload)
    chunk = 800
    for i in range(0, len(xml_str), chunk):
        logger.info(f'XML_SEND[{i}]: {xml_str[i:i+chunk]}')
    # Log respuesta completa en chunks
    resp_str = resp.text
    for i in range(0, len(resp_str), chunk):
        logger.info(f'SAT_RESP[{i}]: {resp_str[i:i+chunk]}')

    # ── Parsear respuesta ─────────────────────────────────────────
    try:
        resp_root = _et.fromstring(resp.content)
        for path in [
            '{*}Body/{*}SolicitaDescargaRecibidosResponse/{*}SolicitaDescargaRecibidosResult',
            '{*}Body/{*}SolicitaDescargaEmitidosResponse/{*}SolicitaDescargaEmitidosResult',
            '{*}Body/{*}SolicitaDescargaResponse/{*}SolicitaDescargaResult',
        ]:
            el = resp_root.find(path)
            if el is not None:
                return dict(el.attrib)
        for el in resp_root.iter():
            if el.get('CodEstatus') or el.get('IdSolicitud'):
                return dict(el.attrib)
    except Exception as ep:
        logger.warning(f'Error parseando respuesta solicitar: {ep}')
    raise Exception(f'Respuesta inesperada SAT (status={resp.status_code}): {resp.text[:300]}')



async def solicitar(request):
    """Solicita descarga masiva RECIBIDOS. Compatible con cualquier versión de satcfdi."""
    try:
        d    = await request.json()
        loop = asyncio.get_event_loop()
        signer = get_signer(d)
        fi = datetime.strptime(d['fecha_inicio'],'%Y-%m-%d').replace(hour=0,  minute=0,  second=0)
        ff = datetime.strptime(d['fecha_fin'],   '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        tipo = d.get('tipo','CFDI')

        def _solicitar():
            sat = SAT(signer=signer)
            # Intentar método nuevo primero
            if hasattr(sat, 'recover_comprobante_received_request'):
                from satcfdi.pacs.sat import TipoDescargaMasivaTerceros as _T, EstadoComprobante as _EC
                tipo_enum   = _T.CFDI if tipo == 'CFDI' else _T.METADATA
                estado_comp = _EC.VIGENTE if tipo == 'CFDI' else None
                _orig = None
                try:
                    # Parchear METADATA si es necesario
                    if tipo != 'CFDI' and hasattr(_T.METADATA, '_value_'):
                        _orig = _T.METADATA._value_
                        _T.METADATA._value_ = 'METADATA'
                    return sat.recover_comprobante_received_request(
                        fecha_inicial=fi, fecha_final=ff,
                        rfc_receptor=signer.rfc,
                        tipo_solicitud=tipo_enum,
                        estado_comprobante=estado_comp,
                    )
                finally:
                    if _orig is not None:
                        _T.METADATA._value_ = _orig

            # Versión antigua: usar clases internas de satcfdi (C14N correcto garantizado)
            logger.info('Usando clases internas satcfdi para solicitar')
            return _solicitar_via_internals(sat, signer, fi, ff, tipo, es_emitidos=False)

        r = await loop.run_in_executor(None, _solicitar)
        id_sol = r.get('IdSolicitud','')
        cod    = r.get('CodEstatus','')
        msg    = r.get('Mensaje','')
        logger.info(f'Solicitud RECIBIDOS: {id_sol} | {cod} | rfc={signer.rfc} fi={fi} ff={ff} tipo={tipo}')
        # Errores conocidos del SAT con mensajes amigables
        errores_sat = {
            '301': 'XML Mal Formado — Verifica que el RFC sea válido y las fechas estén en formato correcto',
            '302': 'Sello Mal Formado — El certificado .cer o .key puede estar dañado o ser incorrecto',
            '303': 'El sello no corresponde al RFC Solicitante — Usa el CSD correcto para tu RFC',
            '304': 'Certificado Revocado o Caducado — Renueva tu CSD en el SAT',
            '305': 'Certificado Inválido — Verifica que subiste el archivo .cer correcto',
            '5002': 'Límite de solicitudes alcanzado para este período — Cambia las fechas por al menos 1 segundo',
            '5003': 'Demasiados resultados — Reduce el rango de fechas (máximo 1 mes recomendado)',
            '5004': 'No se encontró información — No hay CFDIs en ese período',
        }
        if cod != '5000' and cod in errores_sat:
            return json_resp({'ok': False, 'error': f'SAT {cod}: {errores_sat[cod]}',
                'solicitud': {'IdSolicitud': id_sol, 'CodEstatus': cod, 'Mensaje': msg}})
        return json_resp({'ok': cod=='5000',
            'solicitud': {'IdSolicitud': id_sol, 'CodEstatus': cod, 'Mensaje': msg}})
    except Exception as e:
        import traceback as _tb
        _err_detail = _tb.format_exc()
        logger.error('Error /solicitar completo:\n' + _err_detail)
        return json_resp({'ok': False, 'error': str(e), 'detalle': _err_detail}, 500)

async def solicitar_emitidos(request):
    """Solicita descarga masiva EMITIDOS. Compatible con cualquier versión de satcfdi."""
    try:
        d    = await request.json()
        loop = asyncio.get_event_loop()
        signer = get_signer(d)
        fi = datetime.strptime(d['fecha_inicio'],'%Y-%m-%d').replace(hour=0,  minute=0,  second=0)
        ff = datetime.strptime(d['fecha_fin'],   '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        tipo = d.get('tipo','CFDI')

        def _sol_emi():
            sat = SAT(signer=signer)
            if hasattr(sat, 'recover_comprobante_emitted_request'):
                from satcfdi.pacs.sat import TipoDescargaMasivaTerceros as _T
                tipo_enum = _T.CFDI if tipo == 'CFDI' else _T.METADATA
                _orig = None
                try:
                    if tipo != 'CFDI' and hasattr(_T.METADATA, '_value_'):
                        _orig = _T.METADATA._value_
                        _T.METADATA._value_ = 'METADATA'
                    from satcfdi.pacs.sat import EstadoComprobante as _EC
                    # v1.5: EstadoComprobante=Vigente obligatorio para CFDI
                    estado_comp = _EC.VIGENTE if tipo == 'CFDI' else None
                    return sat.recover_comprobante_emitted_request(
                        fecha_inicial=fi, fecha_final=ff,
                        rfc_emisor=signer.rfc,
                        tipo_solicitud=tipo_enum,
                        estado_comprobante=estado_comp,
                    )
                finally:
                    if _orig is not None:
                        _T.METADATA._value_ = _orig

            logger.info('Usando clases internas satcfdi para solicitar emitidos')
            return _solicitar_via_internals(sat, signer, fi, ff, tipo, es_emitidos=True)

        r = await loop.run_in_executor(None, _sol_emi)
        id_sol = r.get('IdSolicitud','')
        cod    = r.get('CodEstatus','')
        msg    = r.get('Mensaje','')
        logger.info(f'Solicitud EMITIDOS: {id_sol} | {cod} | rfc={signer.rfc}')
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

def parse_cfdi_metadata_xml(xml_str: str) -> list:
    """Parsea un archivo XML de metadata que contiene múltiples CFDIs."""
    registros = []
    try:
        from lxml import etree
        raw = xml_str.encode('utf-8') if isinstance(xml_str, str) else xml_str
        if raw.startswith(b'\xef\xbb\xbf'): raw = raw[3:]
        root = etree.fromstring(raw)
        # El SAT usa varios namespaces posibles
        ns_map = {
            'cfdi': 'http://www.sat.gob.mx/cfd/4',
            'cfdi3': 'http://www.sat.gob.mx/cfd/3',
        }
        # Buscar todos los nodos Comprobante — evitar FutureWarning lxml con `or`
        def _find_comp(r):
            for expr, kw in [
                ('.//cfdi:Comprobante',  ns_map),
                ('.//cfdi3:Comprobante', ns_map),
                ('.//{http://www.sat.gob.mx/cfd/4}Comprobante', None),
                ('.//{http://www.sat.gob.mx/cfd/3}Comprobante', None),
            ]:
                try:
                    res = r.findall(expr, kw) if kw else r.findall(expr)
                    if len(res) > 0: return res
                except Exception: pass
            if r.get('Version') or r.get('version'): return [r]
            return []
        comprobantes = _find_comp(root)
        for comp in comprobantes:
            # Timbre fiscal
            tfd_ns = 'http://www.sat.gob.mx/TimbreFiscalDigital'
            tf = comp.find(f'.//{{{tfd_ns}}}TimbreFiscalDigital') or comp.find('.//TimbreFiscalDigital')
            uuid = tf.get('UUID','') if tf is not None else ''
            if not uuid:
                continue
            # Emisor y receptor
            em  = comp.find('.//{http://www.sat.gob.mx/cfd/4}Emisor') or comp.find('.//{http://www.sat.gob.mx/cfd/3}Emisor') or comp.find('.//Emisor')
            rec = comp.find('.//{http://www.sat.gob.mx/cfd/4}Receptor') or comp.find('.//{http://www.sat.gob.mx/cfd/3}Receptor') or comp.find('.//Receptor')
            efecto = comp.get('TipoDeComprobante','')
            est_raw = comp.get('Estatus','1')
            estatus = {'1':'Vigente','2':'Cancelado'}.get(est_raw, 'Vigente')
            registros.append({
                'uuid':            uuid,
                'rfc_emisor':      em.get('Rfc','')  if em  is not None else '',
                'nombre_emisor':   em.get('Nombre','') if em  is not None else '',
                'rfc_receptor':    rec.get('Rfc','') if rec is not None else '',
                'nombre_receptor': rec.get('Nombre','') if rec is not None else '',
                'rfc_pac':         tf.get('RfcProvCertif','') if tf is not None else '',
                'fecha_emision':   comp.get('Fecha',''),
                'fecha_certificacion': tf.get('FechaTimbrado','') if tf is not None else '',
                'monto':           comp.get('Total','0'),
                'subtotal':        comp.get('SubTotal','0'),
                'total':           comp.get('Total','0'),
                'moneda':          comp.get('Moneda','MXN'),
                'forma_pago':      comp.get('FormaPago',''),
                'metodo_pago':     comp.get('MetodoPago',''),
                'uso_cfdi':        rec.get('UsoCFDI','') if rec is not None else '',
                'lugar_expedicion':comp.get('LugarExpedicion',''),
                'serie':           comp.get('Serie',''),
                'folio':           comp.get('Folio',''),
                'version':         comp.get('Version',''),
                'efecto':          efecto,
                'tipo':            efecto,
                'estatus':         estatus,
                'fecha_cancelacion': '',
                'xml_content':     xml_str if isinstance(xml_str,str) else xml_str.decode('utf-8','replace'),
                'raw':             {'uuid': uuid},
            })
        logger.info(f'XML metadata parseado: {len(registros)} CFDIs')
    except Exception as e:
        logger.exception(f'Error parseando XML metadata: {e}')
    return registros


def parse_metadata_zip(paq_bytes: bytes) -> list:
    """
    Parsea un ZIP de METADATA del SAT.
    Soporta:
    - TXT delimitado por | o ~ (formato clásico)
    - XML con múltiples CFDIs (formato nuevo SAT v1.5)
    - ZIP con XMLs individuales por CFDI
    """
    registros = []
    try:
        with zipfile.ZipFile(io.BytesIO(paq_bytes), 'r') as zf:
            for name in zf.namelist():
                raw_bytes = zf.read(name)
                # Decodificar
                for enc in ('utf-8-sig', 'utf-8', 'windows-1252', 'latin-1'):
                    try:
                        raw = raw_bytes.decode(enc)
                        break
                    except Exception:
                        continue
                else:
                    raw = raw_bytes.decode('utf-8', 'replace')

                raw_strip = raw.lstrip('\ufeff').lstrip()
                logger.info(f'Metadata archivo={name} lineas={len(raw.splitlines())} enc={enc}')

                # ── Detectar si es XML ────────────────────────────
                if raw_strip.startswith('<?xml') or raw_strip.startswith('<cfdi:') or '<Comprobante' in raw_strip:
                    logger.info(f'Archivo XML detectado: {name}')
                    nuevos = parse_cfdi_metadata_xml(raw_strip)
                    registros.extend(nuevos)
                    continue

                # ── Formato TXT delimitado ────────────────────────
                lineas = raw.splitlines()
                if len(lineas) < 2:
                    logger.warning(f'Archivo vacío o sin datos: {name}')
                    continue

                primera = lineas[0].lstrip('\ufeff')
                sep = '~' if '~' in primera else '|'
                headers = [h.strip() for h in primera.split(sep)]
                logger.info(f'Headers ({sep}): {headers[:5]}...')

                # Verificar que los headers sean válidos (no XML)
                if not any(h in headers for h in ['Uuid','UUID','uuid','RfcEmisor','Monto']):
                    logger.warning(f'Headers no reconocidos en {name}: {headers[:3]}')
                    continue

                for linea in lineas[1:]:
                    if not linea.strip():
                        continue
                    valores = [v.strip() for v in linea.split(sep)]
                    while len(valores) < len(headers):
                        valores.append('')
                    reg = dict(zip(headers, valores))

                    efecto  = reg.get('EfectoComprobante', '')
                    est_raw = reg.get('Estatus', '')
                    estatus = {'1':'Vigente','2':'Cancelado'}.get(est_raw, est_raw)

                    uuid = reg.get('Uuid', reg.get('UUID', reg.get('uuid','')))
                    if not uuid:
                        continue

                    registros.append({
                        'uuid':               uuid,
                        'rfc_emisor':         reg.get('RfcEmisor',          ''),
                        'nombre_emisor':      reg.get('NombreEmisor',       ''),
                        'rfc_receptor':       reg.get('RfcReceptor',        ''),
                        'nombre_receptor':    reg.get('NombreReceptor',     ''),
                        'rfc_pac':            reg.get('PacCertifico',       reg.get('RfcPac', '')),
                        'fecha_emision':      reg.get('FechaEmision',       ''),
                        'fecha_certificacion':reg.get('FechaCertificacionSat', ''),
                        'monto':              reg.get('Monto',              '0'),
                        'subtotal':           reg.get('SubTotal',           reg.get('Monto','0')),
                        'total':              reg.get('Total',              reg.get('Monto','0')),
                        'moneda':             reg.get('Moneda',             'MXN'),
                        'forma_pago':         reg.get('FormaPago',          ''),
                        'metodo_pago':        reg.get('MetodoPago',         ''),
                        'uso_cfdi':           reg.get('UsoCFDI',            ''),
                        'lugar_expedicion':   reg.get('LugarExpedicion',    ''),
                        'serie':              reg.get('Serie',              ''),
                        'folio':              reg.get('Folio',              ''),
                        'version':            reg.get('Version',            ''),
                        'efecto':             efecto,
                        'tipo':               efecto,
                        'estatus':            estatus,
                        'fecha_cancelacion':  reg.get('FechaCancelacion',   ''),
                        'raw':                reg,
                    })

        logger.info(f'Total metadata parseada: {len(registros)} registros')
    except Exception as e:
        logger.exception('Error parseando metadata ZIP')
    return registros


async def descargar(request):
    """Descarga un paquete ZIP del SAT. Usa SOAP directo como fallback."""
    try:
        d      = await request.json()
        loop   = asyncio.get_event_loop()
        cer_b64 = d.get('cer',''); key_b64 = d.get('key','')
        if ',' in cer_b64: cer_b64 = cer_b64.split(',')[1]
        if ',' in key_b64: key_b64 = key_b64.split(',')[1]
        cer_bytes = base64.b64decode(cer_b64)
        key_bytes = base64.b64decode(key_b64)
        password  = d.get('password','')
        signer = get_signer(d)
        sat    = SAT(signer=signer)
        id_paq = d['id_paquete']
        tipo   = d.get('tipo', 'CFDI')

        def _descargar():
            # Intentar primero con satcfdi
            try:
                r_info, paq_b64 = sat.recover_comprobante_download(id_paquete=id_paq)
                if paq_b64:
                    return paq_b64
            except Exception as e_sat:
                logger.warning(f'satcfdi descarga falló ({e_sat}), usando SOAP directo')
            # Fallback SOAP directo
            key_pem = _get_key_pem(key_bytes, password)
            token   = _get_sat_token(cer_bytes, key_pem)
            return _soap_descargar(cer_bytes, key_pem, token, id_paq)

        paq_b64 = await loop.run_in_executor(None, _descargar)

        if not paq_b64:
            return json_resp({'ok': False, 'error': 'No se pudo descargar el paquete.'}, 400)

        paq_bytes = base64.b64decode(paq_b64)

        # ── Parsear ZIP — detectar contenido real ─────────────────
        import re as _re
        cfdis_xml, metadatos_txt = [], []

        with zipfile.ZipFile(io.BytesIO(paq_bytes), 'r') as zf:
            nombres = zf.namelist()
            logger.info(f'ZIP {id_paq}: {len(nombres)} archivos: {nombres[:5]}')

            for name in nombres:
                raw_bytes = zf.read(name)
                raw = ''
                for enc in ('utf-8-sig', 'utf-8', 'windows-1252', 'latin-1'):
                    try: raw = raw_bytes.decode(enc); break
                    except Exception: continue
                if not raw:
                    raw = raw_bytes.decode('utf-8', 'replace')
                raw_strip = raw.lstrip('\ufeff').lstrip()

                if not name.lower().endswith('.xml'):
                    continue

                # Es un XML — ¿CFDI completo o metadata?
                es_cfdi_completo = (
                    'TimbreFiscalDigital' in raw_strip or
                    'tfd:' in raw_strip or
                    'cfdi:Comprobante' in raw_strip or
                    ('<Comprobante ' in raw_strip and 'Version=' in raw_strip)
                )

                if es_cfdi_completo:
                    parsed = parse_cfdi_xml(raw_strip, name)
                    # UUID fallback: tomar del nombre del archivo ({UUID}.xml)
                    if not parsed.get('uuid'):
                        m = _re.match(
                            r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})',
                            name)
                        if m:
                            parsed['uuid'] = m.group(1)
                    if parsed.get('uuid'):
                        cfdis_xml.append(parsed)
                        logger.info(f'CFDI: {name} uuid={parsed["uuid"]}')
                    else:
                        logger.warning(f'CFDI sin UUID: {name}')
                else:
                    nuevos = parse_cfdi_metadata_xml(raw_strip)
                    metadatos_txt.extend(nuevos)
                    logger.info(f'Meta XML: {name} → {len(nuevos)} registros')

        logger.info(f'ZIP {id_paq}: {len(cfdis_xml)} CFDIs, {len(metadatos_txt)} metadatos')
        return json_resp({
            'ok': True, 'paquete': id_paq, 'tipo': tipo,
            'total': len(cfdis_xml) + len(metadatos_txt),
            'cfdis': cfdis_xml, 'metadatos': metadatos_txt,
        })
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
