#!/usr/bin/env python3
"""
Script diagnóstico — corre en el servidor para ver el XML exacto que se envía al SAT
Uso: python3 test_sat_xml.py
"""
import sys, os, base64, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Simular el XML que construye sat_api.py sin enviarlo
from lxml import etree as _et
from datetime import datetime
import hashlib, base64

NS_S   = 'http://schemas.xmlsoap.org/soap/envelope/'
NS_DES = 'http://DescargaMasivaTerceros.sat.gob.mx'
NS_DS  = 'http://www.w3.org/2000/09/xmldsig#'

RFC = "GOBE840604JLA"
fi_s = "2026-01-01T00:00:00"
ff_s = "2026-03-31T23:59:59"
tipo_str = "CFDI"
es_emitidos = False

method = 'SolicitaDescargaRecibidos'

NSMAP_ENV = {'s': NS_S, 'des': NS_DES}
envelope  = _et.Element(f'{{{NS_S}}}Envelope', nsmap=NSMAP_ENV)
header    = _et.SubElement(envelope, f'{{{NS_S}}}Header')
body      = _et.SubElement(envelope, f'{{{NS_S}}}Body')
method_el = _et.SubElement(body, f'{{{NS_DES}}}{method}')
sol       = _et.SubElement(method_el, f'{{{NS_DES}}}solicitud')

sol.set('EstadoComprobante', 'Vigente')
sol.set('FechaFinal',  ff_s)
sol.set('FechaInicial', fi_s)
sol.set('RfcSolicitante', RFC)
sol.set('TipoSolicitud',  tipo_str)

# Signature placeholder
sig_el = _et.SubElement(sol, f'{{{NS_DS}}}Signature')
sig_el.text = "FIRMA_AQUI"

# v1.5 RfcReceptores AFTER signature
rfc_receptores = _et.SubElement(sol, f'{{{NS_DES}}}RfcReceptores')
rfc_receptor   = _et.SubElement(rfc_receptores, f'{{{NS_DES}}}RfcReceptor')
rfc_receptor.text = RFC

xml_out = _et.tostring(envelope, xml_declaration=True, encoding='UTF-8', pretty_print=True).decode('utf-8')
print("=" * 80)
print("XML STRUCTURE (con firma placeholder):")
print("=" * 80)
print(xml_out)
print("=" * 80)
print(f"Longitud XML: {len(xml_out)} chars")
