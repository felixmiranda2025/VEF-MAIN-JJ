import requests
from datetime import datetime
from . import Utils


class RequestDownloadRequest:
    """
    Servicio de Solicitud de Descarga Masiva SAT v1.5 (vigente desde 2025-05-30).
    Soporta SolicitaDescargaRecibidos y SolicitaDescargaEmitidos.
    """
    XML_FACTURAS = 'CFDI'
    METADATOS    = 'METADATA'

    url = 'https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/SolicitaDescargaService.svc'

    def soapRequest(self, certificate: bytes, keyPEM: str, token: str,
                    start_date: datetime, end_date: datetime,
                    tipo_solicitud=XML_FACTURAS,
                    rfc_emisor: str = '',
                    rfc_receptor: str = ''):
        """
        Por defecto descarga CFDIs RECIBIDOS.
        Para emitidos: pasar rfc_emisor=tu_rfc y rfc_receptor=''.
        """
        rfc_solicitante = Utils.rfc_from_certificate(certificate)

        # Determinar si es recibidos o emitidos
        es_recibidos = bool(rfc_receptor) or (not rfc_emisor and not rfc_receptor)
        if es_recibidos and not rfc_receptor:
            rfc_receptor = rfc_solicitante
        if not es_recibidos and not rfc_emisor:
            rfc_emisor = rfc_solicitante

        if es_recibidos:
            soap_action = 'http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaRecibidos'
            xml = self._body_recibidos(certificate, keyPEM, rfc_solicitante, rfc_receptor,
                                       start_date, end_date, tipo_solicitud)
        else:
            soap_action = 'http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaEmitidos'
            xml = self._body_emitidos(certificate, keyPEM, rfc_solicitante, rfc_emisor,
                                      start_date, end_date, tipo_solicitud)

        headers = Utils.headers(xml=xml, soapAction=soap_action, token=token)
        resp = requests.post(url=self.url, data=xml, headers=headers, timeout=30)
        tree = Utils.xml_etree(resp.text)

        # v1.5: parsear respuesta — buscar en múltiples paths posibles
        result = None
        fault  = tree.find('Body/Fault')
        # Buscar el nodo resultado en todas las variantes posibles
        for path in [
            'Body/SolicitaDescargaRecibidosResponse/SolicitaDescargaRecibidosResult',
            'Body/SolicitaDescargaEmitidosResponse/SolicitaDescargaEmitidosResult',
            'Body/SolicitaDescargaResponse/SolicitaDescargaResult',
        ]:
            result = tree.find(path)
            if result is not None:
                break
        # Si no encontró por path, buscar cualquier nodo con CodEstatus (más robusto)
        if result is None:
            for el in tree.iter():
                if el.get('CodEstatus') or el.get('IdSolicitud'):
                    result = el
                    break

        if result is not None:
            return result.attrib
        elif fault is not None:
            code    = fault.find('faultcode').text
            message = fault.find('faultstring').text
            raise Exception('El servidor repondio con el error {}: {}'.format(code, message))
        else:
            raise Exception('El servidor no respondio. Respuesta:\n' + resp.text[:500])

    # ── Recibidos ────────────────────────────────────────────────────
    def _body_recibidos(self, certificate, keyPEM, rfc_solicitante, rfc_receptor,
                        start_date, end_date, tipo_solicitud):
        fi = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        ff = end_date.strftime('%Y-%m-%dT%H:%M:%S')

        # Atributos en ORDEN ALFABÉTICO (requerido por SAT v1.5)
        # EstadoComprobante=Vigente obligatorio para XML (no para METADATA)
        estado = 'Vigente' if tipo_solicitud == 'CFDI' else 'Todos'

        solicitud_node = (
            '<des:solicitud EstadoComprobante="{estado}" FechaFinal="{ff}" '
            'FechaInicial="{fi}" RfcReceptor="{receptor}" '
            'RfcSolicitante="{sol}" TipoSolicitud="{tipo}">'
            '</des:solicitud>'
        ).format(estado=estado, ff=ff, fi=fi,
                 receptor=rfc_receptor, sol=rfc_solicitante, tipo=tipo_solicitud)

        body_for_digest = (
            '<des:SolicitaDescargaRecibidos xmlns:des="http://DescargaMasivaTerceros.sat.gob.mx">'
            '{node}'
            '</des:SolicitaDescargaRecibidos>'
        ).format(node=solicitud_node)

        return self._build_envelope(
            certificate, keyPEM, body_for_digest, solicitud_node,
            'SolicitaDescargaRecibidos', rfc_solicitante
        )

    # ── Emitidos ─────────────────────────────────────────────────────
    def _body_emitidos(self, certificate, keyPEM, rfc_solicitante, rfc_emisor,
                       start_date, end_date, tipo_solicitud):
        fi = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        ff = end_date.strftime('%Y-%m-%dT%H:%M:%S')

        solicitud_node = (
            '<des:solicitud EstadoComprobante="Todos" FechaFinal="{ff}" '
            'FechaInicial="{fi}" RfcEmisor="{emisor}" '
            'RfcSolicitante="{sol}" TipoSolicitud="{tipo}">'
            '</des:solicitud>'
        ).format(ff=ff, fi=fi, emisor=rfc_emisor, sol=rfc_solicitante, tipo=tipo_solicitud)

        body_for_digest = (
            '<des:SolicitaDescargaEmitidos xmlns:des="http://DescargaMasivaTerceros.sat.gob.mx">'
            '{node}'
            '</des:SolicitaDescargaEmitidos>'
        ).format(node=solicitud_node)

        return self._build_envelope(
            certificate, keyPEM, body_for_digest, solicitud_node,
            'SolicitaDescargaEmitidos', rfc_solicitante
        )

    # ── Constructor de envelope SOAP con firma ────────────────────────
    def _build_envelope(self, certificate, keyPEM, body_for_digest,
                        solicitud_node, method_name, rfc_solicitante):
        digest_value  = Utils.b64_sha1_digest(body_for_digest)

        dataToSign = (
            '<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">'
            '<CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>'
            '<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
            '<Reference URI="">'
            '<Transforms>'
            '<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>'
            '</Transforms>'
            '<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>'
            '<DigestValue>{dv}</DigestValue>'
            '</Reference>'
            '</SignedInfo>'
        ).format(dv=digest_value)

        signature = Utils.b64_signature_pkey(dataToSign, keyPEM)
        b64cert   = Utils.b64_certificate(certificate)
        serial    = Utils.certificate_serial_number(certificate)
        issuer    = Utils.issuer_data_string(certificate)

        signature_block = (
            '<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">'
            '<SignedInfo>'
            '<CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>'
            '<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
            '<Reference URI="">'
            '<Transforms>'
            '<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>'
            '</Transforms>'
            '<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>'
            '<DigestValue>{dv}</DigestValue>'
            '</Reference>'
            '</SignedInfo>'
            '<SignatureValue>{sig}</SignatureValue>'
            '<KeyInfo><X509Data><X509IssuerSerial>'
            '<X509IssuerName>{issuer}</X509IssuerName>'
            '<X509SerialNumber>{serial}</X509SerialNumber>'
            '</X509IssuerSerial>'
            '<X509Certificate>{cert}</X509Certificate>'
            '</X509Data></KeyInfo>'
            '</Signature>'
        ).format(dv=digest_value, sig=signature, issuer=issuer, serial=serial, cert=b64cert)

        # Insertar la firma DENTRO del nodo solicitud
        solicitud_with_sig = solicitud_node.replace(
            '</des:solicitud>', signature_block + '</des:solicitud>', 1
        )

        xml = (
            '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:des="http://DescargaMasivaTerceros.sat.gob.mx" '
            'xmlns:xd="http://www.w3.org/2000/09/xmldsig#">'
            '<s:Header/><s:Body>'
            '<des:{method}>'
            '{sol_sig}'
            '</des:{method}>'
            '</s:Body></s:Envelope>'
        ).format(method=method_name, sol_sig=solicitud_with_sig)

        return xml.encode('utf-8')
