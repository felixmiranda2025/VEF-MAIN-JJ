"""
SAT Descarga Masiva — Microservicio Flask
Puerto: 5050
"""
import sys, os, base64, zipfile, traceback
from io import BytesIO
from datetime import datetime
from flask import Flask, request, jsonify

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sat_service import Login, Request as SatRequest, Verify, Download, Utils

app = Flask(__name__)

def get_fiel(data):
    cer_b64 = data.get('cer','')
    key_b64 = data.get('key','')
    password= data.get('password','')
    if ',' in cer_b64: cer_b64 = cer_b64.split(',')[1]
    if ',' in key_b64: key_b64 = key_b64.split(',')[1]
    cer_bytes  = base64.b64decode(cer_b64)
    key_buffer = BytesIO(base64.b64decode(key_b64))
    key_pem    = Utils.pkey_buffer_to_pem(key_buffer, password)
    return cer_bytes, key_pem

def parse_cfdi(xml_str, filename=''):
    try:
        from lxml import etree
        NS4='http://www.sat.gob.mx/cfd/4'; NS3='http://www.sat.gob.mx/cfd/3'
        TFD='http://www.sat.gob.mx/TimbreFiscalDigital'
        root=etree.fromstring(xml_str.encode('utf-8') if isinstance(xml_str,str) else xml_str)
        def g(el,*a):
            if el is None: return ''
            for x in a:
                v=el.get(x)
                if v: return v
            return ''
        em = root.find('.//{%s}Emisor'%NS4) or root.find('.//{%s}Emisor'%NS3) or root.find('.//Emisor')
        re = root.find('.//{%s}Receptor'%NS4) or root.find('.//{%s}Receptor'%NS3) or root.find('.//Receptor')
        tf = root.find('.//{%s}TimbreFiscalDigital'%TFD) or root.find('.//TimbreFiscalDigital')
        imp= root.find('.//{%s}Impuestos'%NS4) or root.find('.//{%s}Impuestos'%NS3) or root.find('.//Impuestos')
        iva='0'; isr='0'
        if imp is not None:
            tr=imp.find('.//{%s}Traslado'%NS4) or imp.find('.//Traslado')
            rt=imp.find('.//{%s}Retencion'%NS4) or imp.find('.//Retencion')
            if tr: iva=g(tr,'Importe','importe') or '0'
            if rt: isr=g(rt,'Importe','importe') or '0'
        return {
            'archivo':g(root,''),'uuid':g(tf,'UUID','Uuid') if tf is not None else '',
            'fecha':g(root,'Fecha','fecha'),'serie':g(root,'Serie','serie'),
            'folio':g(root,'Folio','folio'),'tipo':g(root,'TipoDeComprobante','tipoDeComprobante'),
            'subtotal':g(root,'SubTotal','subTotal'),'iva':iva,'isr_ret':isr,
            'total':g(root,'Total','total'),'moneda':g(root,'Moneda','moneda') or 'MXN',
            'tipo_cambio':g(root,'TipoCambio') or '1',
            'emisor_rfc':g(em,'Rfc','rfc') if em is not None else '',
            'emisor_nombre':g(em,'Nombre','nombre') if em is not None else '',
            'receptor_rfc':g(re,'Rfc','rfc') if re is not None else '',
            'receptor_nombre':g(re,'Nombre','nombre') if re is not None else '',
            'uso_cfdi':g(re,'UsoCFDI','usoCFDI') if re is not None else '',
            'xml':xml_str if isinstance(xml_str,str) else xml_str.decode('utf-8','replace'),
            'archivo':filename,
        }
    except Exception as e:
        return {'uuid':'','archivo':filename,'error':str(e),'xml':xml_str if isinstance(xml_str,str) else ''}

@app.route('/health')
def health(): return jsonify({'ok':True,'servicio':'SAT Descarga Masiva'})

@app.route('/login', methods=['POST'])
def login():
    try:
        d=request.get_json(force=True) or {}
        cer,kpem=get_fiel(d)
        tok=Login.TokenRequest().soapRequest(certificate=cer,keyPEM=kpem)
        if tok:
            rfc=Utils.rfc_from_certificate(cer)
            return jsonify({'ok':True,'token':tok,'rfc':rfc})
        return jsonify({'ok':False,'error':'No se obtuvo token. Verifica FIEL y contraseña.'}),400
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'ok':False,'error':str(e)}),500

@app.route('/solicitar', methods=['POST'])
def solicitar():
    try:
        d=request.get_json(force=True) or {}
        cer,kpem=get_fiel(d)
        dt_i=datetime.strptime(d['fecha_inicio'],'%Y-%m-%d').replace(hour=0,minute=0,second=0)
        dt_f=datetime.strptime(d['fecha_fin'],   '%Y-%m-%d').replace(hour=23,minute=59,second=59)
        r=SatRequest.RequestDownloadRequest().soapRequest(
            certificate=cer,keyPEM=kpem,token=d['token'],
            start_date=dt_i,end_date=dt_f,tipo_solicitud=d.get('tipo','CFDI'))
        return jsonify({'ok':True,'solicitud':dict(r)})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'ok':False,'error':str(e)}),500

@app.route('/verificar', methods=['POST'])
def verificar():
    try:
        d=request.get_json(force=True) or {}
        cer,kpem=get_fiel(d)
        r=Verify.VerifyRequest().soapRequest(
            certificate=cer,keyPEM=kpem,token=d['token'],id_solicitud=d['id_solicitud'])
        return jsonify({'ok':True,'listo':r.ready,'paquetes':r.paquetes or [],'error_info':r.error})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'ok':False,'error':str(e)}),500

@app.route('/descargar', methods=['POST'])
def descargar():
    try:
        d=request.get_json(force=True) or {}
        cer,kpem=get_fiel(d)
        zip_path=Download.DownloadRequest().soapRequest(
            certificate=cer,keyPEM=kpem,token=d['token'],
            id_paquete=d['id_paquete'],path='/tmp/sat_pkg_')
        if not zip_path:
            return jsonify({'ok':False,'error':'No se pudo descargar. Token expirado o paquete inválido.'}),400
        cfdis=[]
        try:
            with zipfile.ZipFile(zip_path,'r') as zf:
                for name in zf.namelist():
                    if name.lower().endswith('.xml'):
                        raw=zf.read(name)
                        cfdis.append(parse_cfdi(raw.decode('utf-8','replace'),name))
        finally:
            try: os.remove(zip_path)
            except: pass
        return jsonify({'ok':True,'paquete':d['id_paquete'],'total':len(cfdis),'cfdis':cfdis})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'ok':False,'error':str(e)}),500

if __name__=='__main__':
    print('🏛  SAT API en puerto 5050')
    app.run(host='0.0.0.0',port=5050,debug=False)
