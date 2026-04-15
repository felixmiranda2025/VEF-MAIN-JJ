"""
Script de diagnóstico para ver el contenido real del ZIP de metadatos del SAT.
Uso: python DIAGNOSTICO_METADATA.py

Pega tus credenciales FIEL en las variables de abajo.
"""
import base64, zipfile, io, sys, os

# ── CONFIGURACIÓN ────────────────────────────────────────────────
# Pega aquí la ruta a tus archivos FIEL
CER_PATH = r"C:\Users\Siemens\Documents\fiel\00001000000513633540.cer"  # Cambia esta ruta
KEY_PATH = r"C:\Users\Siemens\Documents\fiel\Claveprivada_FIEL_MIGF7707129X3_20220623_144105.key"  # Cambia esta ruta
PASSWORD  = "Brabata1207"         # Cambia la contraseña

# ID de solicitud/paquete ya existente (cópialo del ERP)
# O deja vacío para hacer una solicitud nueva
ID_PAQUETE = "f088f95a-8004-4db5-98e6-d77a74f024e7_01"  # ← Pega aquí tu ID de paquete, o deja "" para solicitar uno nuevo
# ────────────────────────────────────────────────────────────────

try:
    from satcfdi.models import Signer
    from satcfdi.pacs.sat import SAT, TipoDescargaMasivaTerceros, EstadoComprobante
    from datetime import datetime
except ImportError:
    print("ERROR: satcfdi no está instalado. Ejecuta: pip install satcfdi")
    sys.exit(1)

print("=" * 60)
print("DIAGNÓSTICO METADATA SAT")
print("=" * 60)

# Cargar FIEL
try:
    cer_bytes = open(CER_PATH, 'rb').read()
    key_bytes = open(KEY_PATH, 'rb').read()
    signer = Signer.load(certificate=cer_bytes, key=key_bytes, password=PASSWORD)
    print(f"✅ FIEL cargada: {signer.rfc}")
except Exception as e:
    print(f"❌ Error cargando FIEL: {e}")
    sys.exit(1)

sat = SAT(signer=signer)

# Si no hay paquete, hacer una solicitud nueva
if not ID_PAQUETE:
    print("\nSolicitando metadata de los últimos 7 días...")
    from datetime import timedelta
    fi = datetime.now().replace(hour=0, minute=0, second=0) - timedelta(days=7)
    ff = datetime.now().replace(hour=23, minute=59, second=59)
    
    # Parchear enum
    from satcfdi.pacs.sat import TipoDescargaMasivaTerceros as _T
    _T.METADATA._value_ = 'METADATA'
    
    r = sat.recover_comprobante_received_request(
        fecha_inicial=fi, fecha_final=ff,
        rfc_receptor=signer.rfc,
        tipo_solicitud=_T.METADATA,
        estado_comprobante=None,
    )
    print(f"Solicitud: {r}")
    
    if r.get('CodEstatus') == '5000':
        id_sol = r['IdSolicitud']
        print(f"\n✅ Solicitud aceptada: {id_sol}")
        print("Espera unas horas y ejecuta de nuevo con ID_PAQUETE configurado.")
    sys.exit(0)

# Descargar paquete
print(f"\nDescargando paquete: {ID_PAQUETE}")
try:
    r, paq_b64 = sat.recover_comprobante_download(id_paquete=ID_PAQUETE)
    print(f"Respuesta descarga: {r}")
    
    if not paq_b64:
        print("❌ No se obtuvo base64 del paquete")
        sys.exit(1)
    
    paq_bytes = base64.b64decode(paq_b64)
    print(f"✅ ZIP descargado: {len(paq_bytes)} bytes")
    
    # Inspeccionar contenido
    with zipfile.ZipFile(io.BytesIO(paq_bytes), 'r') as zf:
        archivos = zf.namelist()
        print(f"\nArchivos en el ZIP: {archivos}")
        
        for nombre in archivos:
            raw = zf.read(nombre)
            print(f"\n{'='*50}")
            print(f"Archivo: {nombre} ({len(raw)} bytes)")
            
            # Intentar diferentes encodings
            for enc in ('windows-1252', 'latin-1', 'utf-8-sig', 'utf-8'):
                try:
                    texto = raw.decode(enc)
                    print(f"Encoding detectado: {enc}")
                    lineas = texto.splitlines()
                    print(f"Total líneas: {len(lineas)}")
                    
                    if lineas:
                        print(f"\n>>> PRIMERA LÍNEA (headers):")
                        print(repr(lineas[0]))
                        print(f"\n>>> Headers separados por |:")
                        headers = [h.strip() for h in lineas[0].split('|')]
                        for i, h in enumerate(headers):
                            print(f"  [{i}] '{h}'")
                        
                        if len(lineas) > 1:
                            print(f"\n>>> SEGUNDA LÍNEA (primer registro):")
                            print(repr(lineas[1]))
                            valores = [v.strip() for v in lineas[1].split('|')]
                            print(f"\n>>> Valores del primer registro:")
                            for h, v in zip(headers, valores):
                                print(f"  {h}: '{v}'")
                    break
                except Exception as e:
                    print(f"  {enc}: fallo ({e})")

except Exception as e:
    import traceback
    print(f"❌ Error: {e}")
    traceback.print_exc()
pause = input("\nPresiona Enter para salir...")