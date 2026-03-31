import re

path = r'D:\VEF\VEF15\vef-erp\frontend\app.html'

with open(path, 'r', encoding='utf-8', errors='replace') as f:
    c = f.read()

scripts = re.findall(r'<script[^>]*>(.*?)</script>', c, re.DOTALL)
js = scripts[0] if scripts else ''

opens  = len(re.findall(r'<script[^>]*>', c))
closes = len(re.findall(r'</script>', c))
cdn    = c.count('cdn-cgi')

print(f"Script tags:  {opens} open / {closes} close  {'OK' if opens==closes else 'ERROR - DESBALANCEADO'}")
print(f"cdn-cgi refs: {cdn}")
print(f"JS chars:     {len(js)}")
print(f"nav() defined: {'SI' if 'function nav(' in js else 'NO - ACA ESTA EL PROBLEMA'}")
print(f"Ends </html>: {'SI' if c.strip().endswith('</html>') else 'NO - ARCHIVO TRUNCADO'}")
print(f"Total lines:  {c.count(chr(10))}")
print(f"\nUltimas 200 chars del JS:")
print(repr(js[-200:]))

# Find where the script ends
script_end = c.find('</script>')
print(f"\n</script> en posicion: {script_end}")
print(f"nav() en posicion: {c.find('function nav(')}")

input("\nPresiona Enter para salir...")
