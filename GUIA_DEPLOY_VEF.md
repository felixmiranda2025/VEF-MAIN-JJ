# 🚀 GUÍA DEPLOY — VEF ERP en Railway + AWS RDS
# ================================================
# Tu BD: entorno-1.cx6c6s6i0ith.us-east-2.rds.amazonaws.com
# Región AWS: us-east-2 (Ohio)
# ================================================

## ✅ PASO 1 — Abrir puerto 5432 en AWS RDS (MUY IMPORTANTE)

Sin este paso, Railway NO puede conectarse a tu base de datos.

1. Entra a: https://console.aws.amazon.com
2. Ve a: RDS → Bases de datos → entorno-1
3. En la pestaña "Conectividad y seguridad" → clic en el Security Group
4. Pestaña "Reglas de entrada" → "Editar reglas de entrada"
5. Agrega esta regla:
      Tipo:    PostgreSQL
      Puerto:  5432
      Origen:  0.0.0.0/0    ← Permite Railway (puedes restringir después)
6. Guarda los cambios ✅


## ✅ PASO 2 — Copiar archivos a tu proyecto

Copia estos 2 archivos a la carpeta raíz de tu proyecto
(junto a server.js, package.json):

   ✓ railway.toml
   ✓ .gitignore  (reemplaza si ya tienes uno)

Tu carpeta debe verse así:
   📁 VEF_ERP/
      ├── frontend/
      ├── node_modules/      ← NO se sube (está en .gitignore)
      ├── pdfs_guardados/    ← NO se sube (está en .gitignore)
      ├── .env               ← NO se sube (está en .gitignore)
      ├── .gitignore         ← ✅ nuevo
      ├── railway.toml       ← ✅ nuevo
      ├── logo.png
      ├── package.json
      ├── package-lock.json
      ├── server.js
      └── VEF.png


## ✅ PASO 3 — Subir proyecto a GitHub

Abre terminal (cmd) en la carpeta de tu proyecto:

   git init
   git add .
   git commit -m "VEF ERP - Deploy inicial a Railway"

Crea un repositorio nuevo en https://github.com/new
   → Nombre: vef-erp  (o el que quieras)
   → Privado ✓ (recomendado)
   → NO marques "Initialize with README"

Luego conecta y sube:
   git remote add origin https://github.com/TU_USUARIO/vef-erp.git
   git branch -M main
   git push -u origin main


## ✅ PASO 4 — Crear proyecto en Railway

1. Ve a https://railway.app → Inicia sesión con GitHub
2. Clic en "New Project"
3. Selecciona "Deploy from GitHub repo"
4. Elige tu repositorio: vef-erp
5. Railway detecta Node.js automáticamente ✓


## ✅ PASO 5 — Variables de entorno en Railway

En Railway → tu proyecto → pestaña "Variables"
Agrega EXACTAMENTE estas variables (copia y pega):

   PORT              = 3000
   DB_HOST           = entorno-1.cx6c6s6i0ith.us-east-2.rds.amazonaws.com
   DB_PORT           = 5432
   DB_NAME           = postgres
   DB_USER           = postgres
   DB_PASS           = Vef_prueba1
   NODE_ENV          = production
   JWT_SECRET        = vef_erp_secret_2025_xZ9k
   SMTP_HOST         = smtp.zoho.com
   SMTP_PORT         = 465
   SMTP_USER         = soporte.ventas@vef-automatizacion.com
   SMTP_PASS         = Brabata2323!
   FRONTEND_URL      = *
   LOGO_FILE         = logo.png

⚠️  IMPORTANTE: Railway lee estas variables automáticamente.
    Tu .env local se ignora en producción (gracias al .gitignore)


## ✅ PASO 6 — Verificar el despliegue

Después de guardar las variables, Railway redespliega solo.
En la pestaña "Deployments" verás los logs en tiempo real.

Busca estas líneas en los logs:
   ✅ Conectado a AWS RDS PostgreSQL
   ✅ Setup VEF ERP completo
   🚀 http://localhost:3000


## ✅ PASO 7 — Obtener tu URL pública

En Railway → pestaña "Settings" → sección "Domains"
Clic en "Generate Domain" → obtendrás algo como:
   https://vef-erp-production.up.railway.app

¡Esa es tu URL pública! Compártela con tu equipo.

Entra con:
   Usuario: admin
   Contraseña: admin123


## 🔁 ACTUALIZAR TU ERP después del primer deploy

Cada vez que hagas cambios al código:
   git add .
   git commit -m "descripción del cambio"
   git push

Railway redespliega automáticamente en ~2 minutos 🎉


## ⚠️  SOLUCIÓN DE ERRORES COMUNES

| Error en logs                        | Solución                                   |
|--------------------------------------|--------------------------------------------|
| ECONNREFUSED 5432                    | Abre puerto en AWS Security Group (Paso 1) |
| password authentication failed       | Verifica DB_PASS en Variables Railway      |
| SSL SYSCALL error                     | Normal — tu server.js ya maneja SSL ✓      |
| Module not found                     | Ejecuta: npm install  y vuelve a subir     |
| Build failed: no start script         | Verifica que package.json tenga "start"    |
| ETIMEDOUT al conectar a RDS           | Revisa que RDS no esté en VPC privada      |

## 📌 NOTA SOBRE PDFS_GUARDADOS

En Railway el sistema de archivos es temporal (se borra al redesplegar).
Los PDFs se guardan en la BD (columna pdf_data en BYTEA) — eso funciona OK.
La carpeta pdfs_guardados/ solo sirve como cache local.
No necesitas hacer nada adicional para esto.
