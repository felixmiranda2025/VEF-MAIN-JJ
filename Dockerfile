FROM node:20-slim

# Instalar Python y pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Symlink para que "python" y "python3" funcionen
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Instalar dependencias Node
COPY package*.json ./
RUN npm install --production

# Instalar dependencias Python del SAT (si existe requirements.txt)
COPY sat_service/ ./sat_service/
RUN pip3 install --break-system-packages -r sat_service/requirements.txt 2>/dev/null || \
    pip3 install --break-system-packages satcfdi aiohttp lxml pyOpenSSL requests cryptography || true

# Copiar el resto del proyecto
COPY . .

EXPOSE 3000

CMD ["node", "server.js"]