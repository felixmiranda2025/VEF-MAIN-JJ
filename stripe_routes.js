// ================================================================
// STRIPE — Rutas de pago para registro de empresa (VEF ERP)
// Cargado automáticamente por server.js si STRIPE_SECRET_KEY existe
//
// SETUP:
//   1. npm install stripe
//   2. Agrega en .env:
//        STRIPE_SECRET_KEY=sk_live_xxxx    (o sk_test_xxxx para pruebas)
//        STRIPE_WEBHOOK_SECRET=whsec_xxxx
//   3. Stripe Dashboard → Webhooks → endpoint:
//        https://tu-dominio/api/stripe/webhook
//        Evento: payment_intent.succeeded
// ================================================================
'use strict';

module.exports = function stripeRoutes(app, pool, stripe) {
  const bcrypt  = require('bcryptjs');
  const express = require('express');

  const PRECIOS = {
    mensual: { monto: 99900,  label: '$999 MXN/mes',   meses: 1  },
    anual:   { monto: 799900, label: '$7,999 MXN/año', meses: 12 },
  };

  // ── PASO 1: Crear Payment Intent ─────────────────────────────
  app.post('/api/stripe/crear-payment-intent', async (req, res) => {
    try {
      const { plan = 'mensual', empresa_nombre = '', email = '' } = req.body;
      const precio = PRECIOS[plan] || PRECIOS.mensual;
      const pi = await stripe.paymentIntents.create({
        amount:   precio.monto,
        currency: 'mxn',
        automatic_payment_methods: { enabled: true },
        metadata: { plan, fuente: 'vef-erp-registro', empresa_nombre, email },
      });
      res.json({ clientSecret: pi.client_secret, amount: precio.monto, plan, label: precio.label });
    } catch (e) {
      console.error('Stripe PaymentIntent error:', e.message);
      res.status(500).json({ error: 'Error al iniciar pago: ' + e.message });
    }
  });

  // ── PASO 2: Webhook (pago confirmado por Stripe) ──────────────
  app.post('/api/stripe/webhook',
    express.raw({ type: 'application/json' }),
    async (req, res) => {
      let event;
      try {
        event = stripe.webhooks.constructEvent(
          req.body,
          req.headers['stripe-signature'],
          process.env.STRIPE_WEBHOOK_SECRET
        );
      } catch (err) {
        console.error('⚠️  Stripe webhook firma inválida:', err.message);
        return res.status(400).send('Webhook Error: ' + err.message);
      }

      if (event.type === 'payment_intent.succeeded') {
        const pi   = event.data.object;
        const slug = pi.metadata?.empresa_slug;
        if (slug) {
          const plan  = pi.metadata?.plan || 'mensual';
          const meses = PRECIOS[plan]?.meses || 1;
          const hasta = new Date();
          hasta.setMonth(hasta.getMonth() + meses);
          await pool.query(
            `UPDATE public.empresas
             SET suscripcion_estatus='activa', suscripcion_hasta=$1,
                 activa=true, stripe_customer_id=$2
             WHERE slug=$3`,
            [hasta.toISOString().slice(0, 10), pi.customer || null, slug]
          ).catch(e => console.error('Webhook DB error:', e.message));
          console.log('✅ Stripe webhook: empresa activada →', slug);
        }
      }
      res.json({ received: true });
    }
  );

  // ── PASO 3: Registro con pago verificado ─────────────────────
  app.post('/api/registro-con-pago', async (req, res) => {
    try {
      const { nombre, apellido = '', email, password,
              empresa_nombre, telefono = '',
              payment_intent_id, plan = 'mensual' } = req.body;

      if (!nombre || !email || !password || !empresa_nombre || !payment_intent_id)
        return res.status(400).json({ error: 'Todos los campos son requeridos' });
      if (password.length < 8)
        return res.status(400).json({ error: 'La contraseña debe tener mínimo 8 caracteres' });

      // Verificar pago con Stripe
      const pi = await stripe.paymentIntents.retrieve(payment_intent_id);
      if (pi.status !== 'succeeded')
        return res.status(402).json({ error: 'El pago no fue confirmado.', stripe_status: pi.status });

      // Evitar reutilización del payment_intent
      await pool.query(`ALTER TABLE public.empresas ADD COLUMN IF NOT EXISTS stripe_payment_intent TEXT`).catch(() => {});
      await pool.query(`ALTER TABLE public.empresas ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT`).catch(() => {});
      const piUsado = await pool.query('SELECT id FROM public.empresas WHERE stripe_payment_intent=$1', [payment_intent_id]);
      if (piUsado.rows.length > 0)
        return res.status(409).json({ error: 'Este pago ya fue utilizado.' });

      // Email único
      const existing = await pool.query('SELECT id FROM public.usuarios WHERE username=$1 OR email=$1', [email]);
      if (existing.rows.length > 0)
        return res.status(400).json({ error: 'Este email ya está registrado' });

      // Generar slug único
      let base = empresa_nombre.toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-z0-9]/g, '_').replace(/_+/g, '_').slice(0, 30);
      let slug = base, n = 2;
      while ((await pool.query('SELECT id FROM public.empresas WHERE slug=$1', [slug])).rows.length > 0) {
        slug = base + '_' + (n++);
      }

      // Calcular vigencia
      const meses = PRECIOS[plan]?.meses || 1;
      const hasta = new Date();
      hasta.setMonth(hasta.getMonth() + meses);
      const hastaStr = hasta.toISOString().slice(0, 10);

      // Crear empresa activa
      const emp = await pool.query(
        `INSERT INTO public.empresas (slug, nombre, suscripcion_estatus, suscripcion_hasta, activa, stripe_payment_intent)
         VALUES ($1,$2,'activa',$3,true,$4) RETURNING *`,
        [slug, empresa_nombre, hastaStr, payment_intent_id]
      );
      const empId = emp.rows[0]?.id;

      // Crear schema con todas las tablas (función global de server.js)
      let schema = 'emp_' + slug.replace(/[^a-z0-9]/g, '_');
      if (typeof global.crearSchemaEmpresa === 'function') {
        schema = await global.crearSchemaEmpresa(slug, empresa_nombre);
      }

      // Crear usuario admin
      const hash     = await bcrypt.hash(password, 12);
      const fullName = (nombre + ' ' + apellido).trim();
      const usr = await pool.query(
        `INSERT INTO public.usuarios (username,nombre,email,password_hash,password,rol,empresa_id,schema_name)
         VALUES ($1,$2,$3,$4,$4,'admin',$5,$6) RETURNING id,username,nombre,email,rol`,
        [email, fullName, email, hash, empId, schema]
      );

      // Actualizar empresa_config
      const client = await pool.connect();
      try {
        await client.query(`SET search_path TO "${schema}", public`);
        await client.query(`UPDATE empresa_config SET nombre=$1,email=$2,telefono=$3`,
          [empresa_nombre, email, telefono]);
      } finally { client.release(); }

      console.log('✅ Registro con pago:', email, '→', schema, 'hasta', hastaStr);
      res.status(201).json({
        ok:      true,
        mensaje: `Cuenta activada. Suscripción ${plan} activa hasta ${hastaStr}.`,
        empresa: { id: empId, nombre: empresa_nombre, slug, suscripcion_hasta: hastaStr },
        usuario: { id: usr.rows[0]?.id, nombre: fullName, email },
      });
    } catch (e) {
      console.error('Registro con pago error:', e.message);
      res.status(500).json({ error: e.message });
    }
  });

  // ── Info pública de planes ────────────────────────────────────
  app.get('/api/stripe/planes', (_req, res) => {
    res.json({
      mensual: { monto: PRECIOS.mensual.monto / 100, moneda: 'MXN', label: PRECIOS.mensual.label },
      anual:   { monto: PRECIOS.anual.monto   / 100, moneda: 'MXN', label: PRECIOS.anual.label   },
    });
  });
};
