const express = require('express');
const pino = require('pino');
const crypto = require('crypto');
const {
  default: makeWASocket,
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion
} = require('@whiskeysockets/baileys');

const PORT = Number(process.env.PORT || 10000);
const WA_SENDER_TOKEN = (process.env.WA_SENDER_TOKEN || '').trim();
const AUTH_DIR = (process.env.AUTH_DIR || './auth').trim();
const MIN_SECONDS_BETWEEN_SAME_NUMBER = Number(process.env.MIN_SECONDS_BETWEEN_SAME_NUMBER || 60);

if (!WA_SENDER_TOKEN) {
  console.error('❌ WA_SENDER_TOKEN não configurado. Encerrando.');
  process.exit(1);
}

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '256kb' }));
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  next();
});

let sock = null;
let isConnected = false;
let currentQr = null;
let isConnecting = false;
let reconnectTimer = null;

const lastSentByNumber = new Map();
const authRequestsByIp = new Map();
const MAX_AUTH_REQUESTS_PER_MINUTE = Number(process.env.MAX_AUTH_REQUESTS_PER_MINUTE || 120);

function safeTokenEquals(a, b) {
  const one = Buffer.from(String(a || ''), 'utf8');
  const two = Buffer.from(String(b || ''), 'utf8');
  if (one.length !== two.length) return false;
  return crypto.timingSafeEqual(one, two);
}

function getClientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return forwarded || req.socket?.remoteAddress || req.ip || 'unknown';
}

function isAuthRateLimited(ip) {
  const now = Date.now();
  const windowMs = 60 * 1000;
  const timestamps = authRequestsByIp.get(ip) || [];
  const filtered = timestamps.filter((ts) => now - ts < windowMs);
  if (filtered.length >= Math.max(20, MAX_AUTH_REQUESTS_PER_MINUTE)) {
    authRequestsByIp.set(ip, filtered);
    return true;
  }
  filtered.push(now);
  authRequestsByIp.set(ip, filtered);
  return false;
}

function parseBearerToken(req) {
  const auth = req.headers.authorization || '';
  const [type, token] = auth.split(' ');
  if (type !== 'Bearer' || !token) return null;
  return token;
}

function authMiddleware(req, res, next) {
  const ip = getClientIp(req);
  if (isAuthRateLimited(ip)) {
    return res.status(429).json({ ok: false, error: 'rate_limited' });
  }
  const token = parseBearerToken(req);
  if (!token || !safeTokenEquals(token, WA_SENDER_TOKEN)) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  next();
}

function validatePhone(phone) {
  const normalized = String(phone || '').replace(/\D/g, '');
  if (!/^\d{10,15}$/.test(normalized)) {
    throw new Error('phone inválido: use E.164 sem + (10 a 15 dígitos)');
  }
  return normalized;
}

function canSendToNumber(phone) {
  const nowMs = Date.now();
  const minGapMs = Math.max(0, MIN_SECONDS_BETWEEN_SAME_NUMBER) * 1000;
  const last = lastSentByNumber.get(phone) || 0;

  if (nowMs - last < minGapMs) {
    const waitSeconds = Math.ceil((minGapMs - (nowMs - last)) / 1000);
    return { ok: false, waitSeconds };
  }

  return { ok: true, waitSeconds: 0 };
}

function markSent(phone) {
  lastSentByNumber.set(phone, Date.now());
}

function scheduleReconnect(delayMs = 3000) {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectWhatsApp().catch((err) => {
      console.error('❌ Erro ao reconectar:', err.message);
      scheduleReconnect(5000);
    });
  }, delayMs);
}

async function connectWhatsApp() {
  if (isConnecting) return;
  isConnecting = true;

  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
      version,
      auth: state,
      logger: pino({ level: 'silent' })
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        currentQr = qr;
        console.log('📌 QR gerado. Acesse /qr para visualizar.');
      }

      if (connection === 'open') {
        isConnected = true;
        currentQr = null;
        console.log('✅ WhatsApp conectado.');
      }

      if (connection === 'close') {
        isConnected = false;

        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const loggedOut = statusCode === DisconnectReason.loggedOut;

        if (loggedOut) {
          console.log('⚠️ Sessão deslogada. Gerando novo QR...');
          currentQr = null;
          await connectWhatsApp().catch((err) => {
            console.error('❌ Erro ao recriar sessão:', err.message);
            scheduleReconnect(5000);
          });
        } else {
          console.log('⚠️ Conexão caiu. Reconectando...');
          scheduleReconnect(3000);
        }
      }
    });
  } finally {
    isConnecting = false;
  }
}

app.get('/healthz', (_req, res) => {
  return res.json({ ok: true });
});

app.get('/', authMiddleware, (req, res) => {
  res.set('Cache-Control', 'no-store');
  return res.json({
    ok: true,
    connected: isConnected,
    has_qr: Boolean(currentQr)
  });
});

app.get('/qr', authMiddleware, (req, res) => {
  res.set('Cache-Control', 'no-store');
  return res.json({
    ok: true,
    qr: currentQr
  });
});

app.post('/send', authMiddleware, async (req, res) => {
  try {
    if (!isConnected || !sock) {
      return res.status(503).json({ ok: false, error: 'whatsapp não conectado' });
    }

    const phone = validatePhone(req.body.phone);
    const message = String(req.body.message || '').trim();
    const orderId = req.body.order_id || null;

    if (!message) {
      return res.status(400).json({ ok: false, error: 'message obrigatório' });
    }

    const limiter = canSendToNumber(phone);
    if (!limiter.ok) {
      return res.status(429).json({
        ok: false,
        error: `aguarde ${limiter.waitSeconds}s para enviar novamente para este número`
      });
    }

    const jid = `${phone}@s.whatsapp.net`;
    const sent = await sock.sendMessage(jid, { text: message });
    markSent(phone);

    const messageId = sent?.key?.id || null;
    console.log(`📤 Envio realizado para ${phone} (order_id=${orderId || '-'})`);

    return res.json({
      ok: true,
      order_id: orderId,
      message_id: messageId
    });
  } catch (err) {
    console.error('❌ Erro no /send:', err.message);
    return res.status(400).json({ ok: false, error: err.message || 'erro ao enviar' });
  }
});

app.listen(PORT, async () => {
  console.log(`🚀 WA sender HTTP ativo na porta ${PORT}`);
  await connectWhatsApp().catch((err) => {
    console.error('❌ Falha inicial de conexão WhatsApp:', err.message);
    scheduleReconnect(5000);
  });
});
