const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');

// ============================================================
// KONFIGURATION
// ============================================================
const CONFIG = {
  HEARTBEAT_INTERVAL: 15000,
  ROOM_IDLE_TIMEOUT: 600000,
  MAX_ROOM_SIZE: 2,
  JOIN_RATE_LIMIT: 5,
  JOIN_RATE_WINDOW: 60000,
  CHUNK_INTERVAL_MS: 30,
  VIDEO_BITRATE: 800000,
  AUDIO_BITRATE: 48000,
  BUFFER_HIGH: 1.5,
  BUFFER_LOW: 0.4,
  MAX_RECONNECT_ATTEMPTS: 3,
  RECONNECT_BASE_DELAY: 1000
};

// ============================================================
// LOGGING
// ============================================================
function log(level, msg) {
  const time = new Date().toISOString();
  console.log(`[${time}] [${level}] ${msg}`);
}
const logInfo  = (m) => log('INFO', m);
const logWarn  = (m) => log('WARN', m);
const logError = (m) => log('ERROR', m);

// ============================================================
// HTML
// ============================================================
const HTML = `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NERON DECK</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#0a0e14; color:#00ffcc; font-family:'Courier New',monospace; padding:15px; text-align:center; min-height:100vh; }
h1 { font-size:24px; letter-spacing:3px; text-shadow:0 0 10px #00ffcc; margin-bottom:8px; }
h2 { font-size:13px; letter-spacing:2px; margin:12px 0 8px; }
.code { font-size:36px; letter-spacing:10px; text-shadow:0 0 15px #00ffcc; padding:12px; background:#000; border-radius:6px; margin:8px auto; max-width:350px; font-weight:bold; }
button { background:transparent; color:#00ffcc; border:2px solid #00ffcc; padding:8px 18px; font-size:13px; font-family:'Courier New',monospace; font-weight:bold; cursor:pointer; margin:4px; border-radius:4px; }
button:hover:not(:disabled) { background:#00ffcc; color:#0a0e14; }
button:disabled { opacity:0.3; cursor:not-allowed; }
input { background:#000; color:#00ffcc; border:2px solid #00ffcc; padding:8px; font-size:20px; text-align:center; width:150px; letter-spacing:6px; border-radius:4px; font-family:'Courier New',monospace; outline:none; }
#video { width:100%; max-width:900px; border:2px solid #00ffcc44; border-radius:8px; background:#000; margin:12px auto; display:block; aspect-ratio:16/9; }
.status { color:#00ffcc; font-size:13px; margin-top:8px; font-weight:bold; }
.warn { color:#ff4444; font-size:13px; margin-top:8px; font-weight:bold; }
.log { text-align:left; font-size:10px; color:#00ffccaa; max-height:250px; overflow-y:auto; margin:15px auto; max-width:900px; background:#0d1117; padding:10px; border-radius:6px; border:1px solid #00ffcc22; }
.log-entry { margin-bottom:3px; padding:2px 0; border-bottom:1px solid #00ffcc11; }
.log-error { color:#ff4444; }
.log-warn { color:#ffaa00; }
.log-ok { color:#00ffcc; }
</style>
</head>
<body>
<h1>NERON DECK</h1>
<p style="color:#00ffcc66;font-size:10px;">Low-Latency Mode</p>

<h2>&#9654; SENDER</h2>
<div class="code" id="myCode">------</div>
<button id="screenBtn">&#9654; BILDSCHIRM</button>
<button id="camBtn">&#9654; KAMERA</button>
<button id="stopBtn" disabled>&#9632; STOP</button>

<h2>&#9664; RECEIVER</h2>
<input type="text" id="joinCode" maxlength="6" placeholder="000000" inputmode="numeric">
<button id="joinBtn">VERBINDEN</button>
<div class="warn" id="warnBox"></div>

<video id="video" autoplay playsinline muted></video>
<div class="status" id="status">Bereit</div>
<div class="log" id="log"></div>

<script>
(function() {
  'use strict';

  const CONFIG = {
    CHUNK_INTERVAL_MS: 30,
    VIDEO_BITRATE: 800000,
    AUDIO_BITRATE: 48000,
    BUFFER_HIGH: 1.5,
    BUFFER_LOW: 0.4,
    MAX_RECONNECT_ATTEMPTS: 3,
    RECONNECT_BASE_DELAY: 1000
  };

  // ---------- DOM ----------
  const videoEl     = document.getElementById('video');
  const statusEl    = document.getElementById('status');
  const warnEl      = document.getElementById('warnBox');
  const logEl       = document.getElementById('log');
  const myCodeEl    = document.getElementById('myCode');
  const joinCodeEl  = document.getElementById('joinCode');
  const screenBtn   = document.getElementById('screenBtn');
  const camBtn      = document.getElementById('camBtn');
  const stopBtn     = document.getElementById('stopBtn');
  const joinBtn     = document.getElementById('joinBtn');

  // ---------- STATE ----------
  let ws = null;
  let mediaRecorder = null;
  let mediaSource = null;
  let sourceBuffer = null;
  let currentStream = null;
  let isSender = false;
  let chunkCount = 0;
  let bytesReceived = 0;
  let receiverReady = false;
  let firstRealChunkReceived = false;
  let paused = false;
  let reconnectAttempts = 0;
  let lastRoomCode = null;
  let lastRole = null;
  let lastCallback = null;
  let myCode = null;
  let pendingType = null;
  let streamRunning = false;

  const myUserId = 'user-' + Math.floor(Math.random() * 10000);
  const WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host;

  const canScreen = !!(navigator.mediaDevices && navigator.mediaDevices.getDisplayMedia);
  const canCam    = !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
  if (!canScreen) screenBtn.style.display = 'none';
  if (!canCam)    camBtn.style.display = 'none';

  // ---------- ERROR ----------
  function explainError(err) {
    if (!err) return 'Unbekannter Fehler';
    if (err.name === 'NotAllowedError') return 'Berechtigung verweigert.';
    if (err.name === 'NotFoundError') return 'Kein Gerät gefunden.';
    if (err.name === 'NotReadableError') return 'Gerät blockiert (andere App).';
    if (err.name === 'NotSupportedError') return 'Nicht unterstützt.';
    if (err.name === 'QuotaExceededError') return 'Buffer voll.';
    if (err.name === 'InvalidStateError') return 'Buffer wurde entfernt.';
    if (err.code === 1000) return 'Verbindung normal geschlossen.';
    if (err.code === 1001) return 'Verbindung geschlossen (Tab zu).';
    if (err.code === 1006) return 'Verbindung unerwartet abgebrochen.';
    if (err.code === 1011) return 'Server-Fehler.';
    return (err.name || 'Error') + ': ' + (err.message || String(err));
  }

  // ---------- LOG ----------
  function log(msg, type) {
    let cls = 'log-entry';
    if (type === 'error') cls += ' log-error';
    else if (type === 'warn') cls += ' log-warn';
    else if (type === 'ok') cls += ' log-ok';
    const time = new Date().toLocaleTimeString();
    const entry = document.createElement('div');
    entry.className = cls;
    entry.textContent = '[' + time + '] ' + msg;
    logEl.prepend(entry);
    if (type === 'error') console.error(msg);
    else if (type === 'warn') console.warn(msg);
    else console.log(msg);
  }

  // ---------- APPEND ----------
  function appendChunk(data) {
    if (!data || data.byteLength === 0) return;
    if (!sourceBuffer || !mediaSource) return;
    if (mediaSource.readyState !== 'open') return;
    if (sourceBuffer.updating) {
      setTimeout(() => appendChunk(data), 10);
      return;
    }
    try {
      sourceBuffer.appendBuffer(data);
      if (!firstRealChunkReceived) {
        firstRealChunkReceived = true;
        log('Erster ECHTER Chunk verarbeitet (' + data.byteLength + ' bytes)', 'ok');
      }
      checkBufferLevel();
    } catch (e) {
      log('appendBuffer: ' + explainError(e), 'error');
    }
  }

  // ---------- BACKPRESSURE ----------
  function checkBufferLevel() {
    if (isSender) return;
    if (videoEl.buffered.length === 0) return;
    const bufferedEnd = videoEl.buffered.end(videoEl.buffered.length - 1);
    const bufferAhead = bufferedEnd - videoEl.currentTime;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    if (bufferAhead > CONFIG.BUFFER_HIGH && !paused) {
      ws.send('BUFFER_HIGH');
      paused = true;
      log('Buffer HIGH (' + bufferAhead.toFixed(2) + 's) - Pause', 'warn');
    } else if (bufferAhead < CONFIG.BUFFER_LOW && paused) {
      ws.send('BUFFER_LOW');
      paused = false;
      log('Buffer LOW (' + bufferAhead.toFixed(2) + 's) - Resume', 'ok');
    }
  }

  // ---------- CONNECT ----------
  function connect(code, asSender, onOpenCallback) {
    lastRoomCode = code;
    lastRole = asSender;
    lastCallback = onOpenCallback || null;

    // FIX A: intentionalClose pro ws-Instanz statt global
    if (ws) {
      const oldWs = ws;
      try {
        oldWs._intentional = true;
        oldWs.close();
      } catch (e) {}
    }

    ws = new WebSocket(WS_URL);
    ws.binaryType = 'arraybuffer';
    ws._intentional = false;
    isSender = asSender;
    warnEl.textContent = '';
    chunkCount = 0;
    bytesReceived = 0;
    receiverReady = false;
    firstRealChunkReceived = false;
    paused = false;

    ws.onopen = function() {
      log('WebSocket OFFEN', 'ok');
      reconnectAttempts = 0;
      if (code) {
        ws.send('JOIN:' + code + ':' + (asSender ? 'sender' : 'receiver') + ':' + myUserId);
        statusEl.textContent = 'Raum ' + code + ' verbunden';
        if (!asSender && sourceBuffer) sendReceiverReady();
      }
      if (onOpenCallback) onOpenCallback();
    };

    ws.onmessage = function(event) {
      if (typeof event.data === 'string') {
        handleServerMessage(event.data);
        return;
      }
      if (event.data instanceof ArrayBuffer && !isSender) {
        chunkCount++;
        bytesReceived += event.data.byteLength;
        if (chunkCount % 50 === 0) {
          log('Chunks: ' + chunkCount + ' | ' + (bytesReceived/1024).toFixed(1) + ' KB');
        }
        appendChunk(event.data);
      }
    };

    ws.onerror = function(e) {
      log('WebSocket ERROR: ' + explainError(e), 'error');
    };

    ws.onclose = function(e) {
      log('WebSocket GESCHLOSSEN: ' + explainError(e), 'warn');
      // FIX A: pro-Instanz-Flag
      if (ws._intentional) return;
      if (reconnectAttempts < CONFIG.MAX_RECONNECT_ATTEMPTS && lastRoomCode) {
        reconnectAttempts++;
        const delay = CONFIG.RECONNECT_BASE_DELAY * Math.pow(2, reconnectAttempts - 1);
        log('Reconnect in ' + (delay/1000) + 's (' + reconnectAttempts + '/' + CONFIG.MAX_RECONNECT_ATTEMPTS + ')', 'warn');
        setTimeout(function() {
          connect(lastRoomCode, lastRole, lastCallback);
        }, delay);
      }
    };
  }

  function sendReceiverReady() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send('RECEIVER_READY');
      log('RECEIVER_READY gesendet', 'ok');
    }
  }

  // ---------- RESET SENDER UI ----------
  // FIX B/C: Hilfsfunktion für konsistenten Reset
  function resetSenderState() {
    myCode = null;
    myCodeEl.textContent = '------';
    statusEl.textContent = 'Bereit';
    receiverReady = false;
  }

  // ---------- SERVER-NACHRICHTEN ----------
  function handleServerMessage(msg) {
    log('SERVER: ' + msg);

    if (msg === 'ROOM_EMPTY')   warnEl.textContent = 'Kein Sender in diesem Raum!';
    if (msg === 'SENDER_JOINED'){ warnEl.textContent = ''; log('Sender ist da!', 'ok'); }
    if (msg === 'SENDER_LEFT')   warnEl.textContent = 'Sender hat den Raum verlassen!';
    if (msg === 'ERROR:no_code') warnEl.textContent = 'Server konnte keinen Code vergeben.';

    // FIX C: RATE_LIMITED / ROOM_FULL im Sender-Kontext → Reset
    if (msg === 'RATE_LIMITED') {
      warnEl.textContent = 'Zu viele Versuche. Warte eine Minute.';
      if (isSender) {
        log('Sender-Reset wegen RATE_LIMITED', 'warn');
        resetSenderState();
      }
    }
    if (msg === 'ROOM_FULL') {
      warnEl.textContent = 'Raum ist voll (max. 2 Teilnehmer)!';
      if (isSender) {
        log('Sender-Reset wegen ROOM_FULL', 'warn');
        resetSenderState();
      }
    }

    // Code vom Server erhalten → JETZT joinen
    if (msg.startsWith('CODE:')) {
      myCode = msg.substring(5);
      myCodeEl.textContent = myCode;
      log('Code vom Server: ' + myCode, 'ok');
      statusEl.textContent = 'Warte auf Empfänger...';
      if (ws && ws.readyState === WebSocket.OPEN && isSender) {
        ws.send('JOIN:' + myCode + ':sender:' + myUserId);
        lastRoomCode = myCode;
        log('JOIN gesendet mit Code ' + myCode, 'ok');
        if (receiverReady && pendingType && !streamRunning) {
          startStream(pendingType);
          pendingType = null;
        }
      }
    }

    if (msg === 'RECEIVER_READY') {
      receiverReady = true;
      log('Empfänger bereit!', 'ok');
      if (isSender && pendingType && !streamRunning) {
        startStream(pendingType);
        pendingType = null;
      }
    }

    if (msg === 'BUFFER_HIGH' && isSender && mediaRecorder && mediaRecorder.state === 'recording') {
      mediaRecorder.pause();
      log('Empfänger-Buffer voll - pausiere Sender', 'warn');
    }
    if (msg === 'BUFFER_LOW' && isSender && mediaRecorder && mediaRecorder.state === 'paused') {
      mediaRecorder.resume();
      log('Empfänger-Buffer leer - setze Sender fort', 'ok');
    }
  }

  // ---------- CODE ANFORDERN ----------
  function requestNewCode() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send('REQUEST_CODE');
      log('REQUEST_CODE gesendet', 'ok');
    }
  }

  // ---------- EMPFÄNGER SETUP ----------
  function setupReceiver() {
    log('Richte Empfänger ein...');
    if (mediaSource) {
      try { URL.revokeObjectURL(videoEl.src); } catch (e) {}
    }
    mediaSource = new MediaSource();
    videoEl.src = URL.createObjectURL(mediaSource);

    mediaSource.addEventListener('sourceopen', function() {
      log('MediaSource OFFEN', 'ok');
      try {
        const codecs = [
          'video/webm;codecs=h264,opus',
          'video/webm;codecs=vp9,opus',
          'video/webm;codecs=vp8,opus',
          'video/webm'
        ];
        let chosen = null;
        for (const c of codecs) {
          if (MediaSource.isTypeSupported(c)) { chosen = c; break; }
        }
        if (!chosen) { log('Kein Codec unterstützt!', 'error'); return; }

        sourceBuffer = mediaSource.addSourceBuffer(chosen);
        sourceBuffer.mode = 'sequence';
        sourceBuffer.timestampOffset = 0;
        log('SourceBuffer bereit (' + chosen + ')', 'ok');

        sourceBuffer.addEventListener('error', function() {
          log('SourceBuffer ERROR - Rebuild', 'warn');
          try {
            if (mediaSource.readyState === 'open') {
              mediaSource.removeSourceBuffer(sourceBuffer);
              sourceBuffer = mediaSource.addSourceBuffer(chosen);
              sourceBuffer.mode = 'sequence';
              sourceBuffer.timestampOffset = 0;
            }
          } catch (e) { log('Rebuild: ' + explainError(e), 'error'); }
        });

        sendReceiverReady();
      } catch (e) {
        log('addSourceBuffer: ' + explainError(e), 'error');
      }
    });
  }

  // ---------- SENDER START ----------
  async function startStream(type) {
    if (streamRunning) { log('Stream läuft bereits', 'warn'); return; }
    try {
      log('Starte Stream (' + type + ')...');
      let stream;

      if (type === 'screen') {
        stream = await navigator.mediaDevices.getDisplayMedia({
          video: { frameRate: { ideal: 30, max: 30 } },
          audio: true
        });
      } else {
        stream = await navigator.mediaDevices.getUserMedia({
          video: { frameRate: { ideal: 30, max: 30 }, width: { ideal: 1280 }, height: { ideal: 720 } },
          audio: true
        });
      }

      log('Stream erhalten', 'ok');
      currentStream = stream;
      videoEl.srcObject = stream;
      videoEl.muted = true;

      let mimeType = 'video/webm;codecs=vp8,opus';
      const candidates = ['video/webm;codecs=h264,opus', 'video/webm;codecs=vp8,opus', 'video/webm'];
      for (const c of candidates) {
        if (MediaRecorder.isTypeSupported(c)) { mimeType = c; break; }
      }
      log('MediaRecorder: ' + mimeType);

      mediaRecorder = new MediaRecorder(stream, {
        mimeType: mimeType,
        videoBitsPerSecond: CONFIG.VIDEO_BITRATE,
        audioBitsPerSecond: CONFIG.AUDIO_BITRATE
      });

      mediaRecorder.ondataavailable = function(event) {
        if (event.data.size > 0 && ws && ws.readyState === WebSocket.OPEN && isSender) {
          chunkCount++;
          event.data.arrayBuffer().then(function(buf) {
            try {
              ws.send(buf);
              if (chunkCount % 100 === 0) {
                log('Gesendet: ' + chunkCount + ' | ' + (buf.byteLength/1024).toFixed(1) + ' KB');
              }
            } catch (e) {}
          });
        }
      };

      mediaRecorder.start(CONFIG.CHUNK_INTERVAL_MS);
      streamRunning = true;
      log('Stream gestartet (' + type + ')', 'ok');
      screenBtn.disabled = true;
      camBtn.disabled = true;
      stopBtn.disabled = false;

      stream.getVideoTracks()[0].onended = stopStream;
    } catch (err) {
      log('Stream: ' + explainError(err), 'error');
      streamRunning = false;
    }
  }

  // ---------- STOP ----------
  function stopStream() {
    try {
      if (mediaRecorder && mediaRecorder.state !== 'inactive') mediaRecorder.stop();
    } catch (e) {}
    if (currentStream) {
      currentStream.getTracks().forEach(function(t) { t.stop(); });
      currentStream = null;
    }
    mediaRecorder = null;
    streamRunning = false;
    pendingType = null;

    // FIX B: Sender-State komplett zurücksetzen
    resetSenderState();

    screenBtn.disabled = false;
    camBtn.disabled = false;
    stopBtn.disabled = true;
    log('Stream gestoppt', 'warn');
  }

  // ---------- SENDER-FLOW ----------
  function startSenderFlow(type) {
    pendingType = type;
    if (ws && ws.readyState === WebSocket.OPEN && myCode) {
      ws.send('JOIN:' + myCode + ':sender:' + myUserId);
      statusEl.textContent = 'Raum ' + myCode + ' verbunden';
      if (receiverReady && !streamRunning) {
        startStream(type);
        pendingType = null;
      }
      return;
    }
    connect(null, true, function() {
      requestNewCode();
    });
  }

  // ---------- INIT ----------
  log('Seite geladen. User: ' + myUserId);
  log('Bildschirm: ' + canScreen + ' | Kamera: ' + canCam);

  screenBtn.addEventListener('click', function() { startSenderFlow('screen'); });
  camBtn.addEventListener('click',    function() { startSenderFlow('cam'); });
  stopBtn.addEventListener('click',   stopStream);

  joinBtn.addEventListener('click', function() {
    const code = joinCodeEl.value.trim();
    if (!/^\d{6}$/.test(code)) { alert('6-stelligen Code eingeben'); return; }
    log('Verbinde als Empfänger mit Raum ' + code);
    setupReceiver();
    connect(code, false, null);
  });
})();
</script>
</body>
</html>`;

// ============================================================
// HTTP-SERVER
// ============================================================
const server = http.createServer(function(req, res) {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(HTML);
});

// ============================================================
// WEBSOCKET-SERVER
// ============================================================
const wss = new WebSocket.Server({ server });

const rooms = new Map();
const rateLimits = new Map();

// ============================================================
// HILFSFUNKTIONEN
// ============================================================
function generateRoomCode() {
  let attempts = 0;
  while (attempts < 20) {
    const code = String(crypto.randomInt(100000, 1000000));
    if (!rooms.has(code)) return code;
    attempts++;
  }
  logError('Konnte keinen freien Raum-Code generieren');
  return null;
}

function getRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, {
      senders: new Set(),
      receivers: new Set(),
      timer: null
    });
  }
  return rooms.get(code);
}

function checkRateLimit(ip) {
  const now = Date.now();
  const entry = rateLimits.get(ip);
  if (!entry || now > entry.resetTime) {
    rateLimits.set(ip, { count: 1, resetTime: now + CONFIG.JOIN_RATE_WINDOW });
    return true;
  }
  if (entry.count >= CONFIG.JOIN_RATE_LIMIT) return false;
  entry.count++;
  return true;
}

function resetRoomTimer(code) {
  const room = rooms.get(code);
  if (!room) return;
  if (room.timer) clearTimeout(room.timer);
  room.timer = setTimeout(function() {
    logInfo('Raum ' + code + ' wegen Inaktivität gelöscht');
    rooms.delete(code);
  }, CONFIG.ROOM_IDLE_TIMEOUT);
}

function safeSend(ws, data) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(data);
    return true;
  } catch (e) {
    logWarn('ws.send() fehlgeschlagen: ' + e.message);
    return false;
  }
}

// ============================================================
// VERBINDUNGEN
// ============================================================
wss.on('connection', function(ws, req) {
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
  let roomCode = null;
  let role = null;
  let userId = null;
  let isAlive = true;
  let heartbeat = null;

  logInfo('Neue Verbindung von ' + clientIp);

  ws.on('pong', function() { isAlive = true; });

  heartbeat = setInterval(function() {
    if (!isAlive) {
      logWarn('Client ' + userId + ' (' + clientIp + ') antwortet nicht - trenne');
      clearInterval(heartbeat);
      ws.terminate();
      return;
    }
    isAlive = false;
    try { ws.ping(); } catch (e) {}
  }, CONFIG.HEARTBEAT_INTERVAL);

  ws.on('message', function(data) {
    const text = data.toString();

    // --- REQUEST_CODE ---
    if (text === 'REQUEST_CODE') {
      const newCode = generateRoomCode();
      if (!newCode) {
        safeSend(ws, 'ERROR:no_code');
        return;
      }
      getRoom(newCode);
      safeSend(ws, 'CODE:' + newCode);
      logInfo('Code ' + newCode + ' an ' + clientIp + ' vergeben');
      return;
    }

    // --- JOIN ---
    if (text.startsWith('JOIN:')) {
      const parts = text.split(':');
      const joinCode = parts[1];
      const joinRole = parts[2];
      const joinUserId = parts[3] || 'unknown';

      if (!/^\d{6}$/.test(joinCode)) {
        safeSend(ws, 'RATE_LIMITED');
        logWarn('Ungültiger Code von ' + clientIp + ': ' + joinCode);
        return;
      }

      if (joinRole === 'receiver' && !rooms.has(joinCode)) {
        safeSend(ws, 'ROOM_EMPTY');
        logInfo('Empfänger ' + joinUserId + ' → nicht-existenter Raum ' + joinCode);
        return;
      }

      if (!checkRateLimit(clientIp)) {
        safeSend(ws, 'RATE_LIMITED');
        logWarn('Rate-Limit für ' + clientIp);
        return;
      }

      const targetRoom = getRoom(joinCode);

      const totalClients = targetRoom.senders.size + targetRoom.receivers.size;
      if (totalClients >= CONFIG.MAX_ROOM_SIZE) {
        safeSend(ws, 'ROOM_FULL');
        logWarn('Raum ' + joinCode + ' voll - ' + joinUserId + ' abgelehnt');
        return;
      }

      roomCode = joinCode;
      role = joinRole;
      userId = joinUserId;
      resetRoomTimer(roomCode);

      logInfo('User: ' + userId + ' (' + role + ') → Raum ' + roomCode);

      if (role === 'sender') {
        targetRoom.senders.add(ws);
        for (const r of targetRoom.receivers) {
          safeSend(r, 'SENDER_JOINED');
        }
      } else {
        targetRoom.receivers.add(ws);
        if (targetRoom.senders.size === 0) safeSend(ws, 'ROOM_EMPTY');
        else safeSend(ws, 'SENDER_JOINED');
      }
      return;
    }

    // --- RECEIVER_READY ---
    if (text === 'RECEIVER_READY') {
      const room = rooms.get(roomCode);
      if (room) {
        resetRoomTimer(roomCode);
        for (const s of room.senders) safeSend(s, 'RECEIVER_READY');
      }
      return;
    }

    // --- BACKPRESSURE ---
    if (text === 'BUFFER_HIGH' || text === 'BUFFER_LOW') {
      const room = rooms.get(roomCode);
      if (room) {
        for (const s of room.senders) safeSend(s, text);
      }
      return;
    }

    // --- BINÄR (Streaming) ---
    if (roomCode) {
      const room = rooms.get(roomCode);
      if (!room) return;
      resetRoomTimer(roomCode);
      if (role === 'sender') {
        for (const r of room.receivers) {
          if (r !== ws) safeSend(r, data);
        }
      }
    }
  });

  ws.on('close', function() {
    if (heartbeat) clearInterval(heartbeat);
    logInfo('User: ' + userId + ' hat Raum ' + roomCode + ' verlassen');

    if (!roomCode) return;
    const room = rooms.get(roomCode);
    if (!room) return;

    if (role === 'sender') {
      room.senders.delete(ws);
      for (const r of room.receivers) safeSend(r, 'SENDER_LEFT');
    } else {
      room.receivers.delete(ws);
    }

    if (room.senders.size === 0 && room.receivers.size === 0) {
      if (room.timer) clearTimeout(room.timer);
      rooms.delete(roomCode);
      logInfo('Raum ' + roomCode + ' gelöscht (leer)');
    }
  });

  ws.on('error', function(err) {
    logError('WebSocket-Fehler: ' + err.message);
  });
});

// ============================================================
// SERVER START
// ============================================================
const PORT = process.env.PORT || 8080;
server.listen(PORT, function() {
  logInfo('NERON DECK Relay läuft auf Port ' + PORT);
});
