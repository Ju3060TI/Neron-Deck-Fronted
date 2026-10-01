const WebSocket = require('ws');
const http = require('http');

const HTML = `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NERON DECK - Debug</title>
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
.info { color:#ffaa00; font-size:12px; margin-top:8px; }
.log { text-align:left; font-size:10px; color:#00ffccaa; max-height:250px; overflow-y:auto; margin:15px auto; max-width:900px; background:#0d1117; padding:10px; border-radius:6px; border:1px solid #00ffcc22; }
.log-entry { margin-bottom:3px; padding:2px 0; border-bottom:1px solid #00ffcc11; }
.log-error { color:#ff4444; }
.log-warn { color:#ffaa00; }
.log-ok { color:#00ffcc; }
</style>
</head>
<body>
<h1>NERON DECK</h1>
<p style="color:#00ffcc66;font-size:10px;">DEBUG MODE</p>

<h2>&#9654; SENDER</h2>
<div class="code" id="myCode">------</div>
<button id="screenBtn">&#9654; BILDSCHIRM</button>
<button id="camBtn">&#9654; KAMERA</button>
<button id="stopBtn" disabled>&#9632; STOP</button>

<h2>&#9664; RECEIVER</h2>
<input type="text" id="joinCode" maxlength="6" placeholder="000000" inputmode="numeric">
<button id="joinBtn">VERBINDEN</button>
<div class="warn" id="warnBox"></div>
<div class="info" id="infoBox"></div>

<video id="video" autoplay playsinline muted></video>
<div class="status" id="status">Bereit</div>
<div class="log" id="log"></div>

<script>
var videoEl = document.getElementById('video');
var statusEl = document.getElementById('status');
var warnEl = document.getElementById('warnBox');
var infoEl = document.getElementById('infoBox');
var logEl = document.getElementById('log');
var myCodeEl = document.getElementById('myCode');
var joinCodeEl = document.getElementById('joinCode');
var screenBtn = document.getElementById('screenBtn');
var camBtn = document.getElementById('camBtn');
var stopBtn = document.getElementById('stopBtn');
var joinBtn = document.getElementById('joinBtn');

var ws = null;
var mediaRecorder = null;
var mediaSource = null;
var sourceBuffer = null;
var currentStream = null;
var isSender = false;
var chunkCount = 0;
var bytesReceived = 0;

var WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host;

var canScreen = !!(navigator.mediaDevices && navigator.mediaDevices.getDisplayMedia);
var canCam = !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);

if (!canScreen) screenBtn.style.display = 'none';
if (!canCam) camBtn.style.display = 'none';

// ============================================================
// FEHLER-ÜBERSETZER
// ============================================================
function erklärFehler(err) {
  if (!err) return 'Unbekannter Fehler (null/undefined)';

  if (err instanceof SyntaxError) return 'SyntaxError: Code hat Tippfehler.';
  if (err instanceof TypeError) return 'TypeError: ' + (err.message || 'Falscher Datentyp oder undefined.');
  if (err instanceof ReferenceError) return 'ReferenceError: Variable existiert nicht.';
  if (err instanceof RangeError) return 'RangeError: Wert außerhalb des erlaubten Bereichs.';

  if (err.name === 'NotAllowedError') return 'Berechtigung verweigert. Du hast "Blockieren" geklickt oder die Seite ist nicht HTTPS.';
  if (err.name === 'NotFoundError') return 'Kein Gerät gefunden. Keine Kamera / kein Bildschirm verfügbar.';
  if (err.name === 'NotReadableError') return 'Gerät blockiert. Eine andere App (WhatsApp, Kamera-App) benutzt es gerade.';
  if (err.name === 'OverconstrainedError') return 'Einstellungen nicht unterstützt. Deine Kamera kann z.B. 30fps nicht.';
  if (err.name === 'SecurityError') return 'Sicherheitsfehler. Seite läuft nicht über HTTPS.';
  if (err.name === 'AbortError') return 'Vorgang abgebrochen.';
  if (err.name === 'NotSupportedError') return 'Nicht unterstützt. Dein Browser kann das nicht.';
  if (err.name === 'InvalidStateError') return 'Falscher Zustand. Etwas läuft in der falschen Reihenfolge.';
  if (err.name === 'PermissionDeniedError') return 'Berechtigung verweigert (älterer Browser).';
  if (err.name === 'DevicesNotFoundError') return 'Keine Geräte gefunden (älterer Browser).';

  if (err.name === 'NotSupportedError' && err.message && err.message.includes('mime')) return 'Codec nicht unterstützt. Dein Browser kann VP8/WebM nicht.';
  if (err.name === 'InvalidStateError' && err.message && err.message.includes('recorder')) return 'MediaRecorder läuft schon oder ist gestoppt.';

  if (err.name === 'QuotaExceededError') return 'Buffer voll. Der Empfänger ist zu langsam, Chunks gehen verloren.';
  if (err.name === 'InvalidStateError' && err.message && err.message.includes('SourceBuffer')) return 'SourceBuffer im falschen Zustand.';
  if (err.name === 'NotSupportedError' && err.message && err.message.includes('codec')) return 'Codec wird von MediaSource nicht unterstützt.';

  if (err.code === 1000) return 'Verbindung normal geschlossen.';
  if (err.code === 1001) return 'Verbindung geschlossen (Tab zu / Browser weg).';
  if (err.code === 1002) return 'Protokollfehler. Server hat falsche Daten bekommen.';
  if (err.code === 1003) return 'Daten nicht akzeptiert.';
  if (err.code === 1006) return 'Verbindung unerwartet abgebrochen. Render schläft oder Internet weg.';
  if (err.code === 1007) return 'Ungültige Daten.';
  if (err.code === 1008) return 'Policy-Verstoß. Server hat abgelehnt.';
  if (err.code === 1009) return 'Nachricht zu groß.';
  if (err.code === 1010) return 'Client erwartet andere Erweiterung.';
  if (err.code === 1011) return 'Server-Fehler. Render hat ein Problem.';
  if (err.code === 1012) return 'Server wird neu gestartet.';
  if (err.code === 1013) return 'Server überlastet. Später nochmal.';
  if (err.code === 1014) return 'Bad Gateway. Render-Proxy hat Problem.';
  if (err.code === 1015) return 'TLS-Fehler. Zertifikat ungültig oder abgelaufen.';
  if (err.code >= 4000 && err.code <= 4999) return 'Eigener Fehlercode: ' + err.code;

  if (err.message && err.message.includes('Failed to fetch')) return 'Netzwerk nicht erreichbar. Render schläft oder offline.';
  if (err.message && err.message.includes('NetworkError')) return 'Netzwerkfehler.';
  if (err.message && err.message.includes('timeout')) return 'Timeout. Server antwortet nicht.';
  if (err.message && err.message.includes('CORS')) return 'CORS-Fehler. Server erlaubt Zugriff nicht.';
  if (err.message && err.message.includes('SSL')) return 'SSL-Fehler.';

  var name = err.name || 'Error';
  var msg = err.message || String(err);
  var code = err.code ? ' (Code ' + err.code + ')' : '';
  return name + code + ': ' + msg;
}

// ============================================================
// LOG
// ============================================================
function log(msg, type) {
  var cls = 'log-entry';
  if (type === 'error') cls += ' log-error';
  else if (type === 'warn') cls += ' log-warn';
  else if (type === 'ok') cls += ' log-ok';

  var time = new Date().toLocaleTimeString();
  var entry = document.createElement('div');
  entry.className = cls;
  entry.textContent = '[' + time + '] ' + msg;
  logEl.prepend(entry);

  if (type === 'error') console.error(msg);
  else if (type === 'warn') console.warn(msg);
  else console.log(msg);
}

function generateCode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

// ============================================================
// VERBINDUNG
// ============================================================
function connect(code, asSender) {
  if (ws) {
    log('Schließe alte WebSocket-Verbindung...', 'warn');
    try { ws.close(); } catch(e) {}
  }

  log('Baue WebSocket zu ' + WS_URL + '...');
  ws = new WebSocket(WS_URL);
  ws.binaryType = 'arraybuffer';
  isSender = asSender;
  warnEl.textContent = '';
  chunkCount = 0;
  bytesReceived = 0;

  var connectTimeout = setTimeout(function() {
    if (ws && ws.readyState !== WebSocket.OPEN) {
      log('TIMEOUT: WebSocket hat nach 10s nicht geöffnet!', 'error');
      statusEl.textContent = 'Verbindung fehlgeschlagen';
    }
  }, 10000);

  ws.onopen = function() {
    clearTimeout(connectTimeout);
    log('WebSocket OFFEN', 'ok');
    log('Sende JOIN für Raum ' + code + ' als ' + (asSender ? 'SENDER' : 'EMPFÄNGER'));
    ws.send('JOIN:' + code + ':' + (asSender ? 'sender' : 'receiver'));
    statusEl.textContent = 'Raum ' + code + ' verbunden';
  };

  ws.onmessage = function(event) {
    if (typeof event.data === 'string') {
      log('SERVER: ' + event.data);
      if (event.data === 'ROOM_EMPTY') {
        warnEl.textContent = 'Kein Sender in diesem Raum!';
      }
      if (event.data === 'SENDER_JOINED') {
        warnEl.textContent = '';
        log('Sender ist da!', 'ok');
      }
      if (event.data === 'SENDER_LEFT') {
        warnEl.textContent = 'Sender hat den Raum verlassen!';
      }
      return;
    }

    if (event.data instanceof ArrayBuffer && !isSender) {
      chunkCount++;
      bytesReceived += event.data.byteLength;

      if (chunkCount === 1) {
        log('Erster Chunk empfangen (' + event.data.byteLength + ' bytes)', 'ok');
      } else if (chunkCount % 20 === 0) {
        log('Chunks: ' + chunkCount + ' | Empfangen: ' + (bytesReceived/1024).toFixed(1) + ' KB');
      }

      if (sourceBuffer) {
        if (sourceBuffer.updating) {
          log('Buffer beschäftigt - Chunk verworfen', 'warn');
        } else {
          try {
            sourceBuffer.appendBuffer(event.data);
          } catch (e) {
            log('appendBuffer: ' + erklärFehler(e), 'error');
          }
        }
      } else {
        log('sourceBuffer NULL - Chunk verworfen!', 'error');
      }
    }
  };

  ws.onerror = function(e) {
    log('WebSocket ERROR: ' + erklärFehler(e), 'error');
    statusEl.textContent = 'Verbindungsfehler';
  };

  ws.onclose = function(e) {
    clearTimeout(connectTimeout);
    log('WebSocket GESCHLOSSEN: ' + erklärFehler(e), 'warn');
    statusEl.textContent = 'Getrennt';
  };
}

// ============================================================
// EMPFÄNGER
// ============================================================
function setupReceiver() {
  log('Richte Empfänger ein...');
  mediaSource = new MediaSource();
  videoEl.src = URL.createObjectURL(mediaSource);

  mediaSource.addEventListener('sourceopen', function() {
    log('MediaSource OFFEN', 'ok');
    try {
      sourceBuffer = mediaSource.addSourceBuffer('video/webm;codecs=vp8');
      sourceBuffer.mode = 'sequence';
      log('SourceBuffer bereit (vp8/webm)', 'ok');
      sourceBuffer.addEventListener('error', function() {
        log('SourceBuffer ERROR', 'error');
      });
    } catch (e) {
      log('addSourceBuffer: ' + erklärFehler(e), 'error');
    }
  });
}

// ============================================================
// SENDER
// ============================================================
async function startStream(type) {
  try {
    log('Starte Stream (' + type + ')...');
    var stream;

    if (type === 'screen') {
      if (!canScreen) { log('Bildschirm nicht unterstützt', 'error'); return; }
      log('Rufe getDisplayMedia auf...');
      stream = await navigator.mediaDevices.getDisplayMedia({
        video: { frameRate: 30 },
        audio: false
      });
    } else {
      if (!canCam) { log('Kamera nicht unterstützt', 'error'); return; }
      log('Rufe getUserMedia auf...');
      stream = await navigator.mediaDevices.getUserMedia({
        video: { frameRate: 30, facingMode: 'environment' },
        audio: false
      });
    }

    log('Stream erhalten: ' + stream.getTracks().length + ' Track(s)', 'ok');
    currentStream = stream;
    videoEl.srcObject = stream;

    var mimeType = 'video/webm;codecs=vp8';
    if (!MediaRecorder.isTypeSupported(mimeType)) {
      log('vp8 nicht unterstützt, versuche video/webm', 'warn');
      mimeType = 'video/webm';
      if (!MediaRecorder.isTypeSupported(mimeType)) {
        log('video/webm nicht unterstützt!', 'error');
        return;
      }
    }

    log('MediaRecorder mimeType: ' + mimeType);

    mediaRecorder = new MediaRecorder(stream, {
      mimeType: mimeType,
      videoBitsPerSecond: 1500000
    });

    mediaRecorder.onstart = function() { log('MediaRecorder GESTARTET', 'ok'); };
    mediaRecorder.onstop = function() { log('MediaRecorder GESTOPPT', 'warn'); };
    mediaRecorder.onerror = function(e) { log('MediaRecorder: ' + erklärFehler(e.error || e), 'error'); };

    mediaRecorder.ondataavailable = function(event) {
      if (event.data.size > 0 && ws && ws.readyState === WebSocket.OPEN && isSender) {
        chunkCount++;
        event.data.arrayBuffer().then(function(buf) {
          try {
            ws.send(buf);
            if (chunkCount % 20 === 0) {
              log('Gesendet: ' + chunkCount + ' Chunks | ' + (buf.byteLength/1024).toFixed(1) + ' KB');
            }
          } catch(e) {
            log('send: ' + erklärFehler(e), 'error');
          }
        });
      }
    };

    mediaRecorder.start(100);
    log('Stream gestartet (' + type + ')', 'ok');
    screenBtn.disabled = true;
    camBtn.disabled = true;
    stopBtn.disabled = false;

    stream.getVideoTracks()[0].onended = stopStream;
  } catch (err) {
    log('Stream: ' + erklärFehler(err), 'error');
  }
}

function stopStream() {
  log('Stoppe Stream...', 'warn');
  try {
    if (mediaRecorder && mediaRecorder.state !== 'inactive') mediaRecorder.stop();
  } catch(e) {}
  if (currentStream) {
    currentStream.getTracks().forEach(function(t) { t.stop(); });
    currentStream = null;
  }
  screenBtn.disabled = false;
  camBtn.disabled = false;
  stopBtn.disabled = true;
  log('Stream gestoppt', 'warn');
}

// ============================================================
// INIT
// ============================================================
var myCode = generateCode();
myCodeEl.textContent = myCode;
log('Seite geladen. Mein Code: ' + myCode);
log('Bildschirm: ' + canScreen + ' | Kamera: ' + canCam);
log('WebSocket URL: ' + WS_URL);

screenBtn.addEventListener('click', function() {
  connect(myCode, true);
  var waitOpen = setInterval(function() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      clearInterval(waitOpen);
      startStream('screen');
    }
  }, 100);
});

camBtn.addEventListener('click', function() {
  connect(myCode, true);
  var waitOpen = setInterval(function() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      clearInterval(waitOpen);
      startStream('cam');
    }
  }, 100);
});

stopBtn.addEventListener('click', stopStream);

joinBtn.addEventListener('click', function() {
  var code = joinCodeEl.value.trim();
  if (!/^\\d{6}$/.test(code)) { alert('6-stelliger Code eingeben'); return; }
  log('Verbinde als Empfänger mit Raum ' + code);
  setupReceiver();
  connect(code, false);
});

window.addEventListener('error', function(e) {
  log('GLOBAL: ' + erklärFehler(e), 'error');
});
window.addEventListener('unhandledrejection', function(e) {
  log('PROMISE: ' + erklärFehler(e.reason), 'error');
});
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
// WEBSOCKET-RELAY
// ============================================================
const wss = new WebSocket.Server({ server });
const rooms = new Map();

function getRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, { senders: new Set(), receivers: new Set() });
  }
  return rooms.get(code);
}

wss.on('connection', function(ws, req) {
  let roomCode = null;
  let role = null;
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

  console.log('[SERVER] Neue Verbindung von ' + clientIp);

  ws.on('message', function(data) {
    const text = data.toString();

    if (text.startsWith('JOIN:')) {
      const parts = text.split(':');
      roomCode = parts[1];
      role = parts[2];
      const room = getRoom(roomCode);

      if (role === 'sender') {
        room.senders.add(ws);
        console.log('[SERVER] Sender ' + clientIp + ' joined room ' + roomCode);
        for (const r of room.receivers) {
          if (r.readyState === WebSocket.OPEN) r.send('SENDER_JOINED');
        }
      } else {
        room.receivers.add(ws);
        console.log('[SERVER] Receiver ' + clientIp + ' joined room ' + roomCode);
        if (room.senders.size === 0) {
          ws.send('ROOM_EMPTY');
          console.log('[SERVER] Room ' + roomCode + ' leer - ROOM_EMPTY gesendet');
        } else {
          ws.send('SENDER_JOINED');
        }
      }
      return;
    }

    if (roomCode) {
      const room = rooms.get(roomCode);
      if (!room) return;

      if (role === 'sender') {
        for (const r of room.receivers) {
          if (r !== ws && r.readyState === WebSocket.OPEN) {
            r.send(data);
          }
        }
      }
    }
  });

  ws.on('close', function() {
    console.log('[SERVER] Close: ' + clientIp + ' | Raum: ' + roomCode + ' | Rolle: ' + role);
    if (!roomCode) return;
    const room = rooms.get(roomCode);
    if (!room) return;

    if (role === 'sender') {
      room.senders.delete(ws);
      for (const r of room.receivers) {
        if (r.readyState === WebSocket.OPEN) r.send('SENDER_LEFT');
      }
    } else {
      room.receivers.delete(ws);
    }

    if (room.senders.size === 0 && room.receivers.size === 0) {
      rooms.delete(roomCode);
    }
  });

  ws.on('error', function(err) {
    console.log('[SERVER] WebSocket Error: ' + err.message);
  });
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, function() {
  console.log('[SERVER] NERON DECK Relay läuft auf Port ' + PORT);
});
