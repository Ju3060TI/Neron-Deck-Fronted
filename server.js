const WebSocket = require('ws');
const http = require('http');

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
<p style="color:#00ffcc66;font-size:10px;">WebSocket Stream Relay</p>

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
var videoEl = document.getElementById('video');
var statusEl = document.getElementById('status');
var warnEl = document.getElementById('warnBox');
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
var pendingStreamType = null;
var receiverReady = false;

var WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host;

var canScreen = !!(navigator.mediaDevices && navigator.mediaDevices.getDisplayMedia);
var canCam = !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);

if (!canScreen) screenBtn.style.display = 'none';
if (!canCam) camBtn.style.display = 'none';

function erklärFehler(err) {
  if (!err) return 'Unbekannter Fehler';
  if (err.name === 'NotAllowedError') return 'Berechtigung verweigert.';
  if (err.name === 'NotFoundError') return 'Kein Gerät gefunden.';
  if (err.name === 'NotReadableError') return 'Gerät blockiert (andere App).';
  if (err.name === 'NotSupportedError') return 'Nicht unterstützt.';
  if (err.name === 'QuotaExceededError') return 'Buffer voll.';
  if (err.code === 1000) return 'Verbindung normal geschlossen.';
  if (err.code === 1001) return 'Verbindung geschlossen (Tab zu).';
  if (err.code === 1006) return 'Verbindung unerwartet abgebrochen (Render schläft / Internet weg).';
  if (err.code === 1011) return 'Server-Fehler.';
  if (err.message && err.message.includes('Failed to fetch')) return 'Netzwerk nicht erreichbar.';
  return (err.name || 'Error') + ': ' + (err.message || String(err));
}

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

function sendReceiverReady() {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send('RECEIVER_READY');
    log('RECEIVER_READY gesendet', 'ok');
  } else {
    log('Warte auf WebSocket, um RECEIVER_READY zu senden...', 'warn');
    setTimeout(sendReceiverReady, 200);
  }
}

function connect(code, asSender) {
  if (ws) { try { ws.close(); } catch(e) {} }

  ws = new WebSocket(WS_URL);
  ws.binaryType = 'arraybuffer';
  isSender = asSender;
  warnEl.textContent = '';
  chunkCount = 0;
  bytesReceived = 0;
  receiverReady = false;

  ws.onopen = function() {
    log('WebSocket OFFEN', 'ok');
    ws.send('JOIN:' + code + ':' + (asSender ? 'sender' : 'receiver'));
    statusEl.textContent = 'Raum ' + code + ' verbunden';

    if (!asSender && sourceBuffer) {
      sendReceiverReady();
    }
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
      if (event.data === 'RECEIVER_READY') {
        receiverReady = true;
        log('Empfänger bereit! Starte Stream.', 'ok');
        if (pendingStreamType) {
          startStream(pendingStreamType);
          pendingStreamType = null;
        }
      }
      return;
    }

    if (event.data instanceof ArrayBuffer && !isSender) {
      chunkCount++;
      bytesReceived += event.data.byteLength;

      if (chunkCount === 1) {
        log('Erster Chunk empfangen (' + event.data.byteLength + ' bytes)', 'ok');
      } else if (chunkCount % 50 === 0) {
        log('Chunks: ' + chunkCount + ' | ' + (bytesReceived/1024).toFixed(1) + ' KB');
      }

      if (sourceBuffer) {
        if (sourceBuffer.updating) {
          var waitForBuffer = setInterval(function() {
            if (!sourceBuffer.updating) {
              clearInterval(waitForBuffer);
              try { sourceBuffer.appendBuffer(event.data); }
              catch (e) { log('appendBuffer: ' + erklärFehler(e), 'error'); }
            }
          }, 10);
        } else {
          try {
            sourceBuffer.appendBuffer(event.data);
          } catch (e) {
            log('appendBuffer: ' + erklärFehler(e), 'error');
          }
        }
      }
    }
  };

  ws.onerror = function(e) { log('WebSocket ERROR: ' + erklärFehler(e), 'error'); };
  ws.onclose = function(e) { log('WebSocket GESCHLOSSEN: ' + erklärFehler(e), 'warn'); };
}

function setupReceiver() {
  log('Richte Empfänger ein...');
  mediaSource = new MediaSource();
  videoEl.src = URL.createObjectURL(mediaSource);

  mediaSource.addEventListener('sourceopen', function() {
    log('MediaSource OFFEN', 'ok');
    try {
      sourceBuffer = mediaSource.addSourceBuffer('video/webm;codecs=vp8,opus');
      sourceBuffer.mode = 'sequence';
      log('SourceBuffer bereit', 'ok');

      sendReceiverReady();
    } catch (e) {
      log('addSourceBuffer: ' + erklärFehler(e), 'error');
    }
  });
}

async function startStream(type) {
  try {
    log('Starte Stream (' + type + ')...');
    var stream;

    if (type === 'screen') {
      stream = await navigator.mediaDevices.getDisplayMedia({
        video: { frameRate: 30 },
        audio: true
      });
    } else {
      stream = await navigator.mediaDevices.getUserMedia({
        video: { frameRate: 30, facingMode: 'environment' },
        audio: true
      });
    }

    log('Stream erhalten', 'ok');
    currentStream = stream;
    videoEl.srcObject = stream;

    var mimeType = 'video/webm;codecs=vp8,opus';
    if (!MediaRecorder.isTypeSupported(mimeType)) {
      mimeType = 'video/webm';
      log('vp8/opus nicht unterstützt, nutze video/webm', 'warn');
    }

    mediaRecorder = new MediaRecorder(stream, {
      mimeType: mimeType,
      videoBitsPerSecond: 1500000,
      audioBitsPerSecond: 64000
    });

    mediaRecorder.ondataavailable = function(event) {
      if (event.data.size > 0 && ws && ws.readyState === WebSocket.OPEN && isSender) {
        chunkCount++;
        event.data.arrayBuffer().then(function(buf) {
          try {
            ws.send(buf);
            if (chunkCount % 50 === 0) {
              log('Gesendet: ' + chunkCount + ' | ' + (buf.byteLength/1024).toFixed(1) + ' KB');
            }
          } catch(e) {}
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

var myCode = generateCode();
myCodeEl.textContent = myCode;
log('Seite geladen. Code: ' + myCode);
log('Bildschirm: ' + canScreen + ' | Kamera: ' + canCam);

screenBtn.addEventListener('click', function() {
  connect(myCode, true);
  pendingStreamType = 'screen';
  var waitOpen = setInterval(function() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      clearInterval(waitOpen);
      if (receiverReady) { startStream('screen'); pendingStreamType = null; }
      else { log('Warte auf Empfänger...'); }
    }
  }, 100);
});

camBtn.addEventListener('click', function() {
  connect(myCode, true);
  pendingStreamType = 'cam';
  var waitOpen = setInterval(function() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      clearInterval(waitOpen);
      if (receiverReady) { startStream('cam'); pendingStreamType = null; }
      else { log('Warte auf Empfänger...'); }
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
</script>
</body>
</html>`;

const server = http.createServer(function(req, res) {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(HTML);
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, { senders: new Set(), receivers: new Set() });
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
        console.log('[SERVER] Sender joined room ' + roomCode);
        for (const r of room.receivers) {
          if (r.readyState === WebSocket.OPEN) r.send('SENDER_JOINED');
        }
      } else {
        room.receivers.add(ws);
        console.log('[SERVER] Receiver joined room ' + roomCode);
        if (room.senders.size === 0) ws.send('ROOM_EMPTY');
        else ws.send('SENDER_JOINED');
      }
      return;
    }

    if (text === 'RECEIVER_READY') {
      const room = rooms.get(roomCode);
      if (room) {
        for (const s of room.senders) {
          if (s.readyState === WebSocket.OPEN) s.send('RECEIVER_READY');
        }
      }
      return;
    }

    if (roomCode) {
      const room = rooms.get(roomCode);
      if (!room) return;
      if (role === 'sender') {
        for (const r of room.receivers) {
          if (r !== ws && r.readyState === WebSocket.OPEN) r.send(data);
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
    if (room.senders.size === 0 && room.receivers.size === 0) rooms.delete(roomCode);
  });
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, function() {
  console.log('[SERVER] NERON DECK Relay läuft auf Port ' + PORT);
});
