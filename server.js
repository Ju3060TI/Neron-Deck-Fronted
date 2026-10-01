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
body { background:#0a0e14; color:#00ffcc; font-family:'Courier New',monospace; padding:20px; text-align:center; min-height:100vh; }
h1 { font-size:28px; letter-spacing:4px; text-shadow:0 0 10px #00ffcc; margin-bottom:10px; }
h2 { font-size:14px; letter-spacing:2px; margin:15px 0 10px; }
.code { font-size:42px; letter-spacing:12px; text-shadow:0 0 15px #00ffcc; padding:15px; background:#000; border-radius:6px; margin:10px auto; max-width:400px; font-weight:bold; }
button { background:transparent; color:#00ffcc; border:2px solid #00ffcc; padding:10px 22px; font-size:14px; font-family:'Courier New',monospace; font-weight:bold; cursor:pointer; margin:5px; border-radius:4px; transition:all 0.2s; }
button:hover:not(:disabled) { background:#00ffcc; color:#0a0e14; }
button:disabled { opacity:0.3; cursor:not-allowed; }
input { background:#000; color:#00ffcc; border:2px solid #00ffcc; padding:10px; font-size:22px; text-align:center; width:160px; letter-spacing:8px; border-radius:4px; font-family:'Courier New',monospace; outline:none; }
#video { width:90%; max-width:1000px; border:2px solid #00ffcc44; border-radius:8px; background:#000; margin:15px auto; display:block; aspect-ratio:16/9; }
.status { color:#888; font-size:12px; margin-top:10px; }
.warn { color:#ff4444; font-size:14px; margin-top:10px; font-weight:bold; }
.log { text-align:left; font-size:11px; color:#666; max-height:150px; overflow-y:auto; margin:20px auto; max-width:800px; background:#0d1117; padding:10px; border-radius:6px; border:1px solid #00ffcc22; }
</style>
</head>
<body>
<h1>NERON DECK</h1>
<p style="color:#00ffcc66;font-size:11px;">WebSocket Stream Relay</p>

<h2>&#9654; SENDER &mdash; DEIN CODE</h2>
<div class="code" id="myCode">------</div>
<button id="screenBtn">&#9654; BILDSCHIRM</button>
<button id="camBtn">&#9654; KAMERA</button>
<button id="stopBtn" disabled>&#9632; STOP</button>

<h2>&#9664; RECEIVER &mdash; CODE EINGEBEN</h2>
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

var WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host;

var canScreen = !!(navigator.mediaDevices && navigator.mediaDevices.getDisplayMedia);
var canCam = !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);

if (!canScreen) screenBtn.style.display = 'none';
if (!canCam) camBtn.style.display = 'none';

function log(msg) {
  logEl.textContent = '[' + new Date().toLocaleTimeString() + '] ' + msg + '\\n' + logEl.textContent;
  console.log(msg);
}

function generateCode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

function connect(code, asSender) {
  if (ws) {
    try { ws.close(); } catch(e) {}
  }
  ws = new WebSocket(WS_URL);
  ws.binaryType = 'arraybuffer';
  isSender = asSender;
  warnEl.textContent = '';

  ws.onopen = function() {
    log('Verbunden, Raum ' + code);
    ws.send('JOIN:' + code + ':' + (asSender ? 'sender' : 'receiver'));
    statusEl.textContent = 'Raum ' + code + ' verbunden';
  };

  ws.onmessage = function(event) {
    // Text-Nachricht vom Server (Statusmeldung)
    if (typeof event.data === 'string') {
      if (event.data === 'ROOM_EMPTY') {
        warnEl.textContent = '&#9888; Kein Sender in diesem Raum!';
        log('Raum leer - kein Sender aktiv');
      }
      if (event.data === 'SENDER_JOINED') {
        warnEl.textContent = '';
        log('Sender ist dem Raum beigetreten');
      }
      if (event.data === 'SENDER_LEFT') {
        warnEl.textContent = '&#9888; Sender hat den Raum verlassen!';
        log('Sender hat den Raum verlassen');
      }
      return;
    }

    // Binärdaten (Video-Chunks)
    if (event.data instanceof ArrayBuffer && !isSender) {
      if (sourceBuffer && !sourceBuffer.updating) {
        try { sourceBuffer.appendBuffer(event.data); }
        catch (e) { /* Buffer voll - ignorieren */ }
      }
    }
  };

  ws.onerror = function() { /* still */ };
  ws.onclose = function() { /* still */ };
}

function setupReceiver() {
  mediaSource = new MediaSource();
  videoEl.src = URL.createObjectURL(mediaSource);
  mediaSource.addEventListener('sourceopen', function() {
    try {
      sourceBuffer = mediaSource.addSourceBuffer('video/webm;codecs=vp8');
      sourceBuffer.mode = 'sequence';
      log('Empfänger bereit');
    } catch (e) {
      log('MediaSource-Fehler: ' + e.message);
    }
  });
}

async function startStream(type) {
  try {
    var stream;
    if (type === 'screen') {
      if (!canScreen) { log('Bildschirm nicht unterstützt'); return; }
      stream = await navigator.mediaDevices.getDisplayMedia({
        video: { frameRate: 30 },
        audio: false
      });
    } else {
      if (!canCam) { log('Kamera nicht unterstützt'); return; }
      stream = await navigator.mediaDevices.getUserMedia({
        video: { frameRate: 30, facingMode: 'environment' },
        audio: false
      });
    }

    currentStream = stream;
    videoEl.srcObject = stream;

    var mimeType = 'video/webm;codecs=vp8';
    if (!MediaRecorder.isTypeSupported(mimeType)) {
      mimeType = 'video/webm';
    }

    mediaRecorder = new MediaRecorder(stream, {
      mimeType: mimeType,
      videoBitsPerSecond: 1500000
    });

    mediaRecorder.ondataavailable = function(event) {
      if (event.data.size > 0 && ws && ws.readyState === WebSocket.OPEN && isSender) {
        event.data.arrayBuffer().then(function(buf) {
          try { ws.send(buf); } catch(e) {}
        });
      }
    };

    mediaRecorder.start(100);
    log('Stream gestartet (' + type + ')');
    screenBtn.disabled = true;
    camBtn.disabled = true;
    stopBtn.disabled = false;

    stream.getVideoTracks()[0].onended = stopStream;
  } catch (err) {
    log('Fehler: ' + err.message);
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
  log('Stream gestoppt');
}

var myCode = generateCode();
myCodeEl.textContent = myCode;

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
  setupReceiver();
  connect(code, false);
});

window.addEventListener('error', function(e) { e.preventDefault(); });
window.addEventListener('unhandledrejection', function(e) { e.preventDefault(); });
</script>
</body>
</html>`;

const server = http.createServer(function(req, res) {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(HTML);
});

const wss = new WebSocket.Server({ server });

// Raum-Struktur: code -> { senders: Set, receivers: Set }
const rooms = new Map();

function getRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, { senders: new Set(), receivers: new Set() });
  }
  return rooms.get(code);
}

wss.on('connection', function(ws) {
  let roomCode = null;
  let role = null;

  ws.on('message', function(data) {
    const text = data.toString();

    // JOIN:<code>:<role>
    if (text.startsWith('JOIN:')) {
      const parts = text.split(':');
      roomCode = parts[1];
      role = parts[2];

      const room = getRoom(roomCode);

      if (role === 'sender') {
        room.senders.add(ws);
        console.log('Sender joined room ' + roomCode);
        // Empfänger benachrichtigen, dass Sender da ist
        for (const r of room.receivers) {
          if (r.readyState === WebSocket.OPEN) r.send('SENDER_JOINED');
        }
      } else {
        room.receivers.add(ws);
        console.log('Receiver joined room ' + roomCode);
        // Prüfen, ob ein Sender da ist
        if (room.senders.size === 0) {
          ws.send('ROOM_EMPTY');
        } else {
          ws.send('SENDER_JOINED');
        }
      }
      return;
    }

    // Binärdaten weiterleiten
    if (roomCode) {
      const room = rooms.get(roomCode);
      if (!room) return;

      if (role === 'sender') {
        // Sender -> alle Empfänger
        for (const r of room.receivers) {
          if (r !== ws && r.readyState === WebSocket.OPEN) {
            r.send(data);
          }
        }
      } else {
        // Empfänger -> alle Sender (normalerweise nicht nötig)
        for (const s of room.senders) {
          if (s !== ws && s.readyState === WebSocket.OPEN) {
            s.send(data);
          }
        }
      }
    }
  });

  ws.on('close', function() {
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
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, function() {
  console.log('NERON DECK Relay laeuft auf Port ' + PORT);
});
