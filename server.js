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
.log { text-align:left; font-size:11px; color:#666; max-height:150px; overflow-y:auto; margin:20px auto; max-width:800px; background:#0d1117; padding:10px; border-radius:6px; border:1px solid #00ffcc22; }
</style>
</head>
<body>
<h1>NERON DECK</h1>
<p style="color:#00ffcc66;font-size:11px;">WebSocket Stream Relay</p>

<h2>&#9654; SENDER &mdash; DEIN CODE</h2>
<div class="code" id="myCode">------</div>
<button id="startBtn">&#9654; START STREAM</button>
<button id="stopBtn" disabled>&#9632; STOP</button>

<h2>&#9664; RECEIVER &mdash; CODE EINGEBEN</h2>
<input type="text" id="joinCode" maxlength="6" placeholder="000000" inputmode="numeric">
<button id="joinBtn">VERBINDEN</button>

<video id="video" autoplay playsinline muted></video>
<div class="status" id="status">Bereit</div>
<div class="log" id="log"></div>

<script>
var videoEl = document.getElementById('video');
var statusEl = document.getElementById('status');
var logEl = document.getElementById('log');
var myCodeEl = document.getElementById('myCode');
var joinCodeEl = document.getElementById('joinCode');

var ws = null;
var mediaRecorder = null;
var mediaSource = null;
var sourceBuffer = null;
var currentStream = null;

var WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host;

function log(msg) {
  logEl.textContent = '[' + new Date().toLocaleTimeString() + '] ' + msg + '\\n' + logEl.textContent;
  console.log(msg);
}

function generateCode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

function connect(code) {
  if (ws) ws.close();
  ws = new WebSocket(WS_URL);
  ws.binaryType = 'arraybuffer';

  ws.onopen = function() {
    log('Verbunden, Raum ' + code);
    ws.send('JOIN:' + code + ':' + Date.now());
    statusEl.textContent = 'Raum ' + code + ' verbunden';
  };

  ws.onmessage = function(event) {
    if (event.data instanceof ArrayBuffer) {
      if (sourceBuffer && !sourceBuffer.updating) {
        try { sourceBuffer.appendBuffer(event.data); }
        catch (e) { log('Buffer-Fehler: ' + e.message); }
      }
    }
  };

  ws.onerror = function() { log('WebSocket-Fehler'); };
  ws.onclose = function() { log('WebSocket getrennt'); };
}

var myCode = generateCode();
myCodeEl.textContent = myCode;
connect(myCode);

document.getElementById('startBtn').addEventListener('click', async function() {
  try {
    var stream = await navigator.mediaDevices.getDisplayMedia({
      video: { frameRate: 30 },
      audio: false
    });

    currentStream = stream;
    videoEl.srcObject = stream;

    mediaRecorder = new MediaRecorder(stream, {
      mimeType: 'video/webm;codecs=vp8',
      videoBitsPerSecond: 1500000
    });

    mediaRecorder.ondataavailable = function(event) {
      if (event.data.size > 0 && ws && ws.readyState === WebSocket.OPEN) {
        event.data.arrayBuffer().then(function(buf) { ws.send(buf); });
      }
    };

    mediaRecorder.start(100);
    log('Stream gestartet');
    document.getElementById('startBtn').disabled = true;
    document.getElementById('stopBtn').disabled = false;

    stream.getVideoTracks()[0].onended = stopStream;
  } catch (err) {
    log('Fehler: ' + err.message);
  }
});

function stopStream() {
  if (mediaRecorder) mediaRecorder.stop();
  if (currentStream) currentStream.getTracks().forEach(function(t) { t.stop(); });
  document.getElementById('startBtn').disabled = false;
  document.getElementById('stopBtn').disabled = true;
  log('Stream gestoppt');
}

document.getElementById('stopBtn').addEventListener('click', stopStream);

document.getElementById('joinBtn').addEventListener('click', function() {
  var code = joinCodeEl.value.trim();
  if (!/^\\d{6}$/.test(code)) { alert('6-stelliger Code eingeben'); return; }

  mediaSource = new MediaSource();
  videoEl.src = URL.createObjectURL(mediaSource);

  mediaSource.addEventListener('sourceopen', function() {
    sourceBuffer = mediaSource.addSourceBuffer('video/webm;codecs=vp8');
    sourceBuffer.mode = 'sequence';
    log('Empf&auml;nger bereit');
    connect(code);
  });
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

wss.on('connection', function(ws) {
  let roomCode = null;
  let clientId = null;

  ws.on('message', function(data) {
    const text = data.toString();

    if (text.startsWith('JOIN:')) {
      const parts = text.split(':');
      roomCode = parts[1];
      clientId = parts[2];

      if (!rooms.has(roomCode)) rooms.set(roomCode, new Set());
      rooms.get(roomCode).add(ws);

      console.log('Client ' + clientId + ' joined room ' + roomCode);
      return;
    }

    if (roomCode && rooms.has(roomCode)) {
      for (const client of rooms.get(roomCode)) {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          client.send(data);
        }
      }
    }
  });

  ws.on('close', function() {
    if (roomCode && rooms.has(roomCode)) {
      rooms.get(roomCode).delete(ws);
      if (rooms.get(roomCode).size === 0) rooms.delete(roomCode);
      console.log('Client ' + clientId + ' left room ' + roomCode);
    }
  });
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, function() {
  console.log('NERON DECK Relay laeuft auf Port ' + PORT);
});
